/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "pebbles_record_store.h"

#include <mutex>
#include <memory>
#include <algorithm>

#include <boost/thread/locks.hpp>

#include <pebblesdb/comparator.h>
#include <pebblesdb/db.h>
#include <pebblesdb/options.h>
#include <pebblesdb/slice.h>

#include "mongo/base/checked_cast.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/oplog_hack.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "pebbles_counter_manager.h"
#include "pebbles_durability_manager.h"
#include "pebbles_engine.h"
#include "pebbles_recovery_unit.h"
#include "pebbles_util.h"

namespace mongo {

    class LevelRecordStore::CappedInsertChange : public RecoveryUnit::Change {
    public:
        CappedInsertChange(CappedVisibilityManager* cappedVisibilityManager, LevelRecordStore* rs,
                           SortedRecordIds::iterator it)
            : _cappedVisibilityManager(cappedVisibilityManager), _rs(rs), _it(it) {}

        virtual void commit() { _cappedVisibilityManager->dealtWithCappedRecord(_it, true); }

        virtual void rollback() {
            _cappedVisibilityManager->dealtWithCappedRecord(_it, false);
            stdx::lock_guard<stdx::mutex> lk(_rs->_cappedCallbackMutex);
            if (_rs->_cappedCallback) {
                _rs->_cappedCallback->notifyCappedWaitersIfNeeded();
            }
        }

    private:
        CappedVisibilityManager* _cappedVisibilityManager;
        LevelRecordStore* const _rs;
        const SortedRecordIds::iterator _it;
    };

    CappedVisibilityManager::CappedVisibilityManager(LevelRecordStore* rs,
                                                     LevelDurabilityManager* durabilityManger)
        : _rs(rs), _oplog_highestSeen(RecordId::min()), _shuttingDown(false) {
        if (_rs->_isOplog) {
            _oplogJournalThread = stdx::thread(&CappedVisibilityManager::oplogJournalThreadLoop,
                                               this, durabilityManger);
        }
    }

    void CappedVisibilityManager::addUncommittedRecord(OperationContext* txn,
                                                       const RecordId& record) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        _addUncommittedRecord_inlock(txn, record);
    }

    void CappedVisibilityManager::_addUncommittedRecord_inlock(OperationContext* txn,
                                                               const RecordId& record) {
        dassert(_uncommittedRecords.empty() || _uncommittedRecords.back() < record);
        SortedRecordIds::iterator it =
            _uncommittedRecords.insert(_uncommittedRecords.end(), record);
        txn->recoveryUnit()->registerChange(
            new LevelRecordStore::CappedInsertChange(this, _rs, it));
        _oplog_highestSeen = record;
    }

    RecordId CappedVisibilityManager::getNextAndAddUncommittedRecord(
        OperationContext* txn, std::function<RecordId()> nextId) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        RecordId record = nextId();
        _addUncommittedRecord_inlock(txn, record);
        return record;
    }

    void CappedVisibilityManager::oplogJournalThreadLoop(
        LevelDurabilityManager* durabilityManager) try {
        Client::initThread("LevelOplogJournalThread");
        while (true) {
            stdx::unique_lock<stdx::mutex> lk(_uncommittedRecordIdsMutex);
            _opsWaitingForJournalCV.wait(
                lk, [&] { return _shuttingDown || !_opsWaitingForJournal.empty(); });

            if (_shuttingDown) {
                return;
            }

            decltype(_opsWaitingForJournal) opsAboutToBeJournaled = {};
            _opsWaitingForJournal.swap(opsAboutToBeJournaled);

            lk.unlock();
            durabilityManager->waitUntilDurable(/*forceFlush=*/false);
            lk.lock();

            for (auto&& op : opsAboutToBeJournaled) {
                _uncommittedRecords.erase(op);
            }

            _opsBecameVisibleCV.notify_all();
            lk.unlock();

            stdx::lock_guard<stdx::mutex> cappedCallbackLock(_rs->_cappedCallbackMutex);
            if (_rs->_cappedCallback) {
                _rs->_cappedCallback->notifyCappedWaitersIfNeeded();
            }
        }
    } catch (...) {
        std::terminate();
    }

    void CappedVisibilityManager::joinOplogJournalThreadLoop() {
        {
            stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
            _shuttingDown = true;
        }
        if (_oplogJournalThread.joinable()) {
            _opsWaitingForJournalCV.notify_one();
            _oplogJournalThread.join();
        }
    }

    void CappedVisibilityManager::waitForAllEarlierOplogWritesToBeVisible(
        OperationContext* txn) const {
        invariant(txn->lockState()->isNoop() || !txn->lockState()->inAWriteUnitOfWork());

        stdx::unique_lock<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        const auto waitingFor = _oplog_highestSeen;
        txn->waitForConditionOrInterrupt(_opsBecameVisibleCV, lk, [&] {
            return _uncommittedRecords.empty() || _uncommittedRecords.front() > waitingFor;
        });
    }

    void CappedVisibilityManager::dealtWithCappedRecord(SortedRecordIds::iterator it, bool didCommit) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        if (didCommit && _rs->_isOplog && *it != _oplog_highestSeen) {
            // Defer removal from _uncommittedRecordIds until it is durable. We don't need to wait
            // for durability of ops that didn't commit because they won't become durable.
            // As an optimization, we only defer visibility until durable if new ops were created
            // while we were pending. This makes single-threaded w>1 workloads faster and is safe
            // because durability follows commit order for commits that are fully sequenced (B
            // doesn't call commit until after A's commit call returns).
            const bool wasEmpty = _opsWaitingForJournal.empty();
            _opsWaitingForJournal.push_back(it);
            if (wasEmpty) {
                _opsWaitingForJournalCV.notify_one();
            }
        } else {
            _uncommittedRecords.erase(it);
            _opsBecameVisibleCV.notify_all();
        }
    }

    bool CappedVisibilityManager::isCappedHidden(const RecordId& record) const {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        if (_uncommittedRecords.empty()) {
            return false;
        }
        return _uncommittedRecords.front() <= record;
    }

    void CappedVisibilityManager::updateHighestSeen(const RecordId& record) {
        if (record > _oplog_highestSeen) {
            stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
            if (record > _oplog_highestSeen) {
                _oplog_highestSeen = record;
            }
        }
    }

    void CappedVisibilityManager::setHighestSeen(const RecordId& record) {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        _oplog_highestSeen = record;
    }

    RecordId CappedVisibilityManager::oplogStartHack() const {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        if (_uncommittedRecords.empty()) {
            return _oplog_highestSeen;
        } else {
            return _uncommittedRecords.front();
        }
    }

    RecordId CappedVisibilityManager::lowestCappedHiddenRecord() const {
        stdx::lock_guard<stdx::mutex> lk(_uncommittedRecordIdsMutex);
        return _uncommittedRecords.empty() ? RecordId() : _uncommittedRecords.front();
    }

    // this object keeps track of keys in oplog. The format is this:
    // <prefix>RecordId --> dataSize (small endian 32 bytes)
    // <prefix> is oplog_prefix+1 (reserved by level_engine.cpp)
    // That way we can cheaply delete old record in the oplog without actually reading oplog
    // collection.
    // All of the locking is done somewhere else -- we write exactly the same data as oplog, so we
    // assume oplog already locked the relevant keys
    class LevelOplogKeyTracker {
    public:
        LevelOplogKeyTracker(std::string prefix) : _prefix(std::move(prefix)) {}
        void insertKey(LevelRecoveryUnit* ru, const RecordId& loc, int len) {
            uint32_t lenLittleEndian = endian::nativeToLittle(static_cast<uint32_t>(len));
	    ru->writeBatch()->Put(LevelRecordStore::_makePrefixedKey(_prefix, loc),
                                  leveldb::Slice(reinterpret_cast<const char*>(&lenLittleEndian),
                                                 sizeof(lenLittleEndian)));
            ru->writeBatchRepHandler()->Add(LevelRecordStore::_makePrefixedKey(_prefix, loc),
                                  leveldb::Slice(reinterpret_cast<const char*>(&lenLittleEndian),
                                                 sizeof(lenLittleEndian)));
        }
        void deleteKey(LevelRecoveryUnit* ru, const RecordId& loc) {
            ru->writeBatch()->Delete(LevelRecordStore::_makePrefixedKey(_prefix, loc));
            ru->writeBatchRepHandler()->Delete(LevelRecordStore::_makePrefixedKey(_prefix, loc));
            _deletedKeysSinceCompaction++;
        }
        leveldb::Iterator* newIterator(LevelRecoveryUnit* ru) {
            return ru->NewIterator(_prefix, true);
        }
        int decodeSize(const leveldb::Slice& value) {
            uint32_t size =
                endian::littleToNative(*reinterpret_cast<const uint32_t*>(value.data()));
            return static_cast<int>(size);
        }        
        void resetDeletedSinceCompaction() {
            _deletedKeysSinceCompaction = 0;
        }
        long long getDeletedSinceCompaction() {
            return _deletedKeysSinceCompaction;
        }        

    private:
        std::atomic<long long> _deletedKeysSinceCompaction;
        std::string _prefix;
    };

    LevelRecordStore::LevelRecordStore(StringData ns, StringData id, leveldb::DB* db,
                                       LevelCounterManager* counterManager,
                                       LevelDurabilityManager* durabilityManager,
                                       std::string prefix, bool isCapped, int64_t cappedMaxSize,
                                       int64_t cappedMaxDocs, CappedCallback* cappedCallback)
        : RecordStore(ns),
          _db(db),
          _counterManager(counterManager),
          _prefix(std::move(prefix)),
          _isCapped(isCapped),
          _cappedMaxSize(cappedMaxSize),
          _cappedMaxSizeSlack(std::min(cappedMaxSize / 10, int64_t(16 * 1024 * 1024))),
          _cappedMaxDocs(cappedMaxDocs),
          _cappedCallback(cappedCallback),
          _cappedDeleteCheckCount(0),
          _isOplog(NamespaceString::oplog(ns)),
          _oplogKeyTracker(_isOplog ? new LevelOplogKeyTracker(levelGetNextPrefix(_prefix))
                                    : nullptr),
          _cappedOldestKeyHint(0),
          _cappedVisibilityManager((_isCapped || _isOplog)
                                       ? new CappedVisibilityManager(this, durabilityManager)
                                       : nullptr),
          _ident(id.toString()),
          _dataSizeKey(std::string("\0\0\0\0", 4) + "datasize-" + id.toString()),
          _numRecordsKey(std::string("\0\0\0\0", 4) + "numrecords-" + id.toString()),
          _shuttingDown(false) {
        _oplogSinceLastCompaction.reset();

        LOG(1) << "opening collection " << ns << " with prefix "
               << leveldb::Slice(_prefix).ToString();

        if (_isCapped) {
            invariant(_cappedMaxSize > 0);
            invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
        }
        else {
            invariant(_cappedMaxSize == -1);
            invariant(_cappedMaxDocs == -1);
        }

        // Get next id
        std::unique_ptr<LevelIterator> iter(
            LevelRecoveryUnit::NewIteratorNoSnapshot(_db, _prefix));
        // first check if the collection is empty
        iter->SeekPrefix("");
        bool emptyCollection = !iter->Valid();
        if (!emptyCollection) {
            // if it's not empty, find next RecordId
            iter->SeekToLast();
            dassert(iter->Valid());
            leveldb::Slice lastSlice = iter->key();
            RecordId lastId = _makeRecordId(lastSlice);
            if (_isOplog || _isCapped) {
                _cappedVisibilityManager->updateHighestSeen(lastId);
            }
            _nextIdNum.store(lastId.repr() + 1);
        } else {
            // Need to start at 1 so we are always higher than RecordId::min()
            _nextIdNum.store(1);
        }

        // load metadata
        _numRecords.store(_counterManager->loadCounter(_numRecordsKey));
        _dataSize.store(_counterManager->loadCounter(_dataSizeKey));
        if (_dataSize.load() < 0) {
          _dataSize.store(0);
        }
        if (_numRecords.load() < 0) {
          _numRecords.store(0);
        }

        _hasBackgroundThread = LevelEngine::initRsOplogBackgroundThread(ns);
    }

    LevelRecordStore::~LevelRecordStore() {
        {
            stdx::lock_guard<boost::timed_mutex> lk(_cappedDeleterMutex);
            _shuttingDown = true;
        }
        delete _oplogKeyTracker;

        if (_cappedVisibilityManager) {
          _cappedVisibilityManager->joinOplogJournalThreadLoop();
        }
    }

    int64_t LevelRecordStore::storageSize(OperationContext* txn, BSONObjBuilder* extraInfo,
                                          int infoLevel) const {
        // We need to make it multiple of 256 to make
        // jstests/concurrency/fsm_workloads/convert_to_capped_collection.js happy
        return static_cast<int64_t>(
            std::max(_dataSize.load() & (~255), static_cast<long long>(256)));
    }

    RecordData LevelRecordStore::dataFor(OperationContext* txn, const RecordId& loc) const {
      RecordData rd = _getDataFor(_db, _prefix, txn, loc);
        massert(28605, "Didn't find RecordId in LevelRecordStore", (rd.data() != nullptr));
        return rd;
    }

    void LevelRecordStore::deleteRecord( OperationContext* txn, const RecordId& dl ) {
        std::string key(_makePrefixedKey(_prefix, dl));

        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
        if (!ru->transaction()->registerWrite(key)) {
            throw WriteConflictException();
        }

        std::string oldValue;

	auto status = ru->Get(key, &oldValue);
        invariantLevelOK(status);
        int oldLength = oldValue.size();

        ru->writeBatch()->Delete(key);
        ru->writeBatchRepHandler()->Delete(key);
        if (_isOplog) {
            _oplogKeyTracker->deleteKey(ru, dl);
        }

        _changeNumRecords(txn, -1);
        _increaseDataSize(txn, -oldLength);
    }

    long long LevelRecordStore::dataSize(OperationContext* txn) const {
        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
        return _dataSize.load(std::memory_order::memory_order_relaxed) +
               ru->getDeltaCounter(_dataSizeKey);
    }

    long long LevelRecordStore::numRecords(OperationContext* txn) const {
        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit( txn );
        return _numRecords.load(std::memory_order::memory_order_relaxed) +
            ru->getDeltaCounter(_numRecordsKey);
    }

    bool LevelRecordStore::cappedAndNeedDelete(long long dataSizeDelta,
                                               long long numRecordsDelta) const {
        invariant(_isCapped);

        if (_dataSize.load() + dataSizeDelta > _cappedMaxSize)
            return true;

        if ((_cappedMaxDocs != -1) && (_numRecords.load() + numRecordsDelta > _cappedMaxDocs))
            return true;

        return false;
    }

    int64_t LevelRecordStore::cappedDeleteAsNeeded(OperationContext* txn,
                                                   const RecordId& justInserted) {
        if (!_isCapped) {
          return 0;
        }

        // We only want to do the checks occasionally as they are expensive.
        // This variable isn't thread safe, but has loose semantics anyway.
        dassert(!_isOplog || _cappedMaxDocs == -1);

        long long dataSizeDelta = 0, numRecordsDelta = 0;
        if (!_isOplog) {
            auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
            dataSizeDelta = ru->getDeltaCounter(_dataSizeKey);
            numRecordsDelta = ru->getDeltaCounter(_numRecordsKey);
        }

        if (!cappedAndNeedDelete(dataSizeDelta, numRecordsDelta)) {
            return 0;
        }

        // ensure only one thread at a time can do deletes, otherwise they'll conflict.
       boost::unique_lock<boost::timed_mutex> lock(_cappedDeleterMutex, boost::defer_lock);

        if (_cappedMaxDocs != -1) {
            lock.lock(); // Max docs has to be exact, so have to check every time.
        }
        else if(_hasBackgroundThread) {
            // We are foreground, and there is a background thread,

            // Check if we need some back pressure.
            if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack) {
                return 0;
            }

            // Back pressure needed!
            // We're not actually going to delete anything, but we're going to syncronize
            // on the deleter thread.

            if (!lock.try_lock()) {
                (void)lock.try_lock_for(boost::chrono::milliseconds(200));
            }
            return 0;
        } else {
            if (!lock.try_lock()) {
                // Someone else is deleting old records. Apply back-pressure if too far behind,
                // otherwise continue.
                if ((_dataSize.load() - _cappedMaxSize) < _cappedMaxSizeSlack)
                    return 0;

                if (!lock.try_lock_for(boost::chrono::milliseconds(200)))
                    return 0;

                // If we already waited, let someone else do cleanup unless we are significantly
                // over the limit.
                if ((_dataSize.load() - _cappedMaxSize) < (2 * _cappedMaxSizeSlack))
                    return 0;
            }
        }

        return cappedDeleteAsNeeded_inlock(txn, justInserted);
    }

    int64_t LevelRecordStore::cappedDeleteAsNeeded_inlock(OperationContext* txn,
                                                          const RecordId& justInserted) {
        // we do this is a sub transaction in case it aborts
        LevelRecoveryUnit* realRecoveryUnit =
            checked_cast<LevelRecoveryUnit*>(txn->releaseRecoveryUnit());
        invariant(realRecoveryUnit);
        OperationContext::RecoveryUnitState const realRUstate =
            txn->setRecoveryUnit(realRecoveryUnit->newLevelRecoveryUnit(),
                                 OperationContext::kNotInUnitOfWork);

        int64_t dataSize = _dataSize.load() + realRecoveryUnit->getDeltaCounter(_dataSizeKey);
        int64_t numRecords = _numRecords.load() + realRecoveryUnit->getDeltaCounter(_numRecordsKey);

        int64_t sizeOverCap = (dataSize > _cappedMaxSize) ? dataSize - _cappedMaxSize : 0;
        int64_t sizeSaved = 0;
        int64_t docsOverCap = 0, docsRemoved = 0;
        if (_cappedMaxDocs != -1 && numRecords > _cappedMaxDocs) {
            docsOverCap = numRecords - _cappedMaxDocs;
        }
        BSONObj emptyBson;

        try {
            WriteUnitOfWork wuow(txn);
            auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
            std::unique_ptr<leveldb::Iterator> iter;
            if (_isOplog) {
                // we're using _oplogKeyTracker to find which keys to delete -- this is much faster
                // because we don't need to read any values. We theoretically need values to pass
                // the document to the cappedCallback, but the callback is only using
                // documents to remove them from indexes. opLog doesn't have indexes, so there
                // should be no need for us to reconstruct the document to pass it to the callback
                iter.reset(_oplogKeyTracker->newIterator(ru));
            } else {
                iter.reset(ru->NewIterator(_prefix));
            }
            int64_t storage;
            iter->Seek(LevelRecordStore::_makeKey(_cappedOldestKeyHint, &storage));

            RecordId newestOld;
            while ((sizeSaved < sizeOverCap || docsRemoved < docsOverCap) &&
                   (docsRemoved < 20000) && iter->Valid()) {

                newestOld = _makeRecordId(iter->key());

                if (_cappedVisibilityManager->isCappedHidden(newestOld)) {
                    // this means we have an older record that hasn't been committed yet. let's
                    // wait until it gets committed before deleting
                    break;
                }

                // don't go past the record we just inserted
                if (newestOld >= justInserted) {
                    break;
                }

                if (_shuttingDown) {
                    break;
                }

                std::string key(_makePrefixedKey(_prefix, newestOld));
                if (!ru->transaction()->registerWrite(key)) {
                    log() << "got conflict truncating capped, total docs removed " << docsRemoved;
                    break;
                }

                leveldb::Slice oldValue;
                ++docsRemoved;
                if (_isOplog) {
                    // trick the callback by giving it empty bson document
                    oldValue = leveldb::Slice(emptyBson.objdata(), emptyBson.objsize());
                    // we keep data size in the value
                    sizeSaved += _oplogKeyTracker->decodeSize(iter->value());
                } else {
                    oldValue = iter->value();
                    sizeSaved += oldValue.size();
                }

                {
                    stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);
                    if (_cappedCallback) {
                        uassertStatusOK(_cappedCallback->aboutToDeleteCapped(
                            txn, newestOld, RecordData(static_cast<const char*>(oldValue.data()),
                                                       oldValue.size())));
                    }
                }

                ru->writeBatch()->Delete(key);
                ru->writeBatchRepHandler()->Delete(key);
                if (_isOplog) {
                    _oplogKeyTracker->deleteKey(ru, newestOld);
                }

                iter->Next();
            }

            if (!iter->status().ok()) {
                log() << "LevelDB iterator failure when trying to delete capped, ignoring: "
                      << redact(iter->status().ToString());
            }

            if (docsRemoved > 0) {
                _changeNumRecords(txn, -docsRemoved);
                _increaseDataSize(txn, -sizeSaved);
                wuow.commit();
            }

            if (iter->Valid()) {
                auto oldestAliveRecordId = _makeRecordId(iter->key());
                // we check if there's outstanding transaction that is older than
                // oldestAliveRecordId. If there is, we should not skip deleting that record next
                // time we clean up the capped collection. If there isn't, we know for certain this
                // is the record we'll start out deletions from next time
                if (!_cappedVisibilityManager->isCappedHidden(oldestAliveRecordId)) {
                    _cappedOldestKeyHint = oldestAliveRecordId;
                }
            }
        }
        catch ( const WriteConflictException& wce ) {
            delete txn->releaseRecoveryUnit();
            txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
            log() << "got conflict truncating capped, ignoring";
            return 0;
        }
        catch ( ... ) {
            delete txn->releaseRecoveryUnit();
            txn->setRecoveryUnit(realRecoveryUnit, realRUstate);
            throw;
        }

        delete txn->releaseRecoveryUnit();
        txn->setRecoveryUnit(realRecoveryUnit, realRUstate);

        if (_isOplog) {
            if ((_oplogSinceLastCompaction.minutes() >= kOplogCompactEveryMins) || 
            (_oplogKeyTracker->getDeletedSinceCompaction() >= kOplogCompactEveryDeletedRecords)) {
                log() << "Scheduling oplog compactions. time since last " << _oplogSinceLastCompaction.minutes() <<
                    " deleted since last " << _oplogKeyTracker->getDeletedSinceCompaction();
                _oplogSinceLastCompaction.reset();
                // schedule compaction for oplog
                std::string oldestAliveKey(_makePrefixedKey(_prefix, _cappedOldestKeyHint));
                leveldb::Slice begin(_prefix), end(oldestAliveKey);

                // schedule compaction for oplog tracker
                std::string oplogKeyTrackerPrefix(levelGetNextPrefix(_prefix));
                oldestAliveKey = _makePrefixedKey(oplogKeyTrackerPrefix, _cappedOldestKeyHint);
                begin = leveldb::Slice(oplogKeyTrackerPrefix);
                end = leveldb::Slice(oldestAliveKey);
                
                _oplogKeyTracker->resetDeletedSinceCompaction();
            }
        }

        return docsRemoved;
    }

    StatusWith<RecordId> LevelRecordStore::insertRecord( OperationContext* txn,
                                                        const char* data,
                                                        int len,
                                                        bool enforceQuota ) {

        if ( _isCapped && len > _cappedMaxSize ) {
            return StatusWith<RecordId>( ErrorCodes::BadValue,
                                       "object to insert exceeds cappedMaxSize" );
        }

        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit( txn );

        RecordId loc;
        if (_isOplog) {
            StatusWith<RecordId> status = oploghack::extractKey(data, len);
            if (!status.isOK()) {
                return status;
            }
            loc = status.getValue();
            _cappedVisibilityManager->updateHighestSeen(loc);
        } else if (_isCapped) {
            loc = _cappedVisibilityManager->getNextAndAddUncommittedRecord(
                txn, [&]() { return _nextId(); });
        } else {
            loc = _nextId();
        }

        // No need to register the write here, since we just allocated a new RecordId so no other
        // transaction can access this key before we commit
        ru->writeBatch()->Put(_makePrefixedKey(_prefix, loc), leveldb::Slice(data, len));
        ru->writeBatchRepHandler()->Add(_makePrefixedKey(_prefix, loc), leveldb::Slice(data, len));
        if (_isOplog) {
            _oplogKeyTracker->insertKey(ru, loc, len);
        }

        _changeNumRecords( txn, 1 );
        _increaseDataSize( txn, len );

        cappedDeleteAsNeeded(txn, loc);

        return StatusWith<RecordId>( loc );
    }

    Status LevelRecordStore::insertRecordsWithDocWriter(OperationContext* txn,
                                                        const DocWriter* const* docs, size_t nDocs,
                                                        RecordId* idsOut) {
        std::unique_ptr<Record[]> records(new Record[nDocs]);

        size_t totalSize = 0;
        for (size_t i = 0; i < nDocs; i++) {
            const size_t docSize = docs[i]->documentSize();
            records[i].data = RecordData(nullptr, docSize);  // We fill in the real ptr in next loop.
            totalSize += docSize;
        }

        std::unique_ptr<char[]> buffer(new char[totalSize]);
        char* pos = buffer.get();
        for (size_t i = 0; i < nDocs; i++) {
            docs[i]->writeDocument(pos);
            const size_t size = records[i].data.size();
            records[i].data = RecordData(pos, size);
            pos += size;
        }
        invariant(pos == (buffer.get() + totalSize));

        for (size_t i = 0; i < nDocs; ++i) {
	  auto s = insertRecord(txn, records[i].data.data(), records[i].data.size(), true);
            if (!s.isOK())
                return s.getStatus();
            if (idsOut)
                idsOut[i] = s.getValue();
        }

        return Status::OK();
    }

    Status LevelRecordStore::updateRecord(OperationContext* txn, const RecordId& loc,
                                          const char* data, int len, bool enforceQuota,
                                          UpdateNotifier* notifier) {
        std::string key(_makePrefixedKey(_prefix, loc));

        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit( txn );
        if (!ru->transaction()->registerWrite(key)) {
            throw WriteConflictException();
        }

        std::string old_value;

        auto status = ru->Get(key, &old_value);
        invariantLevelOK(status);

        int old_length = old_value.size();

        ru->writeBatch()->Put(key, leveldb::Slice(data, len));
        ru->writeBatchRepHandler()->Add(key, leveldb::Slice(data, len));
        if (_isOplog) {
            _oplogKeyTracker->insertKey(ru, loc, len);
        }

        _increaseDataSize(txn, len - old_length);

        cappedDeleteAsNeeded(txn, loc);

        return Status::OK();
    }

    bool LevelRecordStore::updateWithDamagesSupported() const {
        return false;
    }

    StatusWith<RecordData> LevelRecordStore::updateWithDamages(
        OperationContext* txn,
        const RecordId& loc,
        const RecordData& oldRec,
        const char* damageSource,
        const mutablebson::DamageVector& damages) {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<SeekableRecordCursor> LevelRecordStore::getCursor(OperationContext* txn,
                                                                      bool forward) const {
        if (_isOplog && forward) {
            auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
            // If we already have a snapshot we don't know what it can see, unless we know no
            // one else could be writing (because we hold an exclusive lock).
            if (ru->hasSnapshot() && !txn->lockState()->isNoop() &&
                !txn->lockState()->isCollectionLockedForMode(_ns, MODE_X)) {
                throw WriteConflictException();
            }
            ru->setOplogReadTill(_cappedVisibilityManager->oplogStartHack());
        }

        return stdx::make_unique<Cursor>(txn, _db, _prefix, _cappedVisibilityManager, forward,
                                         _isCapped);
    }

    Status LevelRecordStore::truncate(OperationContext* txn) {
        // We can't use getCursor() here because we need to ignore the visibility of records (i.e.
        // we need to delete all records, regardless of visibility)
        auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
        std::unique_ptr<LevelIterator> iterator(ru->NewIterator(_prefix, _isOplog));
        for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
            deleteRecord(txn, _makeRecordId(iterator->key()));
        }

        return levelToMongoStatus(iterator->status());
    }

    Status LevelRecordStore::compact( OperationContext* txn,
                                      RecordStoreCompactAdaptor* adaptor,
                                      const CompactOptions* options,
                                      CompactStats* stats ) {
        std::string beginString(_makePrefixedKey(_prefix, RecordId()));
        std::string endString(_makePrefixedKey(_prefix, RecordId::max()));
        leveldb::Slice beginRange(beginString);
        leveldb::Slice endRange(endString);
	_db->CompactRange(&beginRange, &endRange);
	return Status::OK();
    }

    Status LevelRecordStore::validate( OperationContext* txn,
                                       ValidateCmdLevel level,
                                       ValidateAdaptor* adaptor,
                                       ValidateResults* results,
                                       BSONObjBuilder* output ) {
        long long nrecords = 0;
        long long dataSizeTotal = 0;
        if (level == kValidateRecordStore || level == kValidateFull) {
            auto cursor = getCursor(txn, true);
            results->valid = true;
            const int interruptInterval = 4096;
            while (auto record = cursor->next()) {
                if (!(nrecords % interruptInterval))
                    txn->checkForInterrupt();
                ++nrecords;
                if (level == kValidateFull) {
                    size_t dataSize;
                    Status status = adaptor->validate(record->id, record->data, &dataSize);
                    if (!status.isOK()) {
                        results->valid = false;
                        results->errors.push_back(str::stream() << record->id << " is corrupted");
                    }
                    dataSizeTotal += static_cast<long long>(dataSize);
                }
            }

            if (level == kValidateFull && results->valid) {
                long long storedNumRecords = numRecords(txn);
                long long storedDataSize = dataSize(txn);

                if (nrecords != storedNumRecords || dataSizeTotal != storedDataSize) {
                    warning() << redact(_ident) << ": Existing record and data size counters ("
                              << storedNumRecords << " records " << storedDataSize << " bytes) "
                              << "are inconsistent with full validation results (" << nrecords
                              << " records " << dataSizeTotal << " bytes). "
                              << "Updating counters with new values.";
                    if (nrecords != storedNumRecords) {
                        _changeNumRecords(txn, nrecords - storedNumRecords);
                        _increaseDataSize(txn, dataSizeTotal - storedDataSize);
                    }
                }
            }
            output->appendNumber("nrecords", nrecords);
        } else {
            output->appendNumber("nrecords", numRecords(txn));
        }

        return Status::OK();
    }

    void LevelRecordStore::appendCustomStats( OperationContext* txn,
                                              BSONObjBuilder* result,
                                              double scale ) const {
        result->appendBool("capped", _isCapped);
        if (_isCapped) {
            result->appendIntOrLL("max", _cappedMaxDocs);
            result->appendIntOrLL("maxSize", _cappedMaxSize / scale);
        }
    }

    Status LevelRecordStore::oplogDiskLocRegister(OperationContext* txn, const Timestamp& opTime) {
        invariant(_isOplog);
        StatusWith<RecordId> record = oploghack::keyForOptime(opTime);
        if (record.isOK()) {
            _cappedVisibilityManager->addUncommittedRecord(txn, record.getValue());
        }

        return record.getStatus();
    }

    void LevelRecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const {
      _cappedVisibilityManager->waitForAllEarlierOplogWritesToBeVisible(txn);
    }

    void LevelRecordStore::updateStatsAfterRepair(OperationContext* txn, long long numRecords,
                                                  long long dataSize) {
        _numRecords.store(numRecords);
        _dataSize.store(dataSize);
        leveldb::WriteBatch wb;
        leveldb::WriteBatchRepHandler wbrh;
        _counterManager->updateCounter(_numRecordsKey, numRecords, &wb, &wbrh);
        _counterManager->updateCounter(_dataSizeKey, dataSize, &wb, &wbrh);
        if (wb.Count() > 0) {
            auto s = _db->Write(leveldb::WriteOptions(), &wb);
	    wbrh.Clear();
            invariantLevelOK(s);
        }
    }

    /**
     * Return the RecordId of an oplog entry as close to startingPosition as possible without
     * being higher. If there are no entries <= startingPosition, return RecordId().
     */
    boost::optional<RecordId> LevelRecordStore::oplogStartHack(
        OperationContext* txn, const RecordId& startingPosition) const {

        if (!_isOplog) {
            return boost::none;
        }

        auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
        ru->setOplogReadTill(_cappedVisibilityManager->oplogStartHack());

        // we use _oplogKeyTracker, which contains exactly the same keys as oplog. the difference is
        // that values are different (much smaller), so reading is faster. in this case, we only
        // need keys (we never touch the values), so this works nicely
        std::unique_ptr<leveldb::Iterator> iter(_oplogKeyTracker->newIterator(ru));
        int64_t storage;
        iter->Seek(_makeKey(startingPosition, &storage));
        if (!iter->Valid()) {
            iter->SeekToLast();
            if (iter->Valid()) {
                // startingPosition is bigger than everything else
                return _makeRecordId(iter->key());
            } else {
                invariantLevelOK(iter->status());
                // record store is empty
                return RecordId();
            }
        }

        // We're at or past target:
        // 1) if we're at -- return
        // 2) if we're past -- do a prev()
        RecordId foundKey = _makeRecordId(iter->key());
        int cmp = startingPosition.compare(foundKey);
        if (cmp != 0) {
            // LevelDB invariant -- iterator needs to land at or past target when Seek-ing
            invariant(cmp < 0);
            // we're past target -- prev()
            iter->Prev();
        }

        if (!iter->Valid()) {
            invariantLevelOK(iter->status());
            // there are no entries <= startingPosition
            return RecordId();
        }

        return _makeRecordId(iter->key());
    }

    void LevelRecordStore::temp_cappedTruncateAfter(OperationContext* txn, RecordId end,
                                               bool inclusive) {
        // copied from WiredTigerRecordStore::cappedTruncateAfter()
        WriteUnitOfWork wuow(txn);
        RecordId lastKeptId = end;
        int64_t recordsRemoved = 0;

        if (inclusive) {
            auto reverseCursor = getCursor(txn, false);
            invariant(reverseCursor->seekExact(end));
            auto prev = reverseCursor->next();
            lastKeptId = prev ? prev->id : RecordId::min();
        }

        {
            auto cursor = getCursor(txn, true);
            stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);

            for (auto record = cursor->seekExact(end); record; record = cursor->next()) {
                if (end < record->id || (inclusive && end == record->id)) {
                    if (_cappedCallback) {
                        uassertStatusOK(
                            _cappedCallback->aboutToDeleteCapped(txn, record->id, record->data));
                    }
                    deleteRecord(txn, record->id);
                    ++recordsRemoved;
                }
            }
        }

        if (recordsRemoved) {
            // Forget that we've ever seen a higher timestamp than we now have.
            _cappedVisibilityManager->setHighestSeen(lastKeptId);
        }

        wuow.commit();
    }

    RecordId LevelRecordStore::_nextId() {
        invariant(!_isOplog);
        return RecordId(_nextIdNum.fetchAndAdd(1));
    }

    leveldb::Slice LevelRecordStore::_makeKey(const RecordId& loc, int64_t* storage) {
        *storage = endian::nativeToBig(loc.repr());
        return leveldb::Slice(reinterpret_cast<const char*>(storage), sizeof(*storage));
    }

    std::string LevelRecordStore::_makePrefixedKey(const std::string& prefix, const RecordId& loc) {
        int64_t storage;
        auto encodedLoc = _makeKey(loc, &storage);
        std::string key(prefix);
        key.append(encodedLoc.data(), encodedLoc.size());
        return key;
    }

    RecordId LevelRecordStore::_makeRecordId(const leveldb::Slice& slice) {
        invariant(slice.size() == sizeof(int64_t));
        int64_t repr = endian::bigToNative(*reinterpret_cast<const int64_t*>(slice.data()));
        RecordId a(repr);
        return RecordId(repr);
    }

    bool LevelRecordStore::findRecord( OperationContext* txn,
                                       const RecordId& loc, RecordData* out ) const {

      RecordData rd = _getDataFor(_db, _prefix, txn, loc);
        if ( rd.data() == NULL )
            return false;
        *out = rd;

        return true;
    }

    RecordData LevelRecordStore::_getDataFor(leveldb::DB* db, const std::string& prefix,
                                             OperationContext* txn, const RecordId& loc) {
        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);

        std::string valueStorage;
	auto status = ru->Get(_makePrefixedKey(prefix, loc), &valueStorage);
        if (status.IsNotFound()) {
            return RecordData(nullptr, 0);
        }
        invariantLevelOK(status);

        SharedBuffer data = SharedBuffer::allocate(valueStorage.size());
        memcpy(data.get(), valueStorage.data(), valueStorage.size());

	return RecordData(data, valueStorage.size());
    }

    void LevelRecordStore::_changeNumRecords(OperationContext* txn, int64_t amount) {
        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit(txn);
        ru->incrementCounter(_numRecordsKey, &_numRecords, amount);
    }

    void LevelRecordStore::_increaseDataSize(OperationContext* txn, int64_t amount) {
        LevelRecoveryUnit* ru = LevelRecoveryUnit::getLevelRecoveryUnit( txn );
        ru->incrementCounter(_dataSizeKey, &_dataSize, amount);
    }

    // --------

    LevelRecordStore::Cursor::Cursor(
            OperationContext* txn,
            leveldb::DB* db,
            std::string prefix,
            std::shared_ptr<CappedVisibilityManager> cappedVisibilityManager,
            bool forward,
            bool isCapped)
        : _txn(txn),
          _db(db),
          _prefix(std::move(prefix)),
          _cappedVisibilityManager(cappedVisibilityManager),
          _forward(forward),
          _isCapped(isCapped),
          _readUntilForOplog(LevelRecoveryUnit::getLevelRecoveryUnit(txn)->getOplogReadTill()) {
    }

    // requires !_eof
    void LevelRecordStore::Cursor::positionIterator() {
        _skipNextAdvance = false;
        int64_t locStorage;
        _iterator->Seek(LevelRecordStore::_makeKey(_lastLoc, &locStorage));
        invariantLevelOK(_iterator->status());

        if (_forward) {
            // If _skipNextAdvance is true we landed after where we were. Return our new location on
            // the next call to next().
            _skipNextAdvance =
                !_iterator->Valid() || _lastLoc != _makeRecordId(_iterator->key());
        }
        else {
            // Seek() lands on or after the key, while reverse cursors need to land on or before.
            if (!_iterator->Valid()) {
                // Nothing left on or after.
                _iterator->SeekToLast();
                invariantLevelOK(_iterator->status());
                _skipNextAdvance = true;
            }
            else {
                if (_lastLoc != _makeRecordId(_iterator->key())) {
                    // Landed after. This is true: iterator->key() > _lastLoc
                    // Since iterator is valid and Seek() landed after key,
                    // iterator will still be valid after we call Prev().
                    _skipNextAdvance = true;
                    _iterator->Prev();
                }
            }
        }
        // _lastLoc != _makeRecordId(_iterator->key()) indicates that the record _lastLoc was
        // deleted. In this case, mark _eof only if the collection is capped.
        _eof = !_iterator->Valid() || (_isCapped && _lastLoc != _makeRecordId(_iterator->key()));
    }

    leveldb::Iterator* LevelRecordStore::Cursor::iterator() {
        if (_iterator.get() != nullptr) {
            return _iterator.get();
        }
        _iterator.reset(LevelRecoveryUnit::getLevelRecoveryUnit(_txn)
                ->NewIterator(_prefix, /* isOplog */ !_readUntilForOplog.isNull()));
        if (!_needFirstSeek) {
            positionIterator();
        }
        return _iterator.get();
    }

    boost::optional<Record> LevelRecordStore::Cursor::next() {
        if (_eof) {
            return {};
        }

        auto iter = iterator();
        // ignore _eof

        if (!_skipNextAdvance) {
            if (_needFirstSeek) {
                _needFirstSeek = false;
                if (_forward) {
                    iter->SeekToFirst();
                } else {
                    iter->SeekToLast();
                }
            } else {
                if (_forward) {
                    iter->Next();
                } else {
                    iter->Prev();
                }
            }
        }
        _skipNextAdvance = false;

        return curr();
    }

    boost::optional<Record> LevelRecordStore::Cursor::seekExact(const RecordId& id) {
        _needFirstSeek = false;
        _skipNextAdvance = false;
        _iterator.reset();

	leveldb::Status status = LevelRecoveryUnit::getLevelRecoveryUnit(_txn)
            ->Get(_makePrefixedKey(_prefix, id), &_seekExactResult);

        if (status.IsNotFound()) {
            _eof = true;
            return {};
        } else if (!status.ok()) {
            invariantLevelOK(status);
            return {};
        }

        _eof = false;
        _lastLoc = id;

        return {{_lastLoc, {_seekExactResult.data(), static_cast<int>(_seekExactResult.size())}}};
    }

    void LevelRecordStore::Cursor::save() {}

    void LevelRecordStore::Cursor::saveUnpositioned() { _eof = true; }

    bool LevelRecordStore::Cursor::restore() {
        auto ru = LevelRecoveryUnit::getLevelRecoveryUnit(_txn);
        if (!_iterator.get()) { 
            _iterator.reset(ru->NewIterator(_prefix, /* isOplog */ !_readUntilForOplog.isNull()));
        }

        _skipNextAdvance = false;

        if (_eof) return true;
        if (_needFirstSeek) return true;

        positionIterator();
        // Return false if the collection is capped and we reached an EOF. Otherwise return true.
        return _cappedVisibilityManager && _eof ? false : true;
    }

    void LevelRecordStore::Cursor::detachFromOperationContext() {
        _txn = nullptr;
        _iterator.reset();
    }

    void LevelRecordStore::Cursor::reattachToOperationContext(OperationContext* txn) {
        _txn = txn;
        // iterator recreated in restore()
    }

    boost::optional<Record> LevelRecordStore::Cursor::curr() {
        invariantLevelOK(_iterator->status());
        if (!_iterator->Valid()) {
            _eof = true;
            return {};
        }
        _eof = false;
        _lastLoc = _makeRecordId(_iterator->key());

        if (_cappedVisibilityManager && _forward) {  // isCapped and forward?
            if (_readUntilForOplog.isNull()) {
                // this is the normal capped case
                if (_cappedVisibilityManager->isCappedHidden(_lastLoc)) {
                    _eof = true;
                    return {};
                }
            } else {
                // this is for oplogs
                if (_lastLoc > _readUntilForOplog ||
                    (_lastLoc == _readUntilForOplog &&
                     _cappedVisibilityManager->isCappedHidden(_lastLoc))) {
                    _eof = true;
                    return {};
                }
            }
        }  // isCapped?

        auto dataSlice = _iterator->value();
        return {{_lastLoc, {dataSlice.data(), static_cast<int>(dataSlice.size())}}};
    }
}
