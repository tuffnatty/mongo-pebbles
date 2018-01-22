/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#include "pebbles_recovery_unit.h"

#include <pebblesdb/comparator.h>
#include <pebblesdb/db.h>
#include <pebblesdb/iterator.h>
#include <pebblesdb/slice.h>
#include <pebblesdb/options.h>
#include <pebblesdb/write_batch.h>

#include "mongo/base/checked_cast.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/util/log.h"

#include "pebbles_transaction.h"
#include "pebbles_util.h"

#include "pebbles_snapshot_manager.h"

namespace mongo {
  namespace {
    class PrefixStrippingIterator : public LevelIterator {
    public:
      // baseIterator is consumed
      PrefixStrippingIterator(std::string prefix, leveldb::Iterator* baseIterator,
			      LevelCompactionScheduler* compactionScheduler,
			      std::unique_ptr<leveldb::Slice> upperBound)
	: _leveldbSkippedDeletionsInitial(0),
	  _prefix(std::move(prefix)),
	  _nextPrefix(levelGetNextPrefix(_prefix)),
	  _prefixSlice(_prefix.data(), _prefix.size()),
	  _prefixSliceEpsilon(_prefix.data(), _prefix.size() + 1),
	  _baseIterator(baseIterator),
	  _compactionScheduler(compactionScheduler),
	  _upperBound(std::move(upperBound)) {
	*_upperBound.get() = leveldb::Slice(_nextPrefix);
      }

      ~PrefixStrippingIterator() {}

      virtual bool Valid() const {
	return _baseIterator->Valid() && _baseIterator->key().starts_with(_prefixSlice) &&
	  _baseIterator->key().size() > _prefixSlice.size();
      }

      virtual void SeekToFirst() {
	startOp();
	// seek to first key bigger than prefix
	_baseIterator->Seek(_prefixSliceEpsilon);
	endOp();
      }
      virtual void SeekToLast() {
	startOp();
	// we can't have upper bound set to _nextPrefix since we need to seek to it
	*_upperBound.get() = leveldb::Slice("\xFF\xFF\xFF\xFF");
	_baseIterator->Seek(_nextPrefix);
	// reset back to original value
	*_upperBound.get() = leveldb::Slice(_nextPrefix);
	if (!_baseIterator->Valid()) {
	  _baseIterator->SeekToLast();
	}
	if (_baseIterator->Valid() && !_baseIterator->key().starts_with(_prefixSlice)) {
	  _baseIterator->Prev();
	}
	endOp();
      }

      virtual void Seek(const leveldb::Slice& target) {
	startOp();
	std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size()]);
	memcpy(buffer.get(), _prefix.data(), _prefix.size());
	memcpy(buffer.get() + _prefix.size(), target.data(), target.size());
	_baseIterator->Seek(leveldb::Slice(buffer.get(), _prefix.size() + target.size()));
	endOp();
      }

      virtual void Next() {
	startOp();
	_baseIterator->Next();
	endOp();
      }

      virtual void Prev() {
	startOp();
	_baseIterator->Prev();
	endOp();
      }

      virtual void SeekForPrev(const leveldb::Slice& target) {
      }

      virtual leveldb::Slice key() const {
	leveldb::Slice strippedKey = _baseIterator->key();
	strippedKey.remove_prefix(_prefix.size());
	return strippedKey;
      }
      
      virtual leveldb::Slice value() const { return _baseIterator->value(); }
      virtual const leveldb::Status& status() const { return _baseIterator->status(); }

      // LevelIterator specific functions

      // This Seek is specific because it will succeed only if it finds a key with `target`
      // prefix. If there is no such key, it will be !Valid()
      virtual void SeekPrefix(const leveldb::Slice& target) {
	std::unique_ptr<char[]> buffer(new char[_prefix.size() + target.size()]);
	memcpy(buffer.get(), _prefix.data(), _prefix.size());
	memcpy(buffer.get() + _prefix.size(), target.data(), target.size());

	std::string tempUpperBound = levelGetNextPrefix(
							leveldb::Slice(buffer.get(), _prefix.size() + target.size()));

	*_upperBound.get() = leveldb::Slice(tempUpperBound);
	if (target.size() == 0) {
	  // if target is empty, we'll try to seek to <prefix>, which is not good
	  _baseIterator->Seek(_prefixSliceEpsilon);
	} else {
	  _baseIterator->Seek(
			      leveldb::Slice(buffer.get(), _prefix.size() + target.size()));
	}
	// reset back to original value
	*_upperBound.get() = leveldb::Slice(_nextPrefix);
      }

    private:
      void startOp() {
	if (_compactionScheduler == nullptr) {
	  return;
	}
      }
      void endOp() {
	if (_compactionScheduler == nullptr) {
	  return;
	}
      }

      int _leveldbSkippedDeletionsInitial;

      std::string _prefix;
      std::string _nextPrefix;
      leveldb::Slice _prefixSlice;
      // the first possible key bigger than prefix. we use this for SeekToFirst()
      leveldb::Slice _prefixSliceEpsilon;
      std::unique_ptr<leveldb::Iterator> _baseIterator;

      // can be nullptr
      LevelCompactionScheduler* _compactionScheduler;  // not owned

      std::unique_ptr<leveldb::Slice> _upperBound;
    };

  }  // anonymous namespace

  std::atomic<int> LevelRecoveryUnit::_totalLiveRecoveryUnits(0);

  LevelRecoveryUnit::LevelRecoveryUnit(LevelTransactionEngine* transactionEngine,
				       LevelSnapshotManager* snapshotManager, leveldb::DB* db,
				       LevelCounterManager* counterManager,
				       LevelCompactionScheduler* compactionScheduler,
				       LevelDurabilityManager* durabilityManager,
				       bool durable)
    : _transactionEngine(transactionEngine),
      _snapshotManager(snapshotManager),
      _db(db),
      _counterManager(counterManager),
      _compactionScheduler(compactionScheduler),
      _durabilityManager(durabilityManager),
      _durable(durable),
      _transaction(transactionEngine),
      _writeBatch(),
      _writeBatchRepHandler(),
      _snapshot(nullptr),
      _preparedSnapshot(nullptr),
      _myTransactionCount(1) {
    LevelRecoveryUnit::_totalLiveRecoveryUnits.fetch_add(1, std::memory_order_relaxed);
  }

  LevelRecoveryUnit::~LevelRecoveryUnit() {
    if (_preparedSnapshot) {
      _db->ReleaseSnapshot(_preparedSnapshot);
      _preparedSnapshot = nullptr;
    }
    _abort();
    LevelRecoveryUnit::_totalLiveRecoveryUnits.fetch_sub(1, std::memory_order_relaxed);
  }

  void LevelRecoveryUnit::beginUnitOfWork(OperationContext* opCtx) {
    invariant(!_areWriteUnitOfWorksBanned);
  }

  void LevelRecoveryUnit::commitUnitOfWork() {
    if (_writeBatch.Count() > 0) {
      _commit();
    }

    try {
      for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
	   ++it) {
	(*it)->commit();
      }
      _changes.clear();
    }
    catch (...) {
      std::terminate();
    }

    _releaseSnapshot();
  }

  void LevelRecoveryUnit::abortUnitOfWork() {
    _abort();
  }

  bool LevelRecoveryUnit::waitUntilDurable() {
    _durabilityManager->waitUntilDurable(false);
    return true;
  }

  void LevelRecoveryUnit::abandonSnapshot() {
    _deltaCounters.clear();
    _writeBatch.Clear();
    _writeBatchRepHandler.Clear();
    _releaseSnapshot();
    _areWriteUnitOfWorksBanned = false;
  }

  leveldb::WriteBatch* LevelRecoveryUnit::writeBatch() { return &_writeBatch; }
  leveldb::WriteBatchRepHandler* LevelRecoveryUnit::writeBatchRepHandler() { return &_writeBatchRepHandler; }
  
  void LevelRecoveryUnit::setOplogReadTill(const RecordId& record) { _oplogReadTill = record; }

  void LevelRecoveryUnit::registerChange(Change* change) { _changes.push_back(change); }

  Status LevelRecoveryUnit::setReadFromMajorityCommittedSnapshot() {
    if (!_snapshotManager->haveCommittedSnapshot()) {
      return {ErrorCodes::ReadConcernMajorityNotAvailableYet,
	  "Read concern majority reads are currently not possible."};
    }
    invariant(_snapshot == nullptr);

    _readFromMajorityCommittedSnapshot = true;
    return Status::OK();
  }

  boost::optional<SnapshotName> LevelRecoveryUnit::getMajorityCommittedSnapshot() const {
    if (!_readFromMajorityCommittedSnapshot)
      return {};
    return SnapshotName(_snapshotManager->getCommittedSnapshot().get()->name);
  }

  SnapshotId LevelRecoveryUnit::getSnapshotId() const { return SnapshotId(_myTransactionCount); }

  void LevelRecoveryUnit::_releaseSnapshot() {
    if (_snapshot) {
      _transaction.abort();
      _db->ReleaseSnapshot(_snapshot);
      _snapshot = nullptr;
    }
    _snapshotHolder.reset();

    _myTransactionCount++;
  }

  void LevelRecoveryUnit::prepareForCreateSnapshot(OperationContext* opCtx) {
    invariant(!_readFromMajorityCommittedSnapshot);
    _areWriteUnitOfWorksBanned = true;
    if (_preparedSnapshot) {
      _db->ReleaseSnapshot(_preparedSnapshot);
    }
    _preparedSnapshot = _db->GetSnapshot();
  }

  void LevelRecoveryUnit::_commit() {
    leveldb::WriteBatch *wb = &_writeBatch;
    leveldb::WriteBatchRepHandler *wbrh = &_writeBatchRepHandler;
    for (auto pair : _deltaCounters) {
      auto& counter = pair.second;
      counter._value->fetch_add(counter._delta, std::memory_order::memory_order_relaxed);
      long long newValue = counter._value->load(std::memory_order::memory_order_relaxed);
      _counterManager->updateCounter(pair.first, newValue, wb, wbrh);
    }

    if (wb->Count() != 0) {
      // Order of operations here is important. It needs to be synchronized with
      // _transaction.recordSnapshotId() and _db->GetSnapshot() and
      leveldb::WriteOptions writeOptions;
      auto status = _db->Write(writeOptions, wb);
      wbrh->Clear();
      invariantLevelOK(status);
      _transaction.commit();
    }
    _deltaCounters.clear();
    _writeBatch.Clear();
    _writeBatchRepHandler.Clear();
  }

  void LevelRecoveryUnit::_abort() {
    try {
      for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
	   it != end; ++it) {
	Change* change = *it;
	LOG(2) << "CUSTOM ROLLBACK " << redact(demangleName(typeid(*change)));
	change->rollback();
      }
      _changes.clear();
    }
    catch (...) {
      std::terminate();
    }

    _deltaCounters.clear();
    _writeBatch.Clear();
    _writeBatchRepHandler.Clear();
    _releaseSnapshot();
  }

  const leveldb::Snapshot* LevelRecoveryUnit::getPreparedSnapshot() {
    auto ret = _preparedSnapshot;
    _preparedSnapshot = nullptr;
    return ret;
  }

  void LevelRecoveryUnit::dbReleaseSnapshot(const leveldb::Snapshot* snapshot) {
    _db->ReleaseSnapshot(snapshot);
  }

  const leveldb::Snapshot* LevelRecoveryUnit::snapshot() {
    if (_readFromMajorityCommittedSnapshot) {
      if (_snapshotHolder.get() == nullptr) {
	_snapshotHolder = _snapshotManager->getCommittedSnapshot();
      }
      return _snapshotHolder->snapshot;
    }
    if (!_snapshot) {
      // RecoveryUnit might be used for writing, so we need to call recordSnapshotId().
      // Order of operations here is important. It needs to be synchronized with
      // _db->Write() and _transaction.commit()
      _transaction.recordSnapshotId();
      _snapshot = _db->GetSnapshot();
    }
    return _snapshot;
  }

  leveldb::Status LevelRecoveryUnit::Get(const leveldb::Slice& key, std::string* value) {
    if (_writeBatch.Count() > 0) {
      leveldb::ReadOptions options;
      std::string temp_val;
      bool is_present = false;
      if(!((temp_val = _writeBatchRepHandler.Find(key, &is_present)).empty()) && is_present == true) {
	*value = temp_val;
	return leveldb::Status::OK();
      }
      
      if(is_present == true)
	return leveldb::Status::NotFound(key);
    }
    
    leveldb::ReadOptions options;
    options.snapshot = snapshot();

    return _db->Get(options, key, value);   
  }

  LevelIterator* LevelRecoveryUnit::NewIterator(std::string prefix, bool isOplog) {
    std::unique_ptr<leveldb::Slice> upperBound(new leveldb::Slice());
    leveldb::ReadOptions options;
    options.snapshot = snapshot();
    leveldb::Iterator *iterator = _db->NewIterator(options);
    auto prefixIterator = new PrefixStrippingIterator(std::move(prefix), iterator,
						      isOplog ? nullptr : _compactionScheduler,
						      std::move(upperBound));
    return prefixIterator;
  }

  LevelIterator* LevelRecoveryUnit::NewIteratorNoSnapshot(leveldb::DB* db, std::string prefix) {
    std::unique_ptr<leveldb::Slice> upperBound(new leveldb::Slice());
    leveldb::ReadOptions options;
    leveldb::Iterator *iterator = db->NewIterator(leveldb::ReadOptions());
    return new PrefixStrippingIterator(std::move(prefix), iterator, nullptr,
				       std::move(upperBound));
  }

  void LevelRecoveryUnit::incrementCounter(const leveldb::Slice& counterKey,
					   std::atomic<long long>* counter, long long delta) {
    if (delta == 0) {
      return;
    }

    auto pair = _deltaCounters.find(counterKey.ToString());
    if (pair == _deltaCounters.end()) {
      _deltaCounters[counterKey.ToString()] =
	mongo::LevelRecoveryUnit::Counter(counter, delta);
    } else {
      pair->second._delta += delta;
    }
  }

  long long LevelRecoveryUnit::getDeltaCounter(const leveldb::Slice& counterKey) {
    auto counter = _deltaCounters.find(counterKey.ToString());
    if (counter == _deltaCounters.end()) {
      return 0;
    } else {
      return counter->second._delta;
    }
  }

  LevelRecoveryUnit* LevelRecoveryUnit::getLevelRecoveryUnit(OperationContext* opCtx) {
    return checked_cast<LevelRecoveryUnit*>(opCtx->recoveryUnit());
  }
}
