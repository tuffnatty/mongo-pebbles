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

#pragma once

#include <atomic>
#include <map>
#include <stack>
#include <string>
#include <vector>
#include <unordered_map>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <pebblesdb/slice.h>
#include <pebblesdb/write_batch.h>
//#include <leveldb/utilities/write_batch_with_index.h>

#include "mongo/base/disallow_copying.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/record_id.h"
#include "mongo/db/storage/recovery_unit.h"

#include "pebbles_compaction_scheduler.h"
#include "pebbles_transaction.h"
#include "pebbles_counter_manager.h"
#include "pebbles_snapshot_manager.h"
#include "pebbles_durability_manager.h"
#include "writebatch_iterator.h"


namespace leveldb {
    class DB;
    class Snapshot;
    class WriteBatch;
    class WriteBatchRepHandler;
    class Comparator;
    class Status;
    class Slice;
    class Iterator;
}

namespace mongo {

    // Same as leveldb::Iterator, but adds couple more useful functions
    class LevelIterator : public leveldb::Iterator {
    public:
        virtual ~LevelIterator() {}

        // This Seek is specific because it will succeed only if it finds a key with `target`
        // prefix. If there is no such key, it will be !Valid()
        virtual void SeekPrefix(const leveldb::Slice& target) = 0;
    };

    class OperationContext;

    class LevelRecoveryUnit : public RecoveryUnit {
        MONGO_DISALLOW_COPYING(LevelRecoveryUnit);
    public:
        LevelRecoveryUnit(LevelTransactionEngine* transactionEngine,
                          LevelSnapshotManager* snapshotManager, leveldb::DB* db,
                          LevelCounterManager* counterManager,
                          LevelCompactionScheduler* compactionScheduler,
                          LevelDurabilityManager* durabilityManager, bool durable);
        virtual ~LevelRecoveryUnit();

        virtual void beginUnitOfWork(OperationContext* opCtx);
        virtual void commitUnitOfWork();
        virtual void abortUnitOfWork();

        virtual bool waitUntilDurable();

        virtual void abandonSnapshot();

        Status setReadFromMajorityCommittedSnapshot() final;
        bool isReadingFromMajorityCommittedSnapshot() const final {
            return _readFromMajorityCommittedSnapshot;
        }

        boost::optional<SnapshotName> getMajorityCommittedSnapshot() const final;

        virtual void* writingPtr(void* data, size_t len) { invariant(!"don't call writingPtr"); }

        virtual void registerChange(Change* change);

        virtual void setRollbackWritesDisabled() {}

        virtual SnapshotId getSnapshotId() const;

        // local api

        leveldb::WriteBatch* writeBatch();
        leveldb::WriteBatchRepHandler* writeBatchRepHandler();

        const leveldb::Snapshot* getPreparedSnapshot();
        void dbReleaseSnapshot(const leveldb::Snapshot* snapshot);

        // Returns snapshot, creating one if needed. Considers _readFromMajorityCommittedSnapshot.
        const leveldb::Snapshot* snapshot();

        bool hasSnapshot() { return _snapshot != nullptr || _snapshotHolder.get() != nullptr; }

        LevelTransaction* transaction() { return &_transaction; }

        leveldb::Status Get(const leveldb::Slice& key, std::string* value);

        LevelIterator* NewIterator(std::string prefix, bool isOplog = false);

        static LevelIterator* NewIteratorNoSnapshot(leveldb::DB* db, std::string prefix);

        void incrementCounter(const leveldb::Slice& counterKey,
                              std::atomic<long long>* counter, long long delta);

        long long getDeltaCounter(const leveldb::Slice& counterKey);

        void setOplogReadTill(const RecordId& loc);
        RecordId getOplogReadTill() const { return _oplogReadTill; }

        LevelRecoveryUnit* newLevelRecoveryUnit() {
            return new LevelRecoveryUnit(_transactionEngine, _snapshotManager, _db, _counterManager,
                                         _compactionScheduler, _durabilityManager, _durable);
        }

        struct Counter {
            std::atomic<long long>* _value;
            long long _delta;
            Counter() : Counter(nullptr, 0) {}
            Counter(std::atomic<long long>* value, long long delta) : _value(value),
                                                                      _delta(delta) {}
        };

        typedef std::unordered_map<std::string, Counter> CounterMap;

        static LevelRecoveryUnit* getLevelRecoveryUnit(OperationContext* opCtx);

        static int getTotalLiveRecoveryUnits() { return _totalLiveRecoveryUnits.load(); }

        void prepareForCreateSnapshot(OperationContext* opCtx);

        void setCommittedSnapshot(const leveldb::Snapshot* committedSnapshot);

        leveldb::DB* getDB() const { return _db; }

    private:
        void _releaseSnapshot();

        void _commit();

        void _abort();
        LevelTransactionEngine* _transactionEngine;      // not owned
        LevelSnapshotManager* _snapshotManager;          // not owned
        leveldb::DB* _db;                                // not owned
        LevelCounterManager* _counterManager;            // not owned
        LevelCompactionScheduler* _compactionScheduler;  // not owned
        LevelDurabilityManager* _durabilityManager;      // not owned

        const bool _durable;

        LevelTransaction _transaction;

        leveldb::WriteBatch _writeBatch;
	leveldb::WriteBatchRepHandler _writeBatchRepHandler;

        // bare because we need to call ReleaseSnapshot when we're done with this
        const leveldb::Snapshot* _snapshot; // owned

        // snapshot that got prepared in prepareForCreateSnapshot
        // it is consumed by getPreparedSnapshot()
        const leveldb::Snapshot* _preparedSnapshot;  // owned

        CounterMap _deltaCounters;

        typedef OwnedPointerVector<Change> Changes;
        Changes _changes;

        uint64_t _myTransactionCount;

        RecordId _oplogReadTill;

        static std::atomic<int> _totalLiveRecoveryUnits;

        // If we read from a committed snapshot, then ownership of the snapshot
        // should be shared here to ensure that it is not released early
        std::shared_ptr<LevelSnapshotManager::SnapshotHolder> _snapshotHolder;

        bool _readFromMajorityCommittedSnapshot = false;
        bool _areWriteUnitOfWorksBanned = false;
    };

}
