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

#include "mongo/db/storage/sorted_data_interface.h"

#include <atomic>
#include <boost/shared_ptr.hpp>
#include <string>

#include <hyperleveldb/db.h>

#include "mongo/bson/ordering.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"

#pragma once

namespace leveldb {
    class DB;
}

namespace mongo {

    class LevelRecoveryUnit;

    class LevelIndexBase : public SortedDataInterface {
        MONGO_DISALLOW_COPYING(LevelIndexBase);

    public:
        LevelIndexBase(leveldb::DB* db, std::string prefix, std::string ident, Ordering order,
                       const BSONObj& config);

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                           bool dupsAllowed) = 0;

        virtual void fullValidate(OperationContext* txn, long long* numKeysOut,
                                  ValidateResults* fullResults) const;

        virtual bool appendCustomStats(OperationContext* txn, BSONObjBuilder* output,
                                       double scale) const {
            // nothing to say here, really
            return false;
        }

        virtual bool isEmpty(OperationContext* txn);

        virtual Status initAsEmpty(OperationContext* txn);

        virtual long long getSpaceUsedBytes( OperationContext* txn ) const;

        static void generateConfig(BSONObjBuilder* configBuilder, int formatVersion,
                                   IndexDescriptor::IndexVersion descVersion);

    protected:
        static std::string _makePrefixedKey(const std::string& prefix, const KeyString& encodedKey);

        leveldb::DB* _db; // not owned

        // Each key in the index is prefixed with _prefix
        std::string _prefix;
        std::string _ident;

        // very approximate index storage size
        std::atomic<long long> _indexStorageSize;

        // used to construct LevelCursors
        const Ordering _order;
        KeyString::Version _keyStringVersion;

        class StandardBulkBuilder;
        class UniqueBulkBuilder;
        friend class UniqueBulkBuilder;
    };

    class LevelUniqueIndex : public LevelIndexBase {
    public:
        LevelUniqueIndex(leveldb::DB* db, std::string prefix, std::string ident, Ordering order,
                         const BSONObj& config);

        virtual Status insert(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                              bool dupsAllowed);
        virtual void unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn,
                                                                       bool forward) const;

        virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc);

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                           bool dupsAllowed) override;
    };

    class LevelStandardIndex : public LevelIndexBase {
    public:
        LevelStandardIndex(leveldb::DB* db, std::string prefix, std::string ident, Ordering order,
                           const BSONObj& config);

        virtual Status insert(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                              bool dupsAllowed);
        virtual void unindex(OperationContext* txn, const BSONObj& key, const RecordId& loc,
                             bool dupsAllowed);
        virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn,
                                                                       bool forward) const;
        virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc) {
            // dupKeyCheck shouldn't be called for non-unique indexes
            invariant(false);
        }

        virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn,
                                                           bool dupsAllowed) override;

        void enableSingleDelete() { useSingleDelete = true; }

    private:
        bool useSingleDelete;
    };

} // namespace mongo
