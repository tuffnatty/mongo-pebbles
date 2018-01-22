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

#include "pebbles_engine.h"

#include <algorithm>
#include <mutex>

#include <boost/filesystem/operations.hpp>

#include <pebblesdb/cache.h>
#include <pebblesdb/comparator.h>
#include <pebblesdb/db.h>
#include <pebblesdb/options.h>
#include <pebblesdb/filter_policy.h>

#include "mongo/db/client.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage/journal_listener.h"
#include "mongo/platform/endian.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

#include "pebbles_counter_manager.h"
#include "pebbles_global_options.h"
#include "pebbles_record_store.h"
#include "pebbles_recovery_unit.h"
#include "pebbles_index.h"
#include "pebbles_util.h"

#define LEVEL_TRACE log()

namespace mongo {

  class LevelEngine::LevelJournalFlusher : public BackgroundJob {
  public:
    explicit LevelJournalFlusher(LevelDurabilityManager* durabilityManager)
      : BackgroundJob(false /* deleteSelf */), _durabilityManager(durabilityManager) {}

    virtual std::string name() const { return "LevelJournalFlusher"; }

    virtual void run() {
      Client::initThread(name().c_str());

      LOG(1) << "starting " << name() << " thread";

      while (!_shuttingDown.load()) {
	try {
	  _durabilityManager->waitUntilDurable(false);
	} catch (const UserException& e) {
	  invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
	}

	int ms = storageGlobalParams.journalCommitIntervalMs.load();
	if (!ms) {
	  ms = 100;
	}

	sleepmillis(ms);
      }
      LOG(1) << "stopping " << name() << " thread";
    }

    void shutdown() {
      _shuttingDown.store(true);
      wait();
    }

  private:
    LevelDurabilityManager* _durabilityManager;  // not owned
    std::atomic<bool> _shuttingDown{false};      // NOLINT
  };

  namespace {
    // we encode prefixes in big endian because we want to quickly jump to the max prefix
    bool extractPrefix(const leveldb::Slice& slice, uint32_t* prefix) {
      if (slice.size() < sizeof(uint32_t)) {
	return false;
      }
      *prefix = endian::bigToNative(*reinterpret_cast<const uint32_t*>(slice.data()));
      return true;
    }

    std::string encodePrefix(uint32_t prefix) {
      uint32_t bigEndianPrefix = endian::nativeToBig(prefix);
      return std::string(reinterpret_cast<const char*>(&bigEndianPrefix), sizeof(uint32_t));
    }
        
    // ServerParameter to limit concurrency, to prevent thousands of threads running
    // concurrent searches and thus blocking the entire DB.
    class LevelTicketServerParameter : public ServerParameter {
      MONGO_DISALLOW_COPYING(LevelTicketServerParameter);

    public:
      LevelTicketServerParameter(TicketHolder* holder, const std::string& name)
	: ServerParameter(ServerParameterSet::getGlobal(), name, true, true), _holder(holder) {};
      virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name) {
	b.append(name, _holder->outof());
      }
      virtual Status set(const BSONElement& newValueElement) {
	if (!newValueElement.isNumber())
	  return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
	return _set(newValueElement.numberInt());
      }
      virtual Status setFromString(const std::string& str) {
	int num = 0;
	Status status = parseNumberFromString(str, &num);
	if (!status.isOK())
	  return status;
	return _set(num);
      }
	
    private:
      Status _set(int newNum) {
	if (newNum <= 0) {
	  return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
	}

	return _holder->resize(newNum);
      }
            
      TicketHolder* _holder;
    };

    TicketHolder openWriteTransaction(128);
    LevelTicketServerParameter openWriteTransactionParam(&openWriteTransaction,
							 "leveldbConcurrentWriteTransactions");

    TicketHolder openReadTransaction(128);
    LevelTicketServerParameter openReadTransactionParam(&openReadTransaction,
							"leveldbConcurrentReadTransactions");

  }  // anonymous namespace

  // first four bytes are the default prefix 0
  const std::string LevelEngine::kMetadataPrefix("\0\0\0\0metadata-", 12);
  const std::string LevelEngine::kDroppedPrefix("\0\0\0\0droppedprefix-", 18);

  LevelEngine::LevelEngine(const std::string& path, bool durable, int formatVersion,
			   bool readOnly)
    : _path(path), _durable(durable), _formatVersion(formatVersion), _maxPrefix(0) {
    {  // create block cache
      uint64_t cacheSizeGB = levelGlobalOptions.cacheSizeGB;
      if (cacheSizeGB == 0) {
	ProcessInfo pi;
	unsigned long long memSizeMB = pi.getMemSizeMB();
	if (memSizeMB > 0) {
	  // reserve 1GB for system and binaries, and use 30% of the rest
	  double cacheMB = (memSizeMB - 1024) * 0.3;
	  cacheSizeGB = static_cast<uint64_t>(cacheMB / 1024);
	}
	if (cacheSizeGB < 1) {
	  cacheSizeGB = 1;
	}
      }
      _block_cache = leveldb::NewLRUCache(8 * 1024 * 1024);
    }
    _maxWriteMBPerSec = levelGlobalOptions.maxWriteMBPerSec;

    // open DB
    leveldb::DB* db;
    leveldb::Status s;
    s = leveldb::DB::Open(_options(), path, &db);
    invariantLevelOK(s);
    _db.reset(db);

    _counterManager.reset(new LevelCounterManager(_db.get(), levelGlobalOptions.crashSafeCounters));
    _compactionScheduler.reset(new LevelCompactionScheduler(_db.get()));

    // open iterator
    std::unique_ptr<leveldb::Iterator> iter(_db->NewIterator(leveldb::ReadOptions()));

    // find maxPrefix
    iter->SeekToLast();
    if (iter->Valid()) {
      // otherwise the DB is empty, so we just keep it at 0
      bool ok = extractPrefix(iter->key(), &_maxPrefix);
      // this is DB corruption here
      invariant(ok);
    }

    // load ident to prefix map. also update _maxPrefix if there's any prefix bigger than
    // current _maxPrefix
    {
      stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
      for (iter->Seek(kMetadataPrefix);
	   iter->Valid() && iter->key().starts_with(kMetadataPrefix); iter->Next()) {
	invariantLevelOK(iter->status());
	leveldb::Slice ident(iter->key());
	ident.remove_prefix(kMetadataPrefix.size());
	// this could throw DBException, which then means DB corruption. We just let it fly
	// to the caller
	BSONObj identConfig(iter->value().data());
	BSONElement element = identConfig.getField("prefix");

	if (element.eoo() || !element.isNumber()) {
	  log() << "Mongo metadata in Leveldb database is corrupted.";
	  invariant(false);
	}
	uint32_t identPrefix = static_cast<uint32_t>(element.numberInt());

	_identMap[StringData(ident.data(), ident.size())] =
	  identConfig.getOwned();

	_maxPrefix = std::max(_maxPrefix, identPrefix);
      }
    }

    // just to be extra sure. we need this if last collection is oplog -- in that case we
    // reserve prefix+1 for oplog key tracker
    ++_maxPrefix;

    // load dropped prefixes
    {
      leveldb::WriteBatch wb;
      leveldb::WriteBatchRepHandler wbrh;
      // we will use this iter to check if prefixes are still alive
      std::unique_ptr<leveldb::Iterator> prefixIter(_db->NewIterator(leveldb::ReadOptions()));
      for (iter->Seek(kDroppedPrefix);
	   iter->Valid() && iter->key().starts_with(kDroppedPrefix); iter->Next()) {
	invariantLevelOK(iter->status());
	leveldb::Slice prefix(iter->key());
	prefix.remove_prefix(kDroppedPrefix.size());
	prefixIter->Seek(prefix);
	invariantLevelOK(iter->status());
	if (prefixIter->Valid() && prefixIter->key().starts_with(prefix)) {
	  // prefix is still alive, let's instruct the compaction filter to clear it up
	  uint32_t int_prefix;
	  bool ok = extractPrefix(prefix, &int_prefix);
	  invariant(ok);
	  {
	    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
	    _droppedPrefixes.insert(int_prefix);
	  }
	} else {
	  // prefix is no longer alive. let's remove the prefix from our dropped prefixes
	  // list
	  wb.Delete(iter->key());
	  wbrh.Delete(iter->key());	  
	}
      }
      if (wb.Count() > 0) {
	auto s = _db->Write(leveldb::WriteOptions(), &wb);
	wbrh.Clear();
	invariantLevelOK(s);
      }
    }

    _durabilityManager.reset(new LevelDurabilityManager(_db.get(), _durable));

    if (_durable) {
      _journalFlusher = stdx::make_unique<LevelJournalFlusher>(_durabilityManager.get());
      _journalFlusher->go();
    }

    Locker::setGlobalThrottling(&openReadTransaction, &openWriteTransaction);
  }

  LevelEngine::~LevelEngine() { cleanShutdown(); }

  void LevelEngine::appendGlobalStats(BSONObjBuilder& b) {
    BSONObjBuilder bb(b.subobjStart("concurrentTransactions"));
    {
      BSONObjBuilder bbb(bb.subobjStart("write"));
      bbb.append("out", openWriteTransaction.used());
      bbb.append("available", openWriteTransaction.available());
      bbb.append("totalTickets", openWriteTransaction.outof());
      bbb.done();
    }
    {
      BSONObjBuilder bbb(bb.subobjStart("read"));
      bbb.append("out", openReadTransaction.used());
      bbb.append("available", openReadTransaction.available());
      bbb.append("totalTickets", openReadTransaction.outof());
      bbb.done();
    }
    bb.done();
  }

  RecoveryUnit* LevelEngine::newRecoveryUnit() {
    return new LevelRecoveryUnit(&_transactionEngine, &_snapshotManager, _db.get(),
				 _counterManager.get(), _compactionScheduler.get(),
				 _durabilityManager.get(), _durable);
  }

  Status LevelEngine::createRecordStore(OperationContext* opCtx, StringData ns, StringData ident,
					const CollectionOptions& options) {
    BSONObjBuilder configBuilder;
    auto s = _createIdent(ident, &configBuilder);
    if (s.isOK() && NamespaceString::oplog(ns)) {
      _oplogIdent = ident.toString();
      // oplog needs two prefixes, so we also reserve the next one
      uint64_t oplogTrackerPrefix = 0;
      {
	stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
	oplogTrackerPrefix = ++_maxPrefix;
      }
      // we also need to write out the new prefix to the database. this is just an
      // optimization
      std::string encodedPrefix(encodePrefix(oplogTrackerPrefix));
      s = levelToMongoStatus(_db->Put(leveldb::WriteOptions(), encodedPrefix, leveldb::Slice()));
    }
    return s;
  }

  std::unique_ptr<RecordStore> LevelEngine::getRecordStore(OperationContext* opCtx, StringData ns,
							   StringData ident, const CollectionOptions& options) {
    if (NamespaceString::oplog(ns)) {
      _oplogIdent = ident.toString();
    }

    auto config = _getIdentConfig(ident);
    std::string prefix = _extractPrefix(config);

    std::unique_ptr<LevelRecordStore> recordStore =
      options.capped
      ? stdx::make_unique<LevelRecordStore>(
					    ns, ident, _db.get(), _counterManager.get(), _durabilityManager.get(), prefix,
					    true, options.cappedSize ? options.cappedSize : 4096,  // default size
					    options.cappedMaxDocs ? options.cappedMaxDocs : -1)
      : stdx::make_unique<LevelRecordStore>(ns, ident, _db.get(), _counterManager.get(),
					    _durabilityManager.get(), prefix);

    {
      stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
      _identCollectionMap[ident] = recordStore.get();
    }
    return std::move(recordStore);
  }

  Status LevelEngine::createSortedDataInterface(OperationContext* opCtx, StringData ident,
						const IndexDescriptor* desc) {
    BSONObjBuilder configBuilder;
    // let index add its own config things
    LevelIndexBase::generateConfig(&configBuilder, _formatVersion, desc->version());
    return _createIdent(ident, &configBuilder);
  }

  SortedDataInterface* LevelEngine::getSortedDataInterface(OperationContext* opCtx,
							   StringData ident,
							   const IndexDescriptor* desc) {

    auto config = _getIdentConfig(ident);
    std::string prefix = _extractPrefix(config);

    LevelIndexBase* index;
    if (desc->unique()) {
      index = new LevelUniqueIndex(_db.get(), prefix, ident.toString(),
				   Ordering::make(desc->keyPattern()), std::move(config));
    } else {
      auto si = new LevelStandardIndex(_db.get(), prefix, ident.toString(),
				       Ordering::make(desc->keyPattern()), std::move(config));
      if (levelGlobalOptions.singleDeleteIndex) {
	si->enableSingleDelete();
      }
      index = si;
    }
    {
      stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);
      _identIndexMap[ident] = index;
    }
    return index;
  }

  // cannot be rolled back
  Status LevelEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    leveldb::WriteBatch wb;
    leveldb::WriteBatchRepHandler wbrh;
    wb.Delete(kMetadataPrefix + ident.toString());
    wbrh.Delete(kMetadataPrefix + ident.toString());
    
    // calculate which prefixes we need to drop
    std::vector<std::string> prefixesToDrop;
    prefixesToDrop.push_back(_extractPrefix(_getIdentConfig(ident)));
    if (_oplogIdent == ident.toString()) {
      // if we're dropping oplog, we also need to drop keys from LevelOplogKeyTracker (they
      // are stored at prefix+1)
      prefixesToDrop.push_back(levelGetNextPrefix(prefixesToDrop[0]));
    }

    // We record the fact that we're deleting this prefix. That way we ensure that the prefix is
    // always deleted
    for (const auto& prefix : prefixesToDrop) {
      wb.Put(kDroppedPrefix + prefix, "");
      wbrh.Add(kDroppedPrefix + prefix, "");
    }

    // we need to make sure this is on disk before starting to delete data in compactions
    leveldb::WriteOptions syncOptions;
    syncOptions.sync = true;
    auto s = _db->Write(syncOptions, &wb);
    
    wbrh.Clear();
    if (!s.ok()) {
      return levelToMongoStatus(s);
    }

    // remove from map
    {
      stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
      _identMap.erase(ident);
    }

    // instruct compaction filter to start deleting
    {
      stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
      for (const auto& prefix : prefixesToDrop) {
	uint32_t int_prefix;
	bool ok = extractPrefix(prefix, &int_prefix);
	invariant(ok);
	_droppedPrefixes.insert(int_prefix);
      }
    }

    // Suggest compaction for the prefixes that we need to drop, So that
    // we free space as fast as possible.
    for (auto& prefix : prefixesToDrop) {
      std::string end_prefix_str = levelGetNextPrefix(prefix);

      leveldb::Slice start_prefix = prefix;
      leveldb::Slice end_prefix = end_prefix_str;
    }

    return Status::OK();
  }

  bool LevelEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    return _identMap.find(ident) != _identMap.end();
  }

  std::vector<std::string> LevelEngine::getAllIdents(OperationContext* opCtx) const {
    std::vector<std::string> indents;
    for (auto& entry : _identMap) {
      indents.push_back(entry.first);
    }
    return indents;
  }

  void LevelEngine::cleanShutdown() {
    if (_journalFlusher) {
      _journalFlusher->shutdown();
      _journalFlusher.reset();
    }
    _durabilityManager.reset();
    _snapshotManager.dropAllSnapshots();
    _counterManager->sync();
    _counterManager.reset();
    _compactionScheduler.reset();
    _db.reset();
  }

  void LevelEngine::setJournalListener(JournalListener* jl) {
    _durabilityManager->setJournalListener(jl);
  }

  int64_t LevelEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    stdx::lock_guard<stdx::mutex> lk(_identObjectMapMutex);

    auto indexIter = _identIndexMap.find(ident);
    if (indexIter != _identIndexMap.end()) {
      return static_cast<int64_t>(indexIter->second->getSpaceUsedBytes(opCtx));
    }
    auto collectionIter = _identCollectionMap.find(ident);
    if (collectionIter != _identCollectionMap.end()) {
      return collectionIter->second->storageSize(opCtx);
    }

    // this can only happen if collection or index exists, but it's not opened (i.e.
    // getRecordStore or getSortedDataInterface are not called)
    return 1;
  }

  //int LevelEngine::flushAllFiles(OperationContext* txn, bool sync) {
  int LevelEngine::flushAllFiles(bool sync) {
    LOG(1) << "LevelEngine::flushAllFiles";
    _counterManager->sync();
    _durabilityManager->waitUntilDurable(true);
    return 1;
  }

  void LevelEngine::setMaxWriteMBPerSec(int maxWriteMBPerSec) {
    _maxWriteMBPerSec = maxWriteMBPerSec;
  }

  std::unordered_set<uint32_t> LevelEngine::getDroppedPrefixes() const {
    stdx::lock_guard<stdx::mutex> lk(_droppedPrefixesMutex);
    // this will copy the set. that way compaction filter has its own copy and doesn't need to
    // worry about thread safety
    return _droppedPrefixes;
  }

  // non public api
  Status LevelEngine::_createIdent(StringData ident, BSONObjBuilder* configBuilder) {
    BSONObj config;
    uint32_t prefix = 0;
    {
      stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
      if (_identMap.find(ident) != _identMap.end()) {
	// already exists
	return Status::OK();
      }

      prefix = ++_maxPrefix;
      configBuilder->append("prefix", static_cast<int32_t>(prefix));

      config = configBuilder->obj();
      _identMap[ident] = config.copy();
    }

    BSONObjBuilder builder;

    auto s = _db->Put(leveldb::WriteOptions(), kMetadataPrefix + ident.toString(),
		      leveldb::Slice(config.objdata(), config.objsize()));

    if (s.ok()) {
      // As an optimization, add a key <prefix> to the DB
      std::string encodedPrefix(encodePrefix(prefix));
      s = _db->Put(leveldb::WriteOptions(), encodedPrefix, leveldb::Slice());
    }

    return levelToMongoStatus(s);
  }

  BSONObj LevelEngine::_getIdentConfig(StringData ident) {
    stdx::lock_guard<stdx::mutex> lk(_identMapMutex);
    auto identIter = _identMap.find(ident);
    invariant(identIter != _identMap.end());
    return identIter->second.copy();
  }

  std::string LevelEngine::_extractPrefix(const BSONObj& config) {
    return encodePrefix(config.getField("prefix").numberInt());
  }

  leveldb::Options LevelEngine::_options() const {
    // default options
    leveldb::Options options;
    options.block_cache = _block_cache;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_size = 4 * 1024; // 16KB
    options.write_buffer_size = 16 * 1024 * 1024;  // 64MB
    options.max_open_files = 3000;
    
    
    if (levelGlobalOptions.compression == "snappy") {
      //options.compression_per_level[2] = leveldb::kSnappyCompression;
    } else if (levelGlobalOptions.compression == "none") {
      //options.compression_per_level[2] = leveldb::kNoCompression;
    }

    // create the DB if it's not already present
    options.create_if_missing = true;

    // allow override
    if (!levelGlobalOptions.configString.empty()) {
      leveldb::Options base_options(options);
    }
    
    return options;
  }
}
