Import("env")

env = env.Clone()

dynamic_syslibdeps = []
conf = Configure(env)

if conf.CheckLibWithHeader("lz4", ["lz4.h","lz4hc.h"], "C", "LZ4_versionNumber();", autoadd=False ):
    dynamic_syslibdeps.append("lz4")	
    
dynamic_syslibdeps.append("/usr/local/lib/libpebblesdb.so")

conf.Finish()

env.InjectMongoIncludePaths()

env.Library(
    target= 'storage_pebbles_base',
    source= [
        'src/pebbles_compaction_scheduler.cpp',
        'src/pebbles_counter_manager.cpp',
        'src/pebbles_global_options.cpp',
        'src/pebbles_engine.cpp',
        'src/pebbles_record_store.cpp',
        'src/pebbles_recovery_unit.cpp',
        'src/pebbles_index.cpp',
        'src/pebbles_durability_manager.cpp',
        'src/pebbles_transaction.cpp',
        'src/pebbles_snapshot_manager.cpp',
        'src/pebbles_util.cpp',
        ],
    LIBDEPS= [
        '$BUILD_DIR/mongo/base',
        '$BUILD_DIR/mongo/db/namespace_string',
        '$BUILD_DIR/mongo/db/catalog/collection_options',
        '$BUILD_DIR/mongo/db/concurrency/lock_manager',
        '$BUILD_DIR/mongo/db/concurrency/write_conflict_exception',
        '$BUILD_DIR/mongo/db/index/index_descriptor',
        '$BUILD_DIR/mongo/db/storage/bson_collection_catalog_entry',
        '$BUILD_DIR/mongo/db/storage/index_entry_comparison',
        '$BUILD_DIR/mongo/db/storage/journal_listener',
        '$BUILD_DIR/mongo/db/storage/key_string',
        '$BUILD_DIR/mongo/db/storage/oplog_hack',
        '$BUILD_DIR/mongo/util/background_job',
        '$BUILD_DIR/mongo/util/processinfo',
        '$BUILD_DIR/third_party/shim_snappy',
        ],
    SYSLIBDEPS=[]
               + dynamic_syslibdeps
    )

env.Library(
    target= 'storage_pebbles',
    source= [
        'src/pebbles_init.cpp',
        'src/pebbles_options_init.cpp',
        'src/pebbles_parameters.cpp',
        'src/pebbles_record_store_mongod.cpp',
        'src/pebbles_server_status.cpp',
        ],
    LIBDEPS= [
        'storage_pebbles_base',
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine'
        ],
    LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/db/serveronly']
    )

env.Library(
    target= 'storage_pebbles_mock',
    source= [
        'src/pebbles_record_store_mock.cpp',
        ],
    LIBDEPS= [
        'storage_pebbles_base',
        # Temporary crutch since the ssl cleanup is hard coded in background.cpp
        '$BUILD_DIR/mongo/util/net/network',
        ]
    )


env.CppUnitTest(
   target='storage_pebbles_index_test',
   source=['src/pebbles_index_test.cpp'
           ],
   LIBDEPS=[
        'storage_pebbles_mock',
        '$BUILD_DIR/mongo/db/storage/sorted_data_interface_test_harness'
        ]
   )


env.CppUnitTest(
   target='storage_pebbles_record_store_test',
   source=['src/pebbles_record_store_test.cpp'
           ],
   LIBDEPS=[
        'storage_pebbles_mock',
        '$BUILD_DIR/mongo/db/storage/record_store_test_harness'
        ]
   )

env.CppUnitTest(
   target='storage_pebbles_engine_test',
   source=['src/pebbles_engine_test.cpp'
           ],
   LIBDEPS=[
        'storage_pebbles_mock',
        '$BUILD_DIR/mongo/db/storage/kv/kv_engine_test_harness',
        '$BUILD_DIR/mongo/db/storage/storage_options'
        ]
   )

