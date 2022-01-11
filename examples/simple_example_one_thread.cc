// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <chrono>
#include <cstdio>
#include <ostream>
#include <string>
#include <iostream>
#include <random>
#include <unistd.h>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/c.h"
#include "rocksdb/perf_context.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/tmp/rocksdb_simple_example";
#endif

#define _GLIBCXX_USE_NANOSLEEP 1
std::string random_string();
Options getOptions();
void rocksDBMetricLogger(std::shared_ptr<rocksdb::Statistics> statistics, rocksdb_perfcontext_t* perf, rocksdb::DB* db);

int main() {
  rocksdb_set_perf_level(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
  DB* db;
  Options options = getOptions();
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  rocksdb_perfcontext_t *perf = rocksdb_perfcontext_create();

  //std::thread metrics(rocksDBMetricLogger, options.statistics, perf, db);
  rocksDBMetricLogger(options.statistics, perf, db);

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  // atomically apply lots of updates
  long size = 0;
  while (size < 1 * 1024)
  {
    WriteBatch batch;
    std::string key1=random_string();
    std::string key2=random_string();
    std::string val2=random_string();
    size = size + key1.size() + key2.size() + val2.size();
    batch.Delete(key1);
    batch.Put(key2, val2);
    s = db->Write(WriteOptions(), &batch);
    db->Get(ReadOptions(), key1, &value);
    db->Get(ReadOptions(), key2, &value);
  }

  rocksDBMetricLogger(options.statistics, perf, db);

  {
    PinnableSlice pinnable_val;
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
  }

  {
    std::string string_val;
    // If it cannot pin the value, it copies the value to its internal buffer.
    // The intenral buffer could be set during construction.
    PinnableSlice pinnable_val(&string_val);
    db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    // If the value is not pinned, the internal buffer must have the value.
    assert(pinnable_val.IsPinned() || string_val == "value");
  }

  PinnableSlice pinnable_val;
  s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
  assert(s.IsNotFound());
  // Reset PinnableSlice after each use and before each reuse
  pinnable_val.Reset();
  db->Get(ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
  assert(pinnable_val == "value");
  pinnable_val.Reset();
  // The Slice pointed by pinnable_val is not valid after this point

  std::cout << "Main finishes. Block on metrics thread" << std::endl;
  //metrics.join();

  delete db;

  return 0;
}

Options getOptions() {
	Options options({}, ColumnFamilyOptions());
	options.avoid_unnecessary_blocking_io = true;
	options.create_if_missing = true;
	// if (SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM > 0) {
	// 	options.IncreaseParallelism(SERVER_KNOBS->ROCKSDB_BACKGROUND_PARALLELISM);
	// }

	options.statistics = rocksdb::CreateDBStatistics();
	options.statistics->set_stats_level(rocksdb::kExceptHistogramOrTimers);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
  rocksdb::get_perf_context()->Reset();

	options.db_log_dir = "/tmp/";
	return options;
}

void rocksDBMetricLogger(std::shared_ptr<rocksdb::Statistics> statistics, rocksdb_perfcontext_t* perf, rocksdb::DB* db) {
	std::vector<std::tuple<const char*, uint32_t, uint64_t>> tickerStats = {
		{ "StallMicros", rocksdb::STALL_MICROS, 0 },
		{ "BytesRead", rocksdb::BYTES_READ, 0 },
		{ "IterBytesRead", rocksdb::ITER_BYTES_READ, 0 },
		{ "BytesWritten", rocksdb::BYTES_WRITTEN, 0 },
		{ "BlockCacheMisses", rocksdb::BLOCK_CACHE_MISS, 0 },
		{ "BlockCacheHits", rocksdb::BLOCK_CACHE_HIT, 0 },
		{ "BloomFilterUseful", rocksdb::BLOOM_FILTER_USEFUL, 0 },
		{ "BloomFilterFullPositive", rocksdb::BLOOM_FILTER_FULL_POSITIVE, 0 },
		{ "BloomFilterTruePositive", rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE, 0 },
		{ "BloomFilterMicros", rocksdb::BLOOM_FILTER_MICROS, 0 },
		{ "MemtableHit", rocksdb::MEMTABLE_HIT, 0 },
		{ "MemtableMiss", rocksdb::MEMTABLE_MISS, 0 },
		{ "GetHitL0", rocksdb::GET_HIT_L0, 0 },
		{ "GetHitL1", rocksdb::GET_HIT_L1, 0 },
		{ "GetHitL2AndUp", rocksdb::GET_HIT_L2_AND_UP, 0 },
		{ "CountKeysWritten", rocksdb::NUMBER_KEYS_WRITTEN, 0 },
		{ "CountKeysRead", rocksdb::NUMBER_KEYS_READ, 0 },
		{ "CountDBSeek", rocksdb::NUMBER_DB_SEEK, 0 },
		{ "CountDBNext", rocksdb::NUMBER_DB_NEXT, 0 },
		{ "CountDBPrev", rocksdb::NUMBER_DB_PREV, 0 },
		{ "BloomFilterPrefixChecked", rocksdb::BLOOM_FILTER_PREFIX_CHECKED, 0 },
		{ "BloomFilterPrefixUseful", rocksdb::BLOOM_FILTER_PREFIX_USEFUL, 0 },
		{ "BlockCacheCompressedMiss", rocksdb::BLOCK_CACHE_COMPRESSED_MISS, 0 },
		{ "BlockCacheCompressedHit", rocksdb::BLOCK_CACHE_COMPRESSED_HIT, 0 },
		{ "CountWalFileSyncs", rocksdb::WAL_FILE_SYNCED, 0 },
		{ "CountWalFileBytes", rocksdb::WAL_FILE_BYTES, 0 },
		{ "CompactReadBytes", rocksdb::COMPACT_READ_BYTES, 0 },
		{ "CompactWriteBytes", rocksdb::COMPACT_WRITE_BYTES, 0 },
		{ "FlushWriteBytes", rocksdb::FLUSH_WRITE_BYTES, 0 },
		{ "CountBlocksCompressed", rocksdb::NUMBER_BLOCK_COMPRESSED, 0 },
		{ "CountBlocksDecompressed", rocksdb::NUMBER_BLOCK_DECOMPRESSED, 0 },
		{ "RowCacheHit", rocksdb::ROW_CACHE_HIT, 0 },
		{ "RowCacheMiss", rocksdb::ROW_CACHE_MISS, 0 },
		{ "CountIterSkippedKeys", rocksdb::NUMBER_ITER_SKIP, 0 },
		// TODO: Add the following counters
		// NO_ITERATORS

	};
	std::vector<std::pair<const char*, std::string>> propertyStats = {
		{ "NumCompactionsRunning", rocksdb::DB::Properties::kNumRunningCompactions },
		{ "NumImmutableMemtables", rocksdb::DB::Properties::kNumImmutableMemTable },
		{ "NumImmutableMemtablesFlushed", rocksdb::DB::Properties::kNumImmutableMemTableFlushed },
		{ "IsMemtableFlushPending", rocksdb::DB::Properties::kMemTableFlushPending },
		{ "NumRunningFlushes", rocksdb::DB::Properties::kNumRunningFlushes },
		{ "IsCompactionPending", rocksdb::DB::Properties::kCompactionPending },
		{ "NumRunningCompactions", rocksdb::DB::Properties::kNumRunningCompactions },
		{ "CumulativeBackgroundErrors", rocksdb::DB::Properties::kBackgroundErrors },
		{ "CurrentSizeActiveMemtable", rocksdb::DB::Properties::kCurSizeActiveMemTable },
		{ "AllMemtablesBytes", rocksdb::DB::Properties::kCurSizeAllMemTables },
		{ "ActiveMemtableBytes", rocksdb::DB::Properties::kSizeAllMemTables },
		{ "CountEntriesActiveMemtable", rocksdb::DB::Properties::kNumEntriesActiveMemTable },
		{ "CountEntriesImmutMemtables", rocksdb::DB::Properties::kNumEntriesImmMemTables },
		{ "CountDeletesActiveMemtable", rocksdb::DB::Properties::kNumDeletesActiveMemTable },
		{ "CountDeletesImmutMemtables", rocksdb::DB::Properties::kNumDeletesImmMemTables },
		{ "EstimatedCountKeys", rocksdb::DB::Properties::kEstimateNumKeys },
		{ "EstimateSstReaderBytes", rocksdb::DB::Properties::kEstimateTableReadersMem },
		{ "CountActiveSnapshots", rocksdb::DB::Properties::kNumSnapshots },
		{ "OldestSnapshotTime", rocksdb::DB::Properties::kOldestSnapshotTime },
		{ "CountLiveVersions", rocksdb::DB::Properties::kNumLiveVersions },
		{ "EstimateLiveDataSize", rocksdb::DB::Properties::kEstimateLiveDataSize },
		{ "BaseLevel", rocksdb::DB::Properties::kBaseLevel },
		{ "EstPendCompactBytes", rocksdb::DB::Properties::kEstimatePendingCompactionBytes },
		{ "IsWriteStopped", rocksdb::DB::Properties::kIsWriteStopped },
		{ "BlockCacheUsage", rocksdb::DB::Properties::kBlockCacheUsage },
		{ "BlockCachePinnedUsage", rocksdb::DB::Properties::kBlockCachePinnedUsage },
		{ "CompressionRatioAtLevel1",
		  rocksdb::DB::Properties::kCompressionRatioAtLevelPrefix }, // compression ratio = uncompressed data size /
		                                                             // compressed file size
		{ "LevelStats", rocksdb::DB::Properties::kLevelStats },
	};

  std::vector<std::pair<const char*, int>> perfContextStats = {
		{ "BlockReadBytes", rocksdb_block_read_byte },
		{ "BlockReadCount", rocksdb_block_read_count},
		{ "BlockReadTime", rocksdb_block_read_time},
		{ "WriteWALns", rocksdb_write_wal_time},
    { "WriteMemtablens", rocksdb_write_memtable_time},
	};

  while (1){
    std::cout << "------------------------------------" << std::endl;
    //rocksdb_perfcontext_t* perf = rocksdb_perfcontext_create();
    {
      std::cout << "RocksDBMetrics:" << std::endl;
      for (auto& t : tickerStats) {
        auto& [name, ticker, cum] = t;
        uint64_t val = statistics->getTickerCount(ticker);
        std::cout << name << ":" << val - cum <<  ", ";
        cum = val;
      }

      for (auto& p : propertyStats) {
        auto& [name, property] = p;
        uint64_t stat = 0;
        // ASSERT(db->GetIntProperty(property, &stat));
        std::cout << name << ":" << stat <<  ", ";
      }
      std::cout << std::endl;

      for (auto& pc : perfContextStats) {
        auto& [name, property] = pc;
        const uint64_t stat = (uint64_t)rocksdb_perfcontext_metric(perf, property);
        std::cout << name << ":" << stat <<  ", ";
      }
      rocksdb_perfcontext_reset(perf);

      std::cout << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }
}

// copy from https://stackoverflow.com/questions/47977829/generate-a-random-string-in-c11
std::string random_string()
{
     std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

     std::random_device rd;
     std::mt19937 generator(rd());

     std::shuffle(str.begin(), str.end(), generator);

     return str.substr(0, 32);    // assumes 32 < number of characters in str
}