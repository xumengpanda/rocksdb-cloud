//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "options/options_helper.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/customizable.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace rocksdb {

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options) {
  DBOptions options;

  options.create_if_missing = immutable_db_options.create_if_missing;
  options.create_missing_column_families =
      immutable_db_options.create_missing_column_families;
  options.error_if_exists = immutable_db_options.error_if_exists;
  options.paranoid_checks = immutable_db_options.paranoid_checks;
  options.env = immutable_db_options.env;
  options.file_system = immutable_db_options.fs;
  options.rate_limiter = immutable_db_options.rate_limiter;
  options.sst_file_manager = immutable_db_options.sst_file_manager;
  options.info_log = immutable_db_options.info_log;
  options.info_log_level = immutable_db_options.info_log_level;
  options.max_open_files = mutable_db_options.max_open_files;
  options.max_file_opening_threads =
      immutable_db_options.max_file_opening_threads;
  options.max_total_wal_size = mutable_db_options.max_total_wal_size;
  options.statistics = immutable_db_options.statistics;
  options.use_fsync = immutable_db_options.use_fsync;
  options.db_paths = immutable_db_options.db_paths;
  options.db_log_dir = immutable_db_options.db_log_dir;
  options.wal_dir = immutable_db_options.wal_dir;
  options.delete_obsolete_files_period_micros =
      mutable_db_options.delete_obsolete_files_period_micros;
  options.max_background_jobs = mutable_db_options.max_background_jobs;
  options.base_background_compactions =
      mutable_db_options.base_background_compactions;
  options.max_background_compactions =
      mutable_db_options.max_background_compactions;
  options.bytes_per_sync = mutable_db_options.bytes_per_sync;
  options.wal_bytes_per_sync = mutable_db_options.wal_bytes_per_sync;
  options.strict_bytes_per_sync = mutable_db_options.strict_bytes_per_sync;
  options.max_subcompactions = immutable_db_options.max_subcompactions;
  options.max_background_flushes = immutable_db_options.max_background_flushes;
  options.max_log_file_size = immutable_db_options.max_log_file_size;
  options.log_file_time_to_roll = immutable_db_options.log_file_time_to_roll;
  options.keep_log_file_num = immutable_db_options.keep_log_file_num;
  options.recycle_log_file_num = immutable_db_options.recycle_log_file_num;
  options.max_manifest_file_size = immutable_db_options.max_manifest_file_size;
  options.table_cache_numshardbits =
      immutable_db_options.table_cache_numshardbits;
  options.WAL_ttl_seconds = immutable_db_options.wal_ttl_seconds;
  options.WAL_size_limit_MB = immutable_db_options.wal_size_limit_mb;
  options.manifest_preallocation_size =
      immutable_db_options.manifest_preallocation_size;
  options.allow_mmap_reads = immutable_db_options.allow_mmap_reads;
  options.allow_mmap_writes = immutable_db_options.allow_mmap_writes;
  options.use_direct_reads = immutable_db_options.use_direct_reads;
  options.use_direct_io_for_flush_and_compaction =
      immutable_db_options.use_direct_io_for_flush_and_compaction;
  options.allow_fallocate = immutable_db_options.allow_fallocate;
  options.is_fd_close_on_exec = immutable_db_options.is_fd_close_on_exec;
  options.stats_dump_period_sec = mutable_db_options.stats_dump_period_sec;
  options.stats_persist_period_sec =
      mutable_db_options.stats_persist_period_sec;
  options.persist_stats_to_disk = immutable_db_options.persist_stats_to_disk;
  options.stats_history_buffer_size =
      mutable_db_options.stats_history_buffer_size;
  options.advise_random_on_open = immutable_db_options.advise_random_on_open;
  options.db_write_buffer_size = immutable_db_options.db_write_buffer_size;
  options.write_buffer_manager = immutable_db_options.write_buffer_manager;
  options.access_hint_on_compaction_start =
      immutable_db_options.access_hint_on_compaction_start;
  options.new_table_reader_for_compaction_inputs =
      immutable_db_options.new_table_reader_for_compaction_inputs;
  options.compaction_readahead_size =
      mutable_db_options.compaction_readahead_size;
  options.random_access_max_buffer_size =
      immutable_db_options.random_access_max_buffer_size;
  options.writable_file_max_buffer_size =
      mutable_db_options.writable_file_max_buffer_size;
  options.use_adaptive_mutex = immutable_db_options.use_adaptive_mutex;
  options.listeners = immutable_db_options.listeners;
  options.enable_thread_tracking = immutable_db_options.enable_thread_tracking;
  options.delayed_write_rate = mutable_db_options.delayed_write_rate;
  options.enable_pipelined_write = immutable_db_options.enable_pipelined_write;
  options.unordered_write = immutable_db_options.unordered_write;
  options.allow_concurrent_memtable_write =
      immutable_db_options.allow_concurrent_memtable_write;
  options.enable_write_thread_adaptive_yield =
      immutable_db_options.enable_write_thread_adaptive_yield;
  options.max_write_batch_group_size_bytes =
      immutable_db_options.max_write_batch_group_size_bytes;
  options.write_thread_max_yield_usec =
      immutable_db_options.write_thread_max_yield_usec;
  options.write_thread_slow_yield_usec =
      immutable_db_options.write_thread_slow_yield_usec;
  options.skip_stats_update_on_db_open =
      immutable_db_options.skip_stats_update_on_db_open;
  options.wal_recovery_mode = immutable_db_options.wal_recovery_mode;
  options.allow_2pc = immutable_db_options.allow_2pc;
  options.row_cache = immutable_db_options.row_cache;
#ifndef ROCKSDB_LITE
  options.wal_filter = immutable_db_options.wal_filter;
#endif  // ROCKSDB_LITE
  options.fail_if_options_file_error =
      immutable_db_options.fail_if_options_file_error;
  options.dump_malloc_stats = immutable_db_options.dump_malloc_stats;
  options.avoid_flush_during_recovery =
      immutable_db_options.avoid_flush_during_recovery;
  options.avoid_flush_during_shutdown =
      mutable_db_options.avoid_flush_during_shutdown;
  options.allow_ingest_behind =
      immutable_db_options.allow_ingest_behind;
  options.preserve_deletes =
      immutable_db_options.preserve_deletes;
  options.two_write_queues = immutable_db_options.two_write_queues;
  options.manual_wal_flush = immutable_db_options.manual_wal_flush;
  options.atomic_flush = immutable_db_options.atomic_flush;
  options.avoid_unnecessary_blocking_io =
      immutable_db_options.avoid_unnecessary_blocking_io;
  options.log_readahead_size = immutable_db_options.log_readahead_size;
  return options;
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);

  // Memtable related options
  cf_opts.write_buffer_size = mutable_cf_options.write_buffer_size;
  cf_opts.max_write_buffer_number = mutable_cf_options.max_write_buffer_number;
  cf_opts.arena_block_size = mutable_cf_options.arena_block_size;
  cf_opts.memtable_prefix_bloom_size_ratio =
      mutable_cf_options.memtable_prefix_bloom_size_ratio;
  cf_opts.memtable_whole_key_filtering =
      mutable_cf_options.memtable_whole_key_filtering;
  cf_opts.memtable_huge_page_size = mutable_cf_options.memtable_huge_page_size;
  cf_opts.max_successive_merges = mutable_cf_options.max_successive_merges;
  cf_opts.inplace_update_num_locks =
      mutable_cf_options.inplace_update_num_locks;
  cf_opts.prefix_extractor = mutable_cf_options.prefix_extractor;

  // Compaction related options
  cf_opts.disable_auto_compactions =
      mutable_cf_options.disable_auto_compactions;
  cf_opts.soft_pending_compaction_bytes_limit =
      mutable_cf_options.soft_pending_compaction_bytes_limit;
  cf_opts.hard_pending_compaction_bytes_limit =
      mutable_cf_options.hard_pending_compaction_bytes_limit;
  cf_opts.level0_file_num_compaction_trigger =
      mutable_cf_options.level0_file_num_compaction_trigger;
  cf_opts.level0_slowdown_writes_trigger =
      mutable_cf_options.level0_slowdown_writes_trigger;
  cf_opts.level0_stop_writes_trigger =
      mutable_cf_options.level0_stop_writes_trigger;
  cf_opts.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  cf_opts.target_file_size_base = mutable_cf_options.target_file_size_base;
  cf_opts.target_file_size_multiplier =
      mutable_cf_options.target_file_size_multiplier;
  cf_opts.max_bytes_for_level_base =
      mutable_cf_options.max_bytes_for_level_base;
  cf_opts.max_bytes_for_level_multiplier =
      mutable_cf_options.max_bytes_for_level_multiplier;
  cf_opts.ttl = mutable_cf_options.ttl;
  cf_opts.periodic_compaction_seconds =
      mutable_cf_options.periodic_compaction_seconds;

  cf_opts.max_bytes_for_level_multiplier_additional.clear();
  for (auto value :
       mutable_cf_options.max_bytes_for_level_multiplier_additional) {
    cf_opts.max_bytes_for_level_multiplier_additional.emplace_back(value);
  }

  cf_opts.compaction_options_fifo = mutable_cf_options.compaction_options_fifo;
  cf_opts.compaction_options_universal =
      mutable_cf_options.compaction_options_universal;

  // Misc options
  cf_opts.max_sequential_skip_in_iterations =
      mutable_cf_options.max_sequential_skip_in_iterations;
  cf_opts.paranoid_file_checks = mutable_cf_options.paranoid_file_checks;
  cf_opts.report_bg_io_stats = mutable_cf_options.report_bg_io_stats;
  cf_opts.compression = mutable_cf_options.compression;
  cf_opts.sample_for_compression = mutable_cf_options.sample_for_compression;

  cf_opts.table_factory = options.table_factory;
  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size

  return cf_opts;
}

std::map<CompactionStyle, std::string>
    OptionsHelper::compaction_style_to_string = {
        {kCompactionStyleLevel, "kCompactionStyleLevel"},
        {kCompactionStyleUniversal, "kCompactionStyleUniversal"},
        {kCompactionStyleFIFO, "kCompactionStyleFIFO"},
        {kCompactionStyleNone, "kCompactionStyleNone"}};

std::map<CompactionPri, std::string> OptionsHelper::compaction_pri_to_string = {
    {kByCompensatedSize, "kByCompensatedSize"},
    {kOldestLargestSeqFirst, "kOldestLargestSeqFirst"},
    {kOldestSmallestSeqFirst, "kOldestSmallestSeqFirst"},
    {kMinOverlappingRatio, "kMinOverlappingRatio"}};

std::map<CompactionStopStyle, std::string>
    OptionsHelper::compaction_stop_style_to_string = {
        {kCompactionStopStyleSimilarSize, "kCompactionStopStyleSimilarSize"},
        {kCompactionStopStyleTotalSize, "kCompactionStopStyleTotalSize"}};

std::unordered_map<std::string, ChecksumType>
    OptionsHelper::checksum_type_string_map = {{"kNoChecksum", kNoChecksum},
                                               {"kCRC32c", kCRC32c},
                                               {"kxxHash", kxxHash},
                                               {"kxxHash64", kxxHash64}};

std::unordered_map<std::string, CompressionType>
    OptionsHelper::compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kXpressCompression", kXpressCompression},
        {"kZSTD", kZSTD},
        {"kZSTDNotFinalCompression", kZSTDNotFinalCompression},
        {"kDisableCompressionOption", kDisableCompressionOption}};
#ifndef ROCKSDB_LITE

const std::string kNameComparator = "comparator";
const std::string kNameEnv = "env";
const std::string kNameMergeOperator = "merge_operator";


namespace {

bool SerializeVectorCompressionType(const std::vector<CompressionType>& types,
                                    std::string* value) {
  std::stringstream ss;
  bool result;
  for (size_t i = 0; i < types.size(); ++i) {
    if (i > 0) {
      ss << ':';
    }
    std::string string_type;
    result = SerializeEnum<CompressionType>(compression_type_string_map,
                                            types[i], &string_type);
    if (result == false) {
      return result;
    }
    ss << string_type;
  }
  *value = ss.str();
  return true;
}

bool ParseVectorCompressionType(
    const std::string& value,
    std::vector<CompressionType>* compression_per_level) {
  compression_per_level->clear();
  size_t start = 0;
  while (start < value.size()) {
    size_t end = value.find(':', start);
    bool is_ok;
    CompressionType type;
    if (end == std::string::npos) {
      is_ok = ParseEnum<CompressionType>(compression_type_string_map,
                                         value.substr(start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      break;
    } else {
      is_ok = ParseEnum<CompressionType>(
          compression_type_string_map, value.substr(start, end - start), &type);
      if (!is_ok) {
        return false;
      }
      compression_per_level->emplace_back(type);
      start = end + 1;
    }
  }
  return true;
}
}  // anonymouse namespace

bool ParseSliceTransformHelper(
    const std::string& kFixedPrefixName, const std::string& kCappedPrefixName,
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  const char* no_op_name = "rocksdb.Noop";
  size_t no_op_length = strlen(no_op_name);
  auto& pe_value = value;
  if (pe_value.size() > kFixedPrefixName.size() &&
      pe_value.compare(0, kFixedPrefixName.size(), kFixedPrefixName) == 0) {
    int prefix_length = ParseInt(trim(value.substr(kFixedPrefixName.size())));
    slice_transform->reset(NewFixedPrefixTransform(prefix_length));
  } else if (pe_value.size() > kCappedPrefixName.size() &&
             pe_value.compare(0, kCappedPrefixName.size(), kCappedPrefixName) ==
                 0) {
    int prefix_length =
        ParseInt(trim(pe_value.substr(kCappedPrefixName.size())));
    slice_transform->reset(NewCappedPrefixTransform(prefix_length));
  } else if (pe_value.size() == no_op_length &&
             pe_value.compare(0, no_op_length, no_op_name) == 0) {
    const SliceTransform* no_op_transform = NewNoopTransform();
    slice_transform->reset(no_op_transform);
  } else if (value == kNullptrString) {
    slice_transform->reset();
  } else {
    return false;
  }

  return true;
}

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  // While we normally don't convert the string representation of a
  // pointer-typed option into its instance, here we do so for backward
  // compatibility as we allow this action in SetOption().

  // TODO(yhchiang): A possible better place for these serialization /
  // deserialization is inside the class definition of pointer-typed
  // option itself, but this requires a bigger change of public API.
  bool result =
      ParseSliceTransformHelper("fixed:", "capped:", value, slice_transform);
  if (result) {
    return result;
  }
  result = ParseSliceTransformHelper(
      "rocksdb.FixedPrefix.", "rocksdb.CappedPrefix.", value, slice_transform);
  if (result) {
    return result;
  }
  // TODO(yhchiang): we can further support other default
  //                 SliceTransforms here.
  return false;
}

static bool ParseOptionHelper(char* opt_address, OptionType opt_type,
                              const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *reinterpret_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *reinterpret_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kInt32T:
      *reinterpret_cast<int32_t*>(opt_address) = ParseInt32(value);
      break;
    case OptionType::kInt64T:
      PutUnaligned(reinterpret_cast<int64_t*>(opt_address), ParseInt64(value));
      break;
    case OptionType::kVectorInt:
      *reinterpret_cast<std::vector<int>*>(opt_address) = ParseVectorInt(value);
      break;
    case OptionType::kUInt:
      *reinterpret_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt32T:
      *reinterpret_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      PutUnaligned(reinterpret_cast<uint64_t*>(opt_address),
                   ParseUint64(value));
      break;
    case OptionType::kSizeT:
      PutUnaligned(reinterpret_cast<size_t*>(opt_address), ParseSizeT(value));
      break;
    case OptionType::kString:
      *reinterpret_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *reinterpret_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kVectorCompressionType:
      return ParseVectorCompressionType(
          value, reinterpret_cast<std::vector<CompressionType>*>(opt_address));
    case OptionType::kSliceTransform:
      return ParseSliceTransform(
          value, reinterpret_cast<std::shared_ptr<const SliceTransform>*>(
                     opt_address));
    case OptionType::kCompactionStyle:
      return ParseEnum<CompactionStyle>(
          compaction_style_string_map, value,
          reinterpret_cast<CompactionStyle*>(opt_address));
    case OptionType::kCompactionPri:
      return ParseEnum<CompactionPri>(
          compaction_pri_string_map, value,
          reinterpret_cast<CompactionPri*>(opt_address));
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          reinterpret_cast<CompressionType*>(opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(
          checksum_type_string_map, value,
          reinterpret_cast<ChecksumType*>(opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(
          encoding_type_string_map, value,
          reinterpret_cast<EncodingType*>(opt_address));
    case OptionType::kCompactionStopStyle:
      return ParseEnum<CompactionStopStyle>(
          compaction_stop_style_string_map, value,
          reinterpret_cast<CompactionStopStyle*>(opt_address));
    default:
      return false;
  }
  return true;
}

static bool SerializeSingleOptionHelper(const char* opt_address,
                                        OptionType opt_type,
                                        std::string* value) {
  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(reinterpret_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = ToString(*(reinterpret_cast<const int*>(opt_address)));
      break;
    case OptionType::kInt32T:
      *value = ToString(*(reinterpret_cast<const int32_t*>(opt_address)));
      break;
    case OptionType::kInt64T:
      {
        int64_t v;
        GetUnaligned(reinterpret_cast<const int64_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kVectorInt:
      return SerializeIntVector(
          *reinterpret_cast<const std::vector<int>*>(opt_address), value);
    case OptionType::kUInt:
      *value = ToString(*(reinterpret_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = ToString(*(reinterpret_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T:
      {
        uint64_t v;
        GetUnaligned(reinterpret_cast<const uint64_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kSizeT:
      {
        size_t v;
        GetUnaligned(reinterpret_cast<const size_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kDouble:
      *value = ToString(*(reinterpret_cast<const double*>(opt_address)));
      break;
    case OptionType::kString:
      *value = EscapeOptionString(
          *(reinterpret_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kVectorCompressionType:
      return SerializeVectorCompressionType(
          *(reinterpret_cast<const std::vector<CompressionType>*>(opt_address)),
          value);
      break;
    case OptionType::kSliceTransform: {
      const auto* slice_transform_ptr =
          reinterpret_cast<const std::shared_ptr<const SliceTransform>*>(
              opt_address);
      *value = slice_transform_ptr->get() ? slice_transform_ptr->get()->Name()
                                          : kNullptrString;
      break;
    }
    case OptionType::kCompactionStyle:
      return SerializeEnum<CompactionStyle>(
          compaction_style_string_map,
          *(reinterpret_cast<const CompactionStyle*>(opt_address)), value);
    case OptionType::kCompactionPri:
      return SerializeEnum<CompactionPri>(
          compaction_pri_string_map,
          *(reinterpret_cast<const CompactionPri*>(opt_address)), value);
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(reinterpret_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *reinterpret_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *reinterpret_cast<const EncodingType*>(opt_address), value);
    case OptionType::kCompactionStopStyle:
      return SerializeEnum<CompactionStopStyle>(
          compaction_stop_style_string_map,
          *reinterpret_cast<const CompactionStopStyle*>(opt_address), value);
    default:
      return false;
  }
  return true;
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  size_t pos = 0;
  std::string opts = trim(opts_str);
  // If the input string starts and ends with "{...}", strip off the brackets
  while (opts.size() > 2 && opts[0] == '{' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }
  while (pos < opts.size()) {
    size_t eq_pos = opts.find('=', pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    }
    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    // skip space after '=' and look for '{' for possible nested options
    pos = eq_pos + 1;
    while (pos < opts.size() && isspace(opts[pos])) {
      ++pos;
    }
    // Empty value at the end
    if (pos >= opts.size()) {
      (*opts_map)[key] = "";
      break;
    }
    if (opts[pos] == '{') {
      int count = 1;
      size_t brace_pos = pos + 1;
      while (brace_pos < opts.size()) {
        if (opts[brace_pos] == '{') {
          ++count;
        } else if (opts[brace_pos] == '}') {
          --count;
          if (count == 0) {
            break;
          }
        }
        ++brace_pos;
      }
      // found the matching closing brace
      if (count == 0) {
        (*opts_map)[key] = trim(opts.substr(pos + 1, brace_pos - pos - 1));
        // skip all whitespace and move to the next ';'
        // brace_pos points to the next position after the matching '}'
        pos = brace_pos + 1;
        while (pos < opts.size() && isspace(opts[pos])) {
          ++pos;
        }
        if (pos < opts.size() && opts[pos] != ';') {
          return Status::InvalidArgument(
              "Unexpected chars after nested options");
        }
        ++pos;
      } else {
        return Status::InvalidArgument(
            "Mismatched curly braces for nested options");
      }
    } else {
      size_t sc_pos = opts.find(';', pos);
      if (sc_pos == std::string::npos) {
        (*opts_map)[key] = trim(opts.substr(pos));
        // It either ends with a trailing semi-colon or the last key-value pair
        break;
      } else {
        (*opts_map)[key] = trim(opts.substr(pos, sc_pos - pos));
      }
      pos = sc_pos + 1;
    }
  }

  return Status::OK();
}

Status ParseCompressionOptions(const std::string& value, const std::string& name,
                              CompressionOptions& compression_opts) {
  size_t start = 0;
  size_t end = value.find(':');
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.window_bits = ParseInt(value.substr(start, end - start));
  start = end + 1;
  end = value.find(':', start);
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.level = ParseInt(value.substr(start, end - start));
  start = end + 1;
  if (start >= value.size()) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  end = value.find(':', start);
  compression_opts.strategy =
      ParseInt(value.substr(start, value.size() - start));
  // max_dict_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // zstd_max_train_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.zstd_max_train_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // enabled is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.enabled =
        ParseBoolean("", value.substr(start, value.size() - start));
  }
  return Status::OK();
}

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type) {
  bool ok = SerializeEnum<CompressionType>(compression_type_string_map,
                                           compression_type, compression_str);
  if (ok) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Invalid compression types");
  }
}

std::vector<CompressionType> GetSupportedCompressions() {
  std::vector<CompressionType> supported_compressions;
  for (const auto& comp_to_name : compression_type_string_map) {
    CompressionType t = comp_to_name.second;
    if (t != kDisableCompressionOption && CompressionTypeSupported(t)) {
      supported_compressions.push_back(t);
    }
  }
  return supported_compressions;
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  DBOptions new_db_options;
  ColumnFamilyOptions new_cf_options;
  std::unordered_map<std::string, std::string> unused_opts;
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  s = GetDBOptionsFromMapInternal(base_options, opts_map, false,
                                  &new_db_options, &unused_opts, true);
  ConfigOptions cfg_options(new_db_options);
  cfg_options.ignore_unknown_options = true;
  if (s.ok()) {
    s = GetColumnFamilyOptionsFromMap(base_options, unused_opts, cfg_options,
                                      &new_cf_options);
    if (s.ok()) {
      *new_options = Options(new_db_options, new_cf_options);
    }
  }
  return s;
}

std::unordered_map<std::string, EncodingType>
    OptionsHelper::encoding_type_string_map = {{"kPlain", kPlain},
                                               {"kPrefix", kPrefix}};

std::unordered_map<std::string, CompactionStyle>
    OptionsHelper::compaction_style_string_map = {
        {"kCompactionStyleLevel", kCompactionStyleLevel},
        {"kCompactionStyleUniversal", kCompactionStyleUniversal},
        {"kCompactionStyleFIFO", kCompactionStyleFIFO},
        {"kCompactionStyleNone", kCompactionStyleNone}};

std::unordered_map<std::string, CompactionPri>
    OptionsHelper::compaction_pri_string_map = {
        {"kByCompensatedSize", kByCompensatedSize},
        {"kOldestLargestSeqFirst", kOldestLargestSeqFirst},
        {"kOldestSmallestSeqFirst", kOldestSmallestSeqFirst},
        {"kMinOverlappingRatio", kMinOverlappingRatio}};


CompactionOptionsFIFO OptionsHelper::dummy_comp_options;
CompactionOptionsUniversal OptionsHelper::dummy_comp_options_universal;

// offset_of is used to get the offset of a class data member
// ex: offset_of(&ColumnFamilyOptions::num_levels)
// This call will return the offset of num_levels in ColumnFamilyOptions class
//
// This is the same as offsetof() but allow us to work with non standard-layout
// classes and structures
// refs:
// http://en.cppreference.com/w/cpp/concept/StandardLayoutType
// https://gist.github.com/graphitemaster/494f21190bb2c63c5516
template <typename T1>
int offset_of(T1 CompactionOptionsFIFO::*member) {
  return int(size_t(&(OptionsHelper::dummy_comp_options.*member)) -
             size_t(&OptionsHelper::dummy_comp_options));
}
template <typename T1>
int offset_of(T1 CompactionOptionsUniversal::*member) {
  return int(size_t(&(OptionsHelper::dummy_comp_options_universal.*member)) -
             size_t(&OptionsHelper::dummy_comp_options_universal));
}

OptionTypeMap OptionsHelper::fifo_compaction_options_type_info = {
    {"max_table_files_size",
     {offset_of(&CompactionOptionsFIFO::max_table_files_size),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(struct CompactionOptionsFIFO, max_table_files_size)}},
    {"ttl",
     {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
      OptionTypeFlags::kNone, 0}},
    {"allow_compaction",
     {offset_of(&CompactionOptionsFIFO::allow_compaction), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
      offsetof(struct CompactionOptionsFIFO, allow_compaction)}}};

OptionTypeMap OptionsHelper::universal_compaction_options_type_info = {
    {"size_ratio",
     {offset_of(&CompactionOptionsUniversal::size_ratio), OptionType::kUInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, size_ratio)}},
    {"min_merge_width",
     {offset_of(&CompactionOptionsUniversal::min_merge_width),
      OptionType::kUInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, min_merge_width)}},
    {"max_merge_width",
     {offset_of(&CompactionOptionsUniversal::max_merge_width),
      OptionType::kUInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, max_merge_width)}},
    {"max_size_amplification_percent",
     {offset_of(&CompactionOptionsUniversal::max_size_amplification_percent),
      OptionType::kUInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal,
               max_size_amplification_percent)}},
    {"compression_size_percent",
     {offset_of(&CompactionOptionsUniversal::compression_size_percent),
      OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, compression_size_percent)}},
    {"stop_style",
     {offset_of(&CompactionOptionsUniversal::stop_style),
      OptionType::kCompactionStopStyle, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, stop_style)}},
    {"allow_trivial_move",
     {offset_of(&CompactionOptionsUniversal::allow_trivial_move),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable,
      offsetof(class CompactionOptionsUniversal, allow_trivial_move)}}};

std::unordered_map<std::string, CompactionStopStyle>
    OptionsHelper::compaction_stop_style_string_map = {
        {"kCompactionStopStyleSimilarSize", kCompactionStopStyleSimilarSize},
        {"kCompactionStopStyleTotalSize", kCompactionStopStyleTotalSize}};

Status OptionTypeInfo::ParseOption(const std::string& opt_name,
                                   const std::string& opt_value,
                                   const ConfigOptions& options,
                                   char* opt_addr) const {
  try {
    if (verification == OptionVerificationType::kDeprecated) {
      return Status::OK();
    } else if (opt_addr == nullptr) {
      return Status::NotFound("Could not find option: ", opt_name);
    } else if (parser_func != nullptr) {
      return parser_func(opt_name, opt_value, options, opt_addr);
    } else if (ParseOptionHelper(opt_addr, type, opt_value)) {
      return Status::OK();
    } else if (verification == OptionVerificationType::kByName ||
               verification == OptionVerificationType::kByNameAllowNull ||
               verification == OptionVerificationType::kByNameAllowFromNull) {
      return Status::NotSupported("Deserializing the option " + opt_name +
                                  " is not supported");
    } else {
      return Status::InvalidArgument("Error parsing:", opt_name);
    }
  } catch (std::exception& e) {
    return Status::InvalidArgument("Error parsing " + opt_name + ":" +
                                   std::string(e.what()));
  }
}

Status OptionTypeInfo::SerializeOption(const std::string& opt_name,
                                       const char* opt_addr,
                                       const ConfigOptions& options,
                                       std::string* opt_value) const {
  // If the option is no longer used in rocksdb and marked as deprecated,
  // we skip it in the serialization.
  Status s;
  if (opt_addr == nullptr ||
      verification == OptionVerificationType::kDeprecated) {
    return Status::OK();
  } else if (string_func != nullptr) {
    return string_func(opt_name, opt_addr, options, opt_value);
  } else if (SerializeSingleOptionHelper(opt_addr, type, opt_value)) {
    s = Status::OK();
  } else {
    s = Status::InvalidArgument("Cannot serialize option: ", opt_name);
  }
  return s;
}

template <typename T>
bool IsOptionEqual(const char* offset1, const char* offset2) {
  return (*reinterpret_cast<const T*>(offset1) ==
          *reinterpret_cast<const T*>(offset2));
}

static bool AreEqualDoubles(const double a, const double b) {
  return (fabs(a - b) < 0.00001);
}

bool OptionTypeInfo::MatchesOption(const std::string& opt_name,
                                   const char* this_offset,
                                   const char* that_offset,
                                   const ConfigOptions& options,
                                   std::string* mismatch) const {
  if (this_offset == nullptr || that_offset == nullptr) {
    return (this_offset == that_offset);
  } else if (equals_func != nullptr) {
    return equals_func(opt_name, this_offset, that_offset, options, mismatch);
  } else {
    switch (type) {
      case OptionType::kBoolean:
        return IsOptionEqual<bool>(this_offset, that_offset);
      case OptionType::kInt:
        return IsOptionEqual<int>(this_offset, that_offset);
      case OptionType::kUInt:
        return IsOptionEqual<unsigned int>(this_offset, that_offset);
      case OptionType::kInt32T:
        return IsOptionEqual<int32_t>(this_offset, that_offset);
      case OptionType::kInt64T: {
        int64_t v1, v2;
        GetUnaligned(reinterpret_cast<const int64_t*>(this_offset), &v1);
        GetUnaligned(reinterpret_cast<const int64_t*>(that_offset), &v2);
        return (v1 == v2);
      }
      case OptionType::kVectorInt:
        return IsOptionEqual<std::vector<int> >(this_offset, that_offset);
      case OptionType::kUInt32T:
        return IsOptionEqual<uint32_t>(this_offset, that_offset);
      case OptionType::kUInt64T: {
        uint64_t v1, v2;
        GetUnaligned(reinterpret_cast<const uint64_t*>(this_offset), &v1);
        GetUnaligned(reinterpret_cast<const uint64_t*>(that_offset), &v2);
        return (v1 == v2);
      }
      case OptionType::kSizeT: {
        size_t v1, v2;
        GetUnaligned(reinterpret_cast<const size_t*>(this_offset), &v1);
        GetUnaligned(reinterpret_cast<const size_t*>(that_offset), &v2);
        return (v1 == v2);
      }
      case OptionType::kString:
        return IsOptionEqual<std::string>(this_offset, that_offset);
      case OptionType::kDouble:
        return AreEqualDoubles(*reinterpret_cast<const double*>(this_offset),
                               *reinterpret_cast<const double*>(that_offset));
      case OptionType::kVectorCompressionType:
        return IsOptionEqual<std::vector<CompressionType> >(this_offset,
                                                            that_offset);
      case OptionType::kCompactionStyle:
        return IsOptionEqual<CompactionStyle>(this_offset, that_offset);
      case OptionType::kCompactionStopStyle:
        return IsOptionEqual<CompactionStopStyle>(this_offset, that_offset);
      case OptionType::kCompactionPri:
        return IsOptionEqual<CompactionPri>(this_offset, that_offset);
      case OptionType::kCompressionType:
        return IsOptionEqual<CompressionType>(this_offset, that_offset);
      case OptionType::kChecksumType:
        return IsOptionEqual<ChecksumType>(this_offset, that_offset);
      case OptionType::kEncodingType:
        return IsOptionEqual<EncodingType>(this_offset, that_offset);
      default:
        return false;
    }  // End switch
  }
  return false;
}
#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
