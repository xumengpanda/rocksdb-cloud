//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include "cache/lru_cache.h"
#include "rocksdb/utilities/object_registry.h"
#include "util/string_util.h"

namespace rocksdb {
const std::string Cache::kLRUCacheName = "LRUCache";
const std::string Cache::kClockCacheName = "ClockCache";

Status Cache::CreateFromString(const std::string& value,
                               const ConfigOptions& cfg_opts,
                               std::shared_ptr<Cache>* result) {
  Status s;
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  if (value.empty() || value == kNullptrString) {
    result->reset();
    return Status::OK();
  } else if (value.find('=') != std::string::npos) {
    // block_cache is specified in format block_cache=<cache_size>.
    // cache options can be specified in the following format
    //   "block_cache={capacity=1M;num_shard_bits=4;
    //    strict_capacity_limit=true;high_pri_pool_ratio=0.5;}"
    s = Customizable::GetOptionsMap(value, &id, &opt_map);
    if (id.empty() && !opt_map.empty()) {
      // This is an old-style option without an ID.
      // Use LRU and ignore the status
      id = Cache::kLRUCacheName;
      s = Status::OK();
    } else {
      return s;
    }
  } else {
    // To support backward compatibility, the following format
    // is also supported.
    //   "block_cache=1M"
    try {
      std::shared_ptr<Cache> cache;
      cache = NewLRUCache(ParseSizeT(value));
      s = cache->SetupCache();
      if (s.ok()) {
        result->swap(cache);
      }
      return s;
    } catch (std::exception& e) {
      // It was not a backward name, just a simple name...
      id = value;
    }
  }
  if (id == Cache::kLRUCacheName) {
    result->reset(new LRUCache());
  } else {
#ifndef ROCKSDB_LITE
    s = cfg_opts.registry->NewSharedObject<Cache>(id, result);
#else
    return Status::NotSupported("Cannot load cache in LITE mode ", value);
#endif  //! ROCKSDB_LITE
  }
  if (s.ok() && !opt_map.empty()) {
    s = (*result)->ConfigureFromMap(opt_map, cfg_opts);
  }
  if (s.ok()) {
    s = (*result)->SetupCache();
  }
  return s;
}
}  // namespace rocksdb
