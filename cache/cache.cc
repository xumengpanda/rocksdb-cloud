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
#include "util/string_util.h"

namespace rocksdb {
const std::string Cache::kLRUCacheName = "LRUCache";
const std::string Cache::kClockCacheName = "ClockCache";

Status Cache::CreateFromString(const std::string& value,
                               const ConfigOptions& cfg_opts,
                               std::shared_ptr<Cache>* result) {
  Status s;
  std::shared_ptr<Cache> cache;
  // block_cache is specified in format block_cache=<cache_size>.
  // cache options can be specified in the following format
  //   "block_cache={capacity=1M;num_shard_bits=4;
  //    strict_capacity_limit=true;high_pri_pool_ratio=0.5;}"
  // To support backward compatibility, the following format
  // is also supported.
  //   "block_cache=1M"
  if (value.find('=') == std::string::npos) {
    cache = NewLRUCache(ParseSizeT(value));
  } else {
    cache.reset(new LRUCache());
    s = cache->ConfigureFromString(value, cfg_opts);
    if (s.ok()) {
      s = cache->SetupCache();
    }
  }
  if (s.ok()) {
    result->swap(cache);
  }
  return s;
}
}  // namespace rocksdb
