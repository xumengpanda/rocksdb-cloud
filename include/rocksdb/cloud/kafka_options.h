//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// Defines parameters required to connect to Kafka
struct KafkaLogOptions {
  // The config parameters for the kafka client. At a bare minimum,
  // there needs to be at least one entry in this map that lists the
  // kafka brokers. That entry is of the type
  //  ("metadata.broker.list", "kafka1.rockset.com,kafka2.rockset.com"
  //
  std::unordered_map<std::string, std::string> client_config_params;
};
  
} // namespace ROCKSDB_NAMESPACE
