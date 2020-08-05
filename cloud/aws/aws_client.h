//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once

#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace Aws {
namespace Auth {
class AWSCredentialsProvider;
}  // namespace Auth
namespace Client {
struct ClientConfiguration;
}  // namespace Client
}  // namespace Aws


namespace ROCKSDB_NAMESPACE {
class Logger;
 
class AwsClientOptions {
 public:
  static Status GetClientConfiguration(
      const std::shared_ptr<Logger>& logger,
      uint64_t connect_timeout_ms,
      uint64_t request_timeout_ms,
      const std::string& region,
      Aws::Client::ClientConfiguration* config);
};
}  // namespace ROCKSDB_NAMESPACE

