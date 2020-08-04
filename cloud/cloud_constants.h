//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
#pragma once

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class CloudImplConstants {
 public:
  static const std::string kImplName /* = "impl" */;
  static const std::string kTestId /* = "?test" */;
  static bool IsTestId(const std::string& name, std::string *id);
};
} // namespace ROCKSDB_NAMESPACE
