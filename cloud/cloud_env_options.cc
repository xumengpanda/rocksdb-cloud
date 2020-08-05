// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <cinttypes>

#include "cloud/cloud_constants.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
const std::string CloudImplConstants::kImplName = "impl";
const std::string CloudImplConstants::kTestId = "?test";

bool CloudImplConstants::IsTestId(const std::string& name, std::string* id) {
  auto idx = name.find(kTestId);
  if (idx > 0 && idx != std::string::npos) {
    *id = name.substr(0, idx);
    return true;
  } else {
    *id = name;
    return false;
  }
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
