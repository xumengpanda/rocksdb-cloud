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

void CloudEnvOptions::Dump(Logger* log) const {
  if (storage_provider) {
    storage_provider->Dump(log);
  }
  if (cloud_log_controller) {
    cloud_log_controller->Dump(log);
  }
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         number_objects_listed_in_one_iteration);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
