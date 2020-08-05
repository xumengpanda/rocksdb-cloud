// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#ifndef _WIN32_WINNT
#include <unistd.h>
#else
#include <windows.h>
#endif

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_constants.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

bool CloudEnvOptions::GetNameFromEnvironment(const char* name, const char* alt,
                                             std::string* result) {
  char* value = getenv(name);  // See if name is set in the environment
  if (value == nullptr &&
      alt != nullptr) {   // Not set.  Do we have an alt name?
    value = getenv(alt);  // See if alt is in the environment
  }
  if (value != nullptr) {   // Did we find the either name/alt in the env?
    result->assign(value);  // Yes, update result
    return true;            // And return success
  } else {
    return false;  // No, return not found
  }
}
void CloudEnvOptions::TEST_Initialize(const std::string& bucket,
                                      const std::string& object,
                                      const std::string& region) {
  src_bucket.TEST_Initialize(bucket, object, region);
  dest_bucket = src_bucket;
}

BucketOptions::BucketOptions() { prefix_ = "rockset."; }

void BucketOptions::SetBucketName(const std::string& bucket,
                                  const std::string& prefix) {
  if (!prefix.empty()) {
    prefix_ = prefix;
  }

  bucket_ = bucket;
  if (bucket_.empty()) {
    name_.clear();
  } else {
    name_ = prefix_ + bucket_;
  }
}

// Initializes the bucket properties

void BucketOptions::TEST_Initialize(const std::string& bucket,
                                    const std::string& object,
                                    const std::string& region) {
  std::string prefix;
  // If the bucket name is not set, then the bucket name is not set,
  // Set it to either the value of the environment variable or geteuid
  if (!CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_BUCKET_NAME",
                                               "ROCKSDB_CLOUD_BUCKET_NAME",
                                               &bucket_)) {
#ifdef _WIN32_WINNT
    char user_name[257];  // UNLEN + 1
    DWORD dwsize = sizeof(user_name);
    if (!::GetUserName(user_name, &dwsize)) {
      bucket_ = bucket_ + "unknown";
    } else {
      bucket_ =
          bucket_ +
          std::string(user_name, static_cast<std::string::size_type>(dwsize));
    }
#else
    bucket_ = bucket + std::to_string(geteuid());
#endif
  }
  if (CloudEnvOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_BUCKET_PREFIX", "ROCKSDB_CLOUD_BUCKET_PREFIX",
          &prefix)) {
    prefix_ = prefix;
  }
  name_ = prefix_ + bucket_;
  if (!CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_OBECT_PATH",
                                               "ROCKSDB_CLOUD_OBJECT_PATH",
                                               &object_)) {
    object_ = object;
  }
  if (!CloudEnvOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_REGION", "ROCKSDB_CLOUD_REGION", &region_)) {
    region_ = region;
  }
}

const std::string CloudEnv::kAwsCloudName = "aws";
const std::string CloudEnv::kEnvCloudName = "cloud";

CloudEnv::CloudEnv(Env* base, const CloudEnvOptions& options,
                   std::unique_ptr<CloudStorageProvider> provider,
                   std::unique_ptr<CloudLogController> controller)
  : base_env_(base), cloud_env_options(options),
    provider_(std::move(provider)),
    controller_(std::move(controller)) {
}

CloudEnv::~CloudEnv() {
  controller_.reset();
  provider_.reset();
}

const char* CloudEnv::Name() const {
    return kEnvCloudName.c_str();
}

const Env* CloudEnv::FindInstance(const std::string& name) const {
  if (name == Name() || name == kEnvCloudName) {
    return this;
  } else if (name == "Default") {
    return base_env_;
  } else {
    return nullptr;
  }
}

void CloudEnv::Dump(Logger* log) const {
  if (provider_) {
    provider_->Dump(log);
  }
  if (controller_) {
    controller_->Dump(log);
  }
  Header(log, "               COptions.keep_local_sst_files: %d",
         cloud_env_options.keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         cloud_env_options.keep_local_log_files);
  Header(log, "           COptions.create_bucket_if_missing: %s",
         cloud_env_options.create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         cloud_env_options.run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         cloud_env_options.ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         cloud_env_options.skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         cloud_env_options.number_objects_listed_in_one_iteration);
}

Status CloudEnv::CreateCloudEnv(const std::string& name, Env* base_env,
                                const CloudEnvOptions& options,
                                std::unique_ptr<CloudEnv>* result) {
  return CreateCloudEnv(name, base_env, options, nullptr, nullptr, result);
}

Status CloudEnv::CreateCloudEnv(const std::string& name, Env* base_env,
                                const CloudEnvOptions& options,
                                std::unique_ptr<CloudStorageProvider> provider,
                                std::unique_ptr<CloudLogController> controller,
                                std::unique_ptr<CloudEnv>* result) {
  Status st = Status::OK();
  // Dump out cloud env options
  std::string id;
  bool is_test = CloudImplConstants::IsTestId(name, &id);
  if (id == kAwsCloudName) {
    st = AwsEnv::NewAwsEnv(base_env, options, std::move(provider), std::move(controller), result);
  } else {
    return Status::NotSupported("Unknown Cloud Environment: ", name);
  }
  if (st.ok()) {
    auto impl = CloudEnvImpl::AsImpl(result->get());
    if (impl != nullptr) {
      if (is_test) {
        impl->TEST_Initialize();
      }
      st = impl->Prepare();
    }
  }
  if (st.ok()) {
    result->get()->Dump(options.info_log.get());
  }    
  return st;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
