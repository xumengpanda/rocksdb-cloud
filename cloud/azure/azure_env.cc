//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#include <unistd.h>
#include <fstream>
#include <iostream>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#include "cloud/azure/azure_env.h"

#ifdef USE_AZURE

#include "cloud/db_cloud_impl.h"

namespace rocksdb {

AzureEnv::AzureEnv(Env* underlying_env, const std::string& src_bucket_prefix,
                   const std::string& src_object_prefix,
                   const std::string& dest_bucket_prefix,
                   const std::string& dest_object_prefix,
                   const CloudEnvOptions& _cloud_env_options,
                   std::shared_ptr<Logger> info_log)
    : CloudEnvImpl(CloudType::kAzure, underlying_env),
      info_log_(info_log),
      cloud_env_options(_cloud_env_options),
      src_bucket_prefix_(src_bucket_prefix),
      src_object_prefix_(src_object_prefix),
      dest_bucket_prefix_(dest_bucket_prefix),
      dest_object_prefix_(dest_object_prefix),
      running_(true),
      has_src_bucket_(false),
      has_dest_bucket_(false),
      has_two_unique_buckets_(false) {
  src_bucket_prefix_ = trim(src_bucket_prefix_);
  src_object_prefix_ = trim(src_object_prefix_);
  dest_bucket_prefix_ = trim(dest_bucket_prefix_);
  dest_object_prefix_ = trim(dest_object_prefix_);

  base_env_ = underlying_env;
}

AzureEnv::~AzureEnv() {
  running_ = false;
  StopPurger();
  if (tid_.joinable()) {
    tid_.join();
  }
}

Status AzureEnv::NewLogger(const std::string& fname, shared_ptr<Logger>* result) {
  return base_env_->NewLogger(fname, result);
}

Status AzureEnv::DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) {
  // fetch the list all all dbids
  std::string dbidkey = dbid_registry_ + dbid;
  return Status::OK();
}

Status AzureEnv::DeleteFile(const std::string& fname) { return Status::OK(); }

Status AzureEnv::RenameFile(const std::string& src, const std::string& target) {
  return Status::OK();
}

Status AzureEnv::FileExists(const std::string& fname) { return Status::OK(); }

Status AzureEnv::UnlockFile(FileLock* lock) { return Status::OK(); }

Status AzureEnv::LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;
  return Status::OK();
}

Status AzureEnv::EmptyBucket(const std::string& bucket_prefix) {
  return Status::OK();
}

Status AzureEnv::GetChildren(const std::string& path,
                             std::vector<std::string>* result) {
  return Status::OK();
}

Status AzureEnv::NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) {
  return Status::OK();
}

Status AzureEnv::DeleteDir(const std::string& dirname) { return Status::OK(); }

Status AzureEnv::GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) {
  return Status::OK();
}

Status AzureEnv::SaveDbid(const std::string& dbid, const std::string& dirname) {
  return Status::OK();
}

Status AzureEnv::GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid, std::string* dirname) {
  return Status::OK();
}

Status AzureEnv::GetFileSize(const std::string& fname, uint64_t* size) {
  return Status::OK();
}

Status AzureEnv::NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::CreateDir(const std::string& dirname) { return Status::OK(); }

Status AzureEnv::CreateDirIfMissing(const std::string& dirname) {
  return Status::OK();
}

Status AzureEnv::NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) {
  return Status::OK();
}
}
#endif  // USE_AZURE
