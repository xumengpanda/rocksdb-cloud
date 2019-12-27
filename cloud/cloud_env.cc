// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <unistd.h>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {

bool CloudEnvOptions::GetNameFromEnvironment(const char *name, const char *alt, std::string * result) {

  char *value = getenv(name);               // See if name is set in the environment
  if (value == nullptr && alt != nullptr) { // Not set.  Do we have an alt name?
    value = getenv(alt);                    // See if alt is in the environment
  }
  if (value != nullptr) {                   // Did we find the either name/alt in the env?
    result->assign(value);                  // Yes, update result
    return true;                            // And return success
  } else {
    return false;                           // No, return not found
  }
}
void CloudEnvOptions::TEST_Initialize(const std::string& bucket,
                                      const std::string& object,
                                      const std::string& region) {
  src_bucket.TEST_Initialize(bucket, object, region);
  dest_bucket = src_bucket;
  credentials.TEST_Initialize();
}

static BucketOptions dummy_bucket_options;
template <typename T1>
int offset_of(T1 BucketOptions::*member) {
  return int(size_t(&(dummy_bucket_options.*member)) - size_t(&dummy_bucket_options));
}
  
OptionTypeMap BucketOptions::type_info = {
    {"region",
     {offset_of(&BucketOptions::region_),
      OptionType::kString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0}},
    {"object",
     {offset_of(&BucketOptions::object_),
      OptionType::kString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0}},
    {"prefix",
     {0, OptionType::kString, OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0,
      [](const std::string&, const std::string& value,
         const ConfigOptions&, char* addr) {
        auto bucket = reinterpret_cast<BucketOptions*>(addr);
        bucket->SetBucketName(bucket->bucket_, value);
        return Status::OK();
      },
      [](const std::string&, const char* addr, const ConfigOptions&, std::string* value) {
        const auto bucket = reinterpret_cast<const BucketOptions*>(addr);
        *value = bucket->prefix_;
        return Status::OK();
      },
      [](const std::string&, const char* addr1, const char* addr2, const ConfigOptions&,
         std::string*) {
        const auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        return bucket1->prefix_ == bucket2->prefix_;
      }
     }},
    {"bucket",
     {0,
      OptionType::kString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0,
      [](const std::string&, const std::string& value,
         const ConfigOptions&, char* addr) {
        auto bucket = reinterpret_cast<BucketOptions*>(addr);
        bucket->SetBucketName(value);
        return Status::OK();
      },
      [](const std::string&, const char* addr, const ConfigOptions&, std::string* value) {
        const auto bucket = reinterpret_cast<const BucketOptions*>(addr);
        *value = bucket->bucket_;
        return Status::OK();
      },
      [](const std::string&, const char* addr1, const char* addr2, const ConfigOptions&,
         std::string*) {
        const auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        return bucket1->bucket_ == bucket2->bucket_;
      }
     }},

};

BucketOptions::BucketOptions() {
    prefix_ = "rockset.";
    RegisterOptions("BucketOptions", this, &type_info);
}

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
    bucket_ = bucket + std::to_string(geteuid());
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

const std::string CloudEnv::kCloudEnvName ="CloudEnv";
const std::string CloudEnv::kCloudEnvOpts ="CloudEnvOptions";
static CloudEnvOptions dummy_cloud_options;

template <typename T1>
int offset_of(T1 CloudEnvOptions::*member) {
  return int(size_t(&(dummy_cloud_options.*member)) - size_t(&dummy_cloud_options));
}

static OptionTypeMap cloud_type_info = {
    {"keep_local_log_files",
     {offset_of(&CloudEnvOptions::keep_local_log_files),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"keep_local_sst_files",
     {offset_of(&CloudEnvOptions::keep_local_sst_files),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"purger_periodicity_millis",
     {offset_of(&CloudEnvOptions::purger_periodicity_millis),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"validate_file_size",
     {offset_of(&CloudEnvOptions::validate_filesize),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"create_bucket_if_missing",
     {offset_of(&CloudEnvOptions::create_bucket_if_missing),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"request_timeout_ms",
     {offset_of(&CloudEnvOptions::request_timeout_ms),
      OptionType::kUInt64T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"run_purger",
     {offset_of(&CloudEnvOptions::run_purger),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"ephemeral_resync_on_open",
     {offset_of(&CloudEnvOptions::ephemeral_resync_on_open),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"skip_dbid_verification",
     {offset_of(&CloudEnvOptions::skip_dbid_verification),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0
     }},
    {"bucket.source",
     {offset_of(&CloudEnvOptions::src_bucket),
      OptionType::kStruct, OptionVerificationType::kNormal,
      OptionTypeFlags::kStruct, 0,
      nullptr,
      nullptr,
      [](const std::string&/*name*/, const char* addr1, const char* addr2,
         const ConfigOptions& /*opts*/, std::string* /*mismatch*/) {
        const BucketOptions* bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const BucketOptions* bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        return *bucket1 == *bucket2;
      }
     }},
    {"bucket.dest",
     {offset_of(&CloudEnvOptions::dest_bucket),
      OptionType::kStruct, OptionVerificationType::kNormal,
      OptionTypeFlags::kStruct, 0,
      nullptr,
      nullptr,
      [](const std::string&/*name*/, const char* addr1, const char* addr2,
         const ConfigOptions& /*opts*/, std::string* /*mismatch*/) {
        const auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        return *bucket1 == *bucket2;
      }
     }},
    {"storage_provider",
      OptionTypeInfo::AsCustomS<CloudStorageProvider>(
          offset_of(&CloudEnvOptions::storage_provider),
          OptionVerificationType::kByName)
    },
    {"log_controller",
      OptionTypeInfo::AsCustomS<CloudLogController>(
          offset_of(&CloudEnvOptions::log_controller),
          OptionVerificationType::kByNameAllowFromNull)
    },
};

//**TODO: If a CloudEnv extends EnvWrapper. this can go away
static OptionTypeMap env_target_type_info = {
    {"target",
     OptionTypeInfo::AsCustomP<Env>(0, OptionVerificationType::kByName)
    },
};

CloudEnv::CloudEnv(const CloudEnvOptions& options, Env* base, const std::shared_ptr<Logger>& logger)
    : cloud_env_options(options), base_env_(base), info_log_(logger) {
  RegisterOptions(kCloudEnvOpts, &cloud_env_options, &cloud_type_info);
  RegisterOptions("BaseEnvOptions", &base_env_, &env_target_type_info); //**See TODO above
}

CloudEnv::~CloudEnv() {
  cloud_env_options.storage_provider.reset();
  cloud_env_options.log_controller.reset();
}

const char *CloudEnv::Name() const {
  return kCloudEnvName.c_str();
}
  
Env* CloudEnv::Find(const std::string& name) {
  if (name == kCloudEnvName) {
    return this;
  } else if (name == Name()) {
    return this;
  } else if (base_env_ != nullptr) {
    return base_env_->Find(name);
  } else {
    return nullptr;
  }
}
  
const OptionTypeMap* CloudEnv::GetOptionsTypeMap(const std::string& option) const {
  if (option == "bucket.source" || option == "bucket.dest") {
    return &BucketOptions::type_info;
  } else {
    return Env::GetOptionsTypeMap(option);
  }
}
    
Status CloudEnv::Verify() const {
  Status s;
  if (!cloud_env_options.storage_provider) {
    s = Status::InvalidArgument("Cloud environment requires a storage provider");
  } else {
    s = cloud_env_options.storage_provider->Verify();
  }
  if (s.ok()) {
    if (cloud_env_options.log_controller) {
      s = cloud_env_options.log_controller->Verify();
    } else if (!cloud_env_options.keep_local_log_files) {
      s = Status::InvalidArgument("Log controller required for remote log files");
    }
  }
  return s;
}

Status CloudEnv::Sanitize(DBOptions& db_opts, ColumnFamilyOptions& /*cf_opts*/) {
  auto *cloud = db_opts.env->AsEnv<CloudEnv>(kCloudEnvName);
  if (cloud == nullptr || cloud != this) {
    return Status::InvalidArgument("Invalid cloud environment");
  } else {
    return Prepare();
  }
}
  
Status CloudEnv::Validate(const DBOptions& db_opts, const ColumnFamilyOptions& /*cf_opts*/) const {
  auto *cloud = db_opts.env->AsEnv<CloudEnv>(kCloudEnvName);
  if (cloud == nullptr || cloud != this) {
    return Status::InvalidArgument("Invalid cloud environment");
  } else {
    return Verify();
  }
}
  
Status CloudEnv::Prepare() {
  Header(info_log_, "     %s.src_bucket_name: %s",
         Name(), cloud_env_options.src_bucket.GetBucketName().c_str());
  Header(info_log_, "     %s.src_object_path: %s",
         Name(), cloud_env_options.src_bucket.GetObjectPath().c_str());
  Header(info_log_, "     %s.src_bucket_region: %s",
         Name(), cloud_env_options.src_bucket.GetRegion().c_str());
  Header(info_log_, "     %s.dest_bucket_name: %s",
         Name(), cloud_env_options.dest_bucket.GetBucketName().c_str());
  Header(info_log_, "     %s.dest_object_path: %s",
         Name(), cloud_env_options.dest_bucket.GetObjectPath().c_str());
  Header(info_log_, "     %s.dest_bucket_region: %s",
         Name(), cloud_env_options.dest_bucket.GetRegion().c_str());

  Status s;
  if (cloud_env_options.src_bucket.GetBucketName().empty() !=
      cloud_env_options.src_bucket.GetObjectPath().empty()) {
    s = Status::InvalidArgument("Must specify both src bucket name and path");
  } else if (cloud_env_options.dest_bucket.GetBucketName().empty() != 
             cloud_env_options.dest_bucket.GetObjectPath().empty()) {
    s = Status::InvalidArgument("Must specify both dest bucket name and path");
  } else if (cloud_env_options.storage_provider) {
    Header(info_log_, "     %s.storage_provider: %s",
           Name(), cloud_env_options.storage_provider->Name());
    s = cloud_env_options.storage_provider->Prepare(this);
  } else if (HasSrcBucket() || HasDestBucket()) {
    s = Status::InvalidArgument("Cloud environment requires a storage provider");
  }
  if (s.ok()) {
    if (cloud_env_options.log_controller) {
      Header(info_log_, "     %s.log controller: %s",
             Name(), cloud_env_options.log_controller->Name());
      s = cloud_env_options.log_controller->Prepare(this);
    } else if (!cloud_env_options.keep_local_log_files) {
      s = Status::InvalidArgument("Log controller required for remote log files");
    }
  }
  return s;
}


Status CloudEnv::NewAwsEnv(
    Env* base_env, const std::string& src_cloud_bucket,
    const std::string& src_cloud_object, const std::string& src_cloud_region,
    const std::string& dest_cloud_bucket, const std::string& dest_cloud_object,
    const std::string& dest_cloud_region, const CloudEnvOptions& cloud_options,
    const std::shared_ptr<Logger>& logger, CloudEnv** cenv) {
  CloudEnvOptions options = cloud_options;
  if (!src_cloud_bucket.empty()) options.src_bucket.SetBucketName(src_cloud_bucket);
  if (!src_cloud_object.empty()) options.src_bucket.SetObjectPath(src_cloud_object);
  if (!src_cloud_region.empty()) options.src_bucket.SetRegion(src_cloud_region);
  if (!dest_cloud_bucket.empty()) options.dest_bucket.SetBucketName(dest_cloud_bucket);
  if (!dest_cloud_object.empty()) options.dest_bucket.SetObjectPath(dest_cloud_object);
  if (!dest_cloud_region.empty()) options.dest_bucket.SetRegion(dest_cloud_region);
  return NewAwsEnv(base_env, options, logger, cenv);
}

#ifndef USE_AWS
Status CloudEnv::NewAwsEnv(Env*, const CloudEnvOptions&,
                           const std::shared_ptr<Logger>&, CloudEnv**) {
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
}
#else
Status CloudEnv::NewAwsEnv(Env* base_env, const CloudEnvOptions& options,
                           const std::shared_ptr<Logger>& logger,
                           CloudEnv** cenv) {
  // Dump out cloud env options
  options.Dump(logger.get());

  Status st = AwsEnv::NewAwsEnv(base_env, options, logger, cenv);
  return st;
}
#endif

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
