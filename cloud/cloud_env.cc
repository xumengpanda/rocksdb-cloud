// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#ifndef _WIN32_WINNT
#include <unistd.h>
#else
#include <windows.h>
#endif
#include <unordered_map>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

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
  credentials.TEST_Initialize();
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
static std::unordered_map<std::string, OptionTypeInfo>
    bucket_options_type_info = {
        {"object",
         {0, OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto bucket = reinterpret_cast<BucketOptions*>(addr);
            bucket->SetObjectPath(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr, std::string* value) {
            auto bucket = reinterpret_cast<const BucketOptions*>(addr);
            *value = bucket->GetObjectPath();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr1, const char* addr2, std::string* /*mismatch*/) {
            auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
            auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
            return bucket1->GetObjectPath() == bucket2->GetObjectPath();
          }}},
        {"region",
         {0, OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kCompareNever,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto bucket = reinterpret_cast<BucketOptions*>(addr);
            bucket->SetRegion(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr, std::string* value) {
            auto bucket = reinterpret_cast<const BucketOptions*>(addr);
            *value = bucket->GetRegion();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr1, const char* addr2, std::string* /*mismatch*/) {
            auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
            auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
            return bucket1->GetRegion() == bucket2->GetRegion();
          }}},
        {"prefix",
         {0, OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto bucket = reinterpret_cast<BucketOptions*>(addr);
            bucket->SetBucketName(bucket->GetBucketName(false), value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr, std::string* value) {
            auto bucket = reinterpret_cast<const BucketOptions*>(addr);
            *value = bucket->GetBucketPrefix();
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr1, const char* addr2, std::string* /*mismatch*/) {
            auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
            auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
            return bucket1->GetBucketPrefix() == bucket2->GetBucketPrefix();
          }}},
        {"bucket",
         {0, OptionType::kString,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto bucket = reinterpret_cast<BucketOptions*>(addr);
            bucket->SetBucketName(value);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr, std::string* value) {
            auto bucket = reinterpret_cast<const BucketOptions*>(addr);
            *value = bucket->GetBucketName(false);
            return Status::OK();
          },
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const char* addr1, const char* addr2, std::string* /*mismatch*/) {
            auto bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
            auto bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
            return bucket1->GetBucketName(false) == bucket2->GetBucketName(false);
          }}},
};

static CloudEnvOptions dummy_ceo_options;
template <typename T1>
int offset_of(T1 CloudEnvOptions::*member) {
  return int(size_t(&(dummy_ceo_options.*member)) - size_t(&dummy_ceo_options));
}

  
static std::unordered_map<std::string, OptionTypeInfo>
    cloud_env_option_type_info = {
        {"keep_local_sst_files",
         {offset_of(&CloudEnvOptions::keep_local_sst_files), OptionType::kBoolean}},
        {"keep_local_log_files",
         {offset_of(&CloudEnvOptions::keep_local_log_files), OptionType::kBoolean}},
        {"create_bucket_if_missing",
         {offset_of(&CloudEnvOptions::create_bucket_if_missing), OptionType::kBoolean}},
        {"validate_filesize",
         {offset_of(&CloudEnvOptions::validate_filesize), OptionType::kBoolean}},
        {"skip_dbid_verification",
         {offset_of(&CloudEnvOptions::skip_dbid_verification), OptionType::kBoolean}},
        {"ephemeral_resync_on_open",
         {offset_of(&CloudEnvOptions::ephemeral_resync_on_open), OptionType::kBoolean}},
        {"skip_cloud_children_files",
         {offset_of(&CloudEnvOptions::skip_cloud_files_in_getchildren), OptionType::kBoolean}},
        {"constant_sst_file_size_in_manager",
         {offset_of(&CloudEnvOptions::constant_sst_file_size_in_sst_file_manager), OptionType::kInt64T}},
        {"run_purger",
         {offset_of(&CloudEnvOptions::run_purger), OptionType::kBoolean}},
        {"purger_periodicity_ms",
         {offset_of(&CloudEnvOptions::purger_periodicity_millis), OptionType::kUInt64T}},
        
        {"provider",
         {offset_of(&CloudEnvOptions::storage_provider),
          OptionType::kConfigurable, OptionVerificationType::kByNameAllowNull,
          (OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
           OptionTypeFlags::kCompareNever | OptionTypeFlags::kAllowNull),
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto provider =
                reinterpret_cast<std::shared_ptr<CloudStorageProvider>*>(addr);
            return CloudStorageProvider::CreateFromString(opts, value,
                                                          provider);
          }}},
        {"controller",
         {offset_of(&CloudEnvOptions::cloud_log_controller),
          OptionType::kConfigurable, OptionVerificationType::kByNameAllowNull,
          (OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose |
           OptionTypeFlags::kCompareNever | OptionTypeFlags::kAllowNull),
          // Creates a new TableFactory based on value
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto controller =
                reinterpret_cast<std::shared_ptr<CloudLogController>*>(addr);
            return CloudLogController::CreateFromString(opts, value,
                                                        controller);
          }}},
        {"src",
         OptionTypeInfo::Struct("src",
                                &bucket_options_type_info, 
                                offset_of(&CloudEnvOptions::src_bucket),
                                OptionVerificationType::kNormal, OptionTypeFlags::kNone)
        },
        {"dest",
         OptionTypeInfo::Struct("dest",
                                &bucket_options_type_info, 
                                offset_of(&CloudEnvOptions::dest_bucket),
                                OptionVerificationType::kNormal, OptionTypeFlags::kNone)
        },
};
  
Status CloudEnvOptions::Configure(const ConfigOptions& config_options,
                                  const std::string& opts_str) {
  std::string current;
  Status s;
  if (!config_options.ignore_unknown_options) {
    s = Serialize(config_options, &current);
    if (!s.ok()) {
      return s;
    }
  }
  if (s.ok()) {
    s = OptionTypeInfo::ParseStruct(config_options, CloudEnvOptions::kName(),
                                    &cloud_env_option_type_info,
                                    CloudEnvOptions::kName(), opts_str, reinterpret_cast<char*>(this));
    if (!s.ok()) { // Something went wrong.  Attempt to reset
      OptionTypeInfo::ParseStruct(config_options, CloudEnvOptions::kName(),
                                  &cloud_env_option_type_info,
                                  CloudEnvOptions::kName(), current, reinterpret_cast<char*>(this));
    }
  }
  return s;
}
  
Status CloudEnvOptions::Serialize(const ConfigOptions& config_options, std::string* value) const {
  return OptionTypeInfo::SerializeStruct(config_options, CloudEnvOptions::kName(),
                                         &cloud_env_option_type_info,
                                         CloudEnvOptions::kName(), reinterpret_cast<const char*>(this), value);
}

CloudEnv::CloudEnv(const CloudEnvOptions& options, Env* base,
                   const std::shared_ptr<Logger>& logger)
    : cloud_env_options(options), base_env_(base), info_log_(logger) {
  ConfigurableHelper::RegisterOptions(*this, &cloud_env_options,
                                      &cloud_env_option_type_info);
}

CloudEnv::~CloudEnv() {
  cloud_env_options.cloud_log_controller.reset();
  cloud_env_options.storage_provider.reset();
}

Status CloudEnv::NewAwsEnv(
    Env* base_env, const std::string& src_cloud_bucket,
    const std::string& src_cloud_object, const std::string& src_cloud_region,
    const std::string& dest_cloud_bucket, const std::string& dest_cloud_object,
    const std::string& dest_cloud_region, const CloudEnvOptions& cloud_options,
    const std::shared_ptr<Logger>& logger, CloudEnv** cenv) {
  CloudEnvOptions options = cloud_options;
  if (!src_cloud_bucket.empty())
    options.src_bucket.SetBucketName(src_cloud_bucket);
  if (!src_cloud_object.empty())
    options.src_bucket.SetObjectPath(src_cloud_object);
  if (!src_cloud_region.empty()) options.src_bucket.SetRegion(src_cloud_region);
  if (!dest_cloud_bucket.empty())
    options.dest_bucket.SetBucketName(dest_cloud_bucket);
  if (!dest_cloud_object.empty())
    options.dest_bucket.SetObjectPath(dest_cloud_object);
  if (!dest_cloud_region.empty())
    options.dest_bucket.SetRegion(dest_cloud_region);
  return NewAwsEnv(base_env, options, logger, cenv);
}

  int DoRegisterCloudObjects(ObjectLibrary& library, const std::string& /*arg*/) {
  int count = 0;
  // Register the Env types
  library.Register<Env>(
        CloudEnv::kName(),
        [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
           std::string* /*errmsg*/) {
          guard->reset(new CloudEnvImpl(CloudEnvOptions(), Env::Default(), nullptr));
          return guard->get();
        });
  count++;
  library.Register<Env>(
      CloudEnv::kAws(),
      [](const std::string& /*uri*/, std::unique_ptr<Env>* guard,
         std::string* errmsg) {
        std::unique_ptr<CloudEnv> cguard;
        Status s = AwsEnv::NewAwsEnv(Env::Default(), &cguard);
        if (s.ok()) {
          guard->reset(cguard.release());
          return guard->get();
        } else {
          *errmsg = s.ToString();
          return static_cast<Env*>(nullptr);
        }
      });
  count++;

  // Register the Cloud Log Controllers

  library.Register<CloudLogController>(
      CloudLogController::kKinesis(), 
      [](const std::string& /*uri*/, std::unique_ptr<CloudLogController>* guard,
         std::string* errmsg) {
        Status s = CloudLogControllerImpl::CreateKinesisController(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
  
  library.Register<CloudLogController>(
      CloudLogController::kKafka(), 
      [](const std::string& /*uri*/, std::unique_ptr<CloudLogController>* guard,
         std::string* errmsg) {
        Status s = CloudLogControllerImpl::CreateKafkaController(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
     

  // Register the Cloud Storage Providers
  
  library.Register<CloudStorageProvider>( // s3
        CloudStorageProvider::kAws(), 
        [](const std::string& /*uri*/, std::unique_ptr<CloudStorageProvider>* guard,
           std::string* errmsg) {
          Status s = CloudStorageProviderImpl::CreateS3Provider(guard);
          if (!s.ok()) {
            *errmsg = s.ToString();
          }
          return guard->get();
        });
  count++;
  
  return count;
}
  
void CloudEnv::RegisterCloudObjects(const std::string& arg) {
  static std::once_flag do_once;
  std::call_once(do_once,
    [&]() {
      auto library = ObjectLibrary::Default();
      DoRegisterCloudObjects(*library, arg);
    });
}     

Status CloudEnv::CreateFromString(const ConfigOptions& config_options, const std::string& value,
                                  std::unique_ptr<CloudEnv>* result) {
  RegisterCloudObjects();
  std::string id;
  std::unordered_map<std::string, std::string> options;  
  Status s;
  if (value.find("=") == std::string::npos) {
    id = value;
  } else {
    s = StringToMap(value, &options);
    if (s.ok()) {
      auto iter = options.find("id");
      if (iter != options.end()) {
        id = iter->second;
        options.erase(iter);
      } else {
        id = CloudEnv::kName();
      }
    }
  }
  if (!s.ok()) {
    return s;
  }
  ConfigOptions copy = config_options;
  std::unique_ptr<Env> env;
  copy.invoke_prepare_options = false;  // Prepare here, not there
  s = ObjectRegistry::NewInstance()->NewUniqueObject<Env>(id, &env);
  if (s.ok()) {
    CloudEnv* cenv = static_cast<CloudEnv*>(env.get());
    if (!options.empty()) {
      s = cenv->ConfigureFromMap(copy, options);
    }
    if (s.ok() && config_options.invoke_prepare_options) {
      copy.invoke_prepare_options = config_options.invoke_prepare_options;
      copy.env = cenv;
      s = cenv->PrepareOptions(copy);
      if (s.ok()) {
        Options tmp;
        s = cenv->ValidateOptions(tmp, tmp);
      }
    }
  }
  
  if (s.ok()) {
    result->reset(static_cast<CloudEnv*>(env.release()));
  }
  
  return s;  
}
Status CloudEnv::CreateFromString(const ConfigOptions& config_options, const std::string& value,
                                  const CloudEnvOptions& cloud_options,
                                  std::unique_ptr<CloudEnv>* result) {
  RegisterCloudObjects();
  std::string id;
  std::unordered_map<std::string, std::string> options;  
  Status s;
  if (value.find("=") == std::string::npos) {
    id = value;
  } else {
    s = StringToMap(value, &options);
    if (s.ok()) {
      auto iter = options.find("id");
      if (iter != options.end()) {
        id = iter->second;
        options.erase(iter);
      } else {
        id = CloudEnv::kName();
      }
    }
  }
  if (!s.ok()) {
    return s;
  }
  ConfigOptions copy = config_options;
  std::unique_ptr<Env> env;
  copy.invoke_prepare_options = false;  // Prepare here, not there
  s = ObjectRegistry::NewInstance()->NewUniqueObject<Env>(id, &env);
  if (s.ok()) {
    CloudEnv* cenv = static_cast<CloudEnv*>(env.get());
    auto copts = cenv->GetOptions<CloudEnvOptions>();
    *copts = cloud_options;
    if (!options.empty()) {
      s = cenv->ConfigureFromMap(copy, options);
    }
    if (s.ok() && config_options.invoke_prepare_options) {
      copy.invoke_prepare_options = config_options.invoke_prepare_options;
      copy.env = cenv;
      s = cenv->PrepareOptions(copy);
      if (s.ok()) {
        Options tmp;
        s = cenv->ValidateOptions(tmp, tmp);
      }
    }
  }
  
  if (s.ok()) {
    result->reset(static_cast<CloudEnv*>(env.release()));
  }
  
  return s;  
}
  
#ifndef USE_AWS
Status CloudEnv::NewAwsEnv(Env* /*base_env*/,
                           const CloudEnvOptions& /*options*/,
                           const std::shared_ptr<Logger>& /*logger*/,
                           CloudEnv** /*cenv*/) {
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
}
#else
Status CloudEnv::NewAwsEnv(Env* base_env, const CloudEnvOptions& options,
                           const std::shared_ptr<Logger>& logger,
                           CloudEnv** cenv) {
  CloudEnv::RegisterCloudObjects();
  // Dump out cloud env options
  options.Dump(logger.get());

  Status st = AwsEnv::NewAwsEnv(base_env, options, logger, cenv);
  if (st.ok()) {
    // store a copy of the logger
    CloudEnvImpl* cloud = static_cast<CloudEnvImpl*>(*cenv);
    cloud->info_log_ = logger;

    // start the purge thread only if there is a destination bucket
    if (options.dest_bucket.IsValid() && options.run_purger) {
      cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
    }
  }
  return st;
}
#endif

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
