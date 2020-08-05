//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <memory>

#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
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
// Type of AWS access credentials
enum class AwsAccessType {
  kUndefined,  // Use AWS SDK's default credential chain
  kSimple,
  kInstance,
  kTaskRole,
  kEnvironment,
  kConfig,
  kAnonymous,
};

// Credentials needed to access AWS cloud service
class AwsCloudAccessCredentials {
 public:
  static const std::string kAwsCredentials;
  
  // functions to support AWS credentials
  //
  // Initialize AWS credentials using access_key_id and secret_key
  void InitializeSimple(const std::string& aws_access_key_id,
                        const std::string& aws_secret_key);
  // Initialize AWS credentials using a config file
  void InitializeConfig(const std::string& aws_config_file);

  // test if valid AWS credentials are present
  Status HasValid() const;
  // Get AWSCredentialsProvider to supply to AWS API calls when required (e.g.
  // to create S3Client)
  Status GetCredentialsProvider(
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider>* result) const;
  // Initialize credentials for tests (relies on config vars)
  Status TEST_Initialize();

 private:
  AwsAccessType GetAccessType() const;
  Status CheckCredentials(const AwsAccessType& aws_type) const;

 public:
  std::string access_key_id;
  std::string secret_key;
  std::string config_file;
  AwsAccessType type{AwsAccessType::kUndefined};
  static const std::shared_ptr<AwsCloudAccessCredentials>& Default();
};

struct S3ProviderOptions {
  // If true, we will use AWS TransferManager instead of Put/Get operaations to
  // download and upload S3 files.
  // Default: false
  bool use_aws_transfer_manager = false;
};
} // namespace ROCKSDB_NAMESPACE
