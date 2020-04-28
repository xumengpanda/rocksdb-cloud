#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "cloud/aws/aws_env.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace rocksdb {

class AwsOptionsTest : public testing::Test {};
#ifdef USE_AWS
#ifndef ROCKSDB_LITE
static Status RegisterAws(ConfigOptions &config_options) {
#ifdef ROCKSDB_DLL
  return config_options.registry->AddDynamicLibrary(
      config_options.env, "rocksdb_cloud_debug", "RegisterAwsObjects", "shared:standalone");
#else
  config_options.registry->AddLocalLibrary(RegisterAwsObjects, "RegisterAwsObjects",
                                     "static:standalone");
  return Status::OK();
#endif  // ROCKSDB_DLL
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
TEST_F(AwsOptionsTest, TestLoadAws) {
  DBOptions db_opts;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(RegisterAws(config_options));
  ASSERT_OK(Env::CreateFromString(
      config_options, "bucket.source={bucket=test;object=path;region=east}; "
      "bucket.dest={bucket=test;object=path;region=west}; "
      "id=AWS; storage_provider=S3; log_controller=Kinesis",
      &db_opts.env));
  ASSERT_NE(db_opts.env, nullptr);
  ASSERT_NOK(
      db_opts.env->PrepareOptions(config_options));  // AWS requires regions to match
  delete db_opts.env;
  db_opts.env = nullptr;

  ASSERT_OK(
      Env::CreateFromString(config_options, "bucket.source={bucket=test;object=path}; "
                            "bucket.dest={bucket=test;object=path}; "
                            "id=AWS; storage_provider=S3;",
                            &db_opts.env));
  ASSERT_NE(db_opts.env, nullptr);
  auto *options =
      db_opts.env->GetOptions<CloudEnvOptions>(CloudOptionNames::kNameCloud);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->storage_provider, nullptr);
  ASSERT_EQ(options->storage_provider->GetId(), "S3");
  ASSERT_OK(db_opts.env->PrepareOptions(config_options));

  CloudEnv *cloud = db_opts.env->CastAs<CloudEnv>(CloudOptionNames::kNameAws);
  ASSERT_NE(cloud, nullptr);
  ASSERT_EQ(cloud, db_opts.env);
  cloud = db_opts.env->CastAs<CloudEnv>("CloudEnvImpl");
  ASSERT_NE(cloud, nullptr);
  ASSERT_EQ(cloud, db_opts.env);
  cloud = db_opts.env->CastAs<CloudEnv>(CloudOptionNames::kNameCloud);
  ASSERT_NE(cloud, nullptr);
  ASSERT_EQ(cloud, db_opts.env);
  delete db_opts.env;
  db_opts.env = nullptr;
}

TEST_F(AwsOptionsTest, TestLoadSharedAws) {
  Env *env = nullptr;
  std::shared_ptr<Env> env_guard;
  
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(RegisterAws(config_options));
  ASSERT_OK(Env::CreateFromString(config_options, "id=AWS", &env));
  ASSERT_NE(env, nullptr);
  ASSERT_NOK(Env::CreateFromString(config_options, "id=AWS", &env, &env_guard));
  ASSERT_OK(Env::CreateFromString(config_options, "id=AWS:shared", &env, &env_guard));
  ASSERT_NE(env, nullptr);
  ASSERT_EQ(env, env_guard.get());
}
  
TEST_F(AwsOptionsTest, TestLoadGuardedAws) {
  DBOptions db_opts;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(RegisterAws(config_options));
  std::shared_ptr<Env> env_guard;
  
  ASSERT_NOK(Env::CreateFromString(config_options,"id=AWS", 
                                   &db_opts.env, &env_guard));
  ASSERT_OK(Env::CreateFromString(config_options,"id=AWS:shared", 
                                  &db_opts.env, &env_guard));
  ASSERT_EQ(db_opts.env, env_guard.get());
}

TEST_F(AwsOptionsTest, TestAwsEnvOptions) {
  Env *aws = nullptr;
  Env *copy = nullptr;

  ConfigOptions config_options;
  ASSERT_OK(RegisterAws(config_options));
  ASSERT_OK(Env::CreateFromString(config_options,  "id=AWS", &aws));
  ASSERT_OK(Env::CreateFromString(config_options,  "id=AWS", &copy));

  std::string opt_str;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.server_side_encryption=true; aws.encryption_key_id=my-key; "
      "aws.use_transfer_manager=false"));
  const auto *options =
      aws->GetOptions<CloudEnvOptions>(CloudOptionNames::kNameCloud);
  ASSERT_NE(options, nullptr);
  ASSERT_TRUE(options->server_side_encryption);
  ASSERT_EQ(options->encryption_key_id, "my-key");
  ASSERT_FALSE(options->use_aws_transfer_manager);
  ASSERT_OK(aws->GetOptionString(config_options, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(config_options, opt_str));
  ASSERT_TRUE(copy->Matches(config_options, aws));

  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.server_side_encryption=false; aws.encryption_key_id=; "
      "aws.use_transfer_manager=true"));
  ASSERT_FALSE(options->server_side_encryption);
  ASSERT_EQ(options->encryption_key_id, "");
  ASSERT_TRUE(options->use_aws_transfer_manager);
  delete aws;
  delete copy;
}

TEST_F(AwsOptionsTest, TestAwsCredentialOptions) {
  // Note that this test does not compile when loading against a shared library
  // because the symbols are defined in the shared library that is not linked
  // into the executable
#ifndef ROCKSDB_DLL
  Env *aws = nullptr;
  ConfigOptions config_options;
  ASSERT_OK(RegisterAws(config_options));
  config_options.invoke_prepare_options = false;
  ASSERT_OK(Env::CreateFromString(config_options, "id=AWS", &aws));

  auto *creds = aws->GetOptions<AwsCloudAccessCredentials>("AwsCredentials");
  bool has_env_creds = (getenv("AWS_ACCESS_KEY_ID") != nullptr &&
                        getenv("AWS_SECRET_ACCESS_KEY") != nullptr);
  ASSERT_NE(creds, nullptr);
  if (has_env_creds) {
    ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kEnvironment);
    ASSERT_OK(creds->HasValid());
  } else {
    ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kUndefined);
    ASSERT_NOK(creds->HasValid());
  }

  // Test simple creds.  Simple are valid if both keys are specified or in the
  // environment
  ASSERT_OK(aws->ConfigureFromString(config_options, "aws.credentials.type=simple"));
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kSimple);
  ASSERT_EQ(creds->HasValid().ok(), has_env_creds);
  // Set one
  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.type=undefined; aws.credentials.access_key_id=access"));
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kSimple);
  ASSERT_EQ(creds->HasValid().ok(), getenv("AWS_SECRET_ACCESS_KEY") != nullptr);

  // Set both
  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.type=undefined; aws.credentials.secret_key=secret"));
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kSimple);
  ASSERT_OK(creds->HasValid());
  // Set just the other
  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.type=undefined; aws.credentials.access_key_id="));
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kSimple);
  ASSERT_EQ(creds->HasValid().ok(), getenv("AWS_ACCESS_KEY_ID") != nullptr);

  // Test config credentials
  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.config_file=file; aws.credentials.secret_key=; "
      "aws.credentials.access_key_id="));
  ASSERT_OK(creds->HasValid());
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kConfig);

  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.type=config; aws.credentials.config_file="));
  ASSERT_OK(creds->HasValid());
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kConfig);

  ASSERT_OK(aws->ConfigureFromString(
      config_options, "aws.credentials.type=undefined; aws.credentials.config_file="));
  if (has_env_creds) {
    ASSERT_OK(creds->HasValid());
    ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kEnvironment);
  } else {
    ASSERT_NOK(creds->HasValid());
    ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kUndefined);
  }

  // Now test the instance/anonymous
  ASSERT_OK(
      aws->ConfigureFromString(config_options, "aws.credentials.type=anonymous"));
  ASSERT_OK(creds->HasValid());
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kAnonymous);

  ASSERT_OK(
      aws->ConfigureFromString(config_options,  "aws.credentials.type=instance"));
  ASSERT_OK(creds->HasValid());
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kInstance);

  ASSERT_OK(aws->ConfigureFromString(config_options,  "aws.credentials.type=EC2"));
  ASSERT_OK(creds->HasValid());
  ASSERT_EQ(creds->GetAccessType(), AwsAccessType::kInstance);
  delete aws;
#endif  // ROCKSDB_DLL
}
#endif  // USE_AWS

#endif  // !ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
