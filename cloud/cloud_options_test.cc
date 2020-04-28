#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_wrapper.h"
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

class CloudOptionsTest : public testing::Test {};
#ifndef ROCKSDB_LITE
TEST_F(CloudOptionsTest, TestBucketOptions) {
  BucketOptions base, copy;
  ConfigOptions config_options;
  std::string opt_str;
  base.SetBucketName("test");
  base.SetRegion("local");
  base.SetObjectPath("my-path");
  ASSERT_OK(base.GetOptionString(config_options, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(config_options, opt_str));
  ASSERT_TRUE(copy == base);
  ASSERT_EQ(copy.GetBucketName(), "rockset.test");
  ASSERT_OK(base.ConfigureFromString(config_options, "prefix=new-prefix."));
  ASSERT_EQ(base.GetBucketName(), "new-prefix.test");
  ASSERT_FALSE(copy == base);

  ASSERT_OK(base.ConfigureFromString(config_options, "bucket=new-bucket"));
  ASSERT_EQ(base.GetBucketName(), "new-prefix.new-bucket");

  ASSERT_OK(base.GetOptionString(config_options, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(config_options, opt_str));
  ASSERT_TRUE(copy == base);

  ASSERT_OK(base.ConfigureFromString(config_options,
                                     "prefix=my-prefix.;bucket=my-bucket"));
  ASSERT_EQ(base.GetBucketName(), "my-prefix.my-bucket");
}

// DummyStorageProvider to test that Sanitize works properly
class DummyStorageProvider : public MockStorageProvider {
 private:
  bool exists_;
  bool create_;

 public:
  DummyStorageProvider(bool e = true, bool c = true) : exists_(e), create_(c) {}
  virtual const char* Name() const override { return "Dummy"; }
  virtual Status CreateBucket(const std::string& /*bucket_name*/) override {
    if (create_) {
      return Status::OK();
    } else {
      return notsup_;
    }
  }

  virtual Status ExistsBucket(const std::string& /*bucket_name*/) override {
    if (exists_) {
      return Status::OK();
    } else {
      return notsup_;
    }
  }
};

TEST_F(CloudOptionsTest, ValidateBucketOptions) {
  CloudEnvOptions opts;
  ConfigOptions config_options;
  opts.storage_provider.reset(new DummyStorageProvider());
  DBOptions db_opts;
  ColumnFamilyOptions cf_opts;
  std::unique_ptr<CloudEnv> mock(new MockCloudEnv(opts));
  db_opts.env = mock.get();

  ASSERT_OK(db_opts.env->ValidateOptions(db_opts, cf_opts));
  config_options.invoke_prepare_options = false;
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.source={bucket=test}"));
  ASSERT_NOK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Invalid src bucket
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.source={object=path}"));
  ASSERT_OK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Good source bucket
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.source={bucket=}"));
  ASSERT_NOK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Invalid src bucket
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.source={object=}"));

  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.dest={bucket=test}"));
  ASSERT_NOK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Invalid dest bucket
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.dest={object=path}"));
  ASSERT_OK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Good dest bucket
  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.dest={bucket=}"));
  ASSERT_NOK(
      db_opts.env->ValidateOptions(db_opts, cf_opts));  // Invalid dest bucket

  ASSERT_OK(db_opts.env->ConfigureFromString(config_options,
                                             "bucket.dest={object=}"));
}

class DummyLogController : public CloudLogController {
 public:
  DummyLogController(bool valid) : valid_(valid) {}

  // Create a stream to store all log files.
  Status CreateStream(const std::string&) override {
    return Status::NotSupported();
  }
  // Waits for stream to be ready (blocking).
  Status WaitForStreamReady(const std::string&) override {
    return Status::NotSupported();
  }

  // Continuously tail the cloud log stream and apply changes to
  // the local file system (blocking).
  Status TailStream() override { return Status::NotSupported(); }
  const std::string& GetCacheDir() const override { return kNullptrString; }
  Status const status() const override { return Status::NotSupported(); }

  // Creates a new cloud log writable file.
  CloudLogWritableFile* CreateWritableFile(
      const std::string& /*fname*/, const EnvOptions& /*options*/) override {
    return nullptr;
  }
  Status StartTailingStream(const std::string&) override {
    if (valid_) {
      return Status::OK();
    } else {
      return Status::NotSupported();
    }
  }

  void StopTailingStream() override {}

  Status GetFileModificationTime(const std::string& /*fname*/,
                                 uint64_t* /*time*/) override {
    return Status::NotSupported();
  }
  Status NewSequentialFile(const std::string& /*fname*/,
                           std::unique_ptr<SequentialFile>* /*result*/,
                           const EnvOptions& /*options*/) override {
    return Status::NotSupported();
  }
  Status NewRandomAccessFile(const std::string& /*fname*/,
                             std::unique_ptr<RandomAccessFile>* /*result*/,
                             const EnvOptions& /*options*/) override {
    return Status::NotSupported();
  }
  Status FileExists(const std::string& /*fname*/) override {
    return Status::NotSupported();
  }
  Status GetFileSize(const std::string& /*logical_fname*/,
                     uint64_t* /*size*/) override {
    return Status::NotSupported();
  }
  // Prepares/Initializes the log controller for the input cloud environment
  const char* Name() const override { return "Dummy"; }

 private:
  bool valid_;
};

void RegisterTestProviderOptions(ObjectLibrary& lib,
                                 const std::string& /*arg*/) {
  lib.Register<CloudStorageProvider>(
      "Dummy:true:true",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudStorageProvider>* guard, std::string*) {
        guard->reset(new DummyStorageProvider(true, true));
        return guard->get();
      });
  lib.Register<CloudStorageProvider>(
      "Dummy:true:false",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudStorageProvider>* guard, std::string*) {
        guard->reset(new DummyStorageProvider(true, false));
        return guard->get();
      });
  lib.Register<CloudStorageProvider>(
      "Dummy:false:true",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudStorageProvider>* guard, std::string*) {
        guard->reset(new DummyStorageProvider(false, true));
        return guard->get();
      });
  lib.Register<CloudStorageProvider>(
      "Dummy:false:false",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudStorageProvider>* guard, std::string*) {
        guard->reset(new DummyStorageProvider(false, false));
        return guard->get();
      });
  lib.Register<CloudLogController>(
      "Dummy:good",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudLogController>* guard, std::string*) {
        guard->reset(new DummyLogController(true));
        return guard->get();
      });
  lib.Register<CloudLogController>(
      "Dummy:bad",
      [](const std::string& /*uri*/,
         std::unique_ptr<rocksdb::CloudLogController>* guard, std::string*) {
        guard->reset(new DummyLogController(false));
        return guard->get();
      });
}
TEST_F(CloudOptionsTest, PrepareProviderOptions) {
  ConfigOptions config_options;
  ColumnFamilyOptions cf_opts;

  config_options.registry->AddLocalLibrary(RegisterTestProviderOptions,
                                           "RegisterTestProviderOptions", "");

  std::unique_ptr<Env> mock(new MockCloudEnv());
  config_options.env = mock.get();

  ASSERT_OK(mock->PrepareOptions(
      config_options));  // No buckets works without a provider

  config_options.invoke_prepare_options = false;
  ASSERT_OK(mock->ConfigureFromString(
      config_options, "bucket.source={bucket=test; object=path}"));
  ASSERT_NOK(
      mock->PrepareOptions(config_options));  // Have src but no provider.
  ASSERT_OK(
      mock->ConfigureFromString(config_options,
                                "bucket.source={bucket=; object=}; "
                                "bucket.dest={bucket=test; object=path}"));
  ASSERT_NOK(
      mock->PrepareOptions(config_options));  // Have dest but no provider.
  ASSERT_OK(
      mock->ConfigureFromString(config_options,
                                "bucket.source={bucket=; object=}; "
                                "bucket.dest={bucket=test; object=path}"));
  ASSERT_NOK(
      mock->PrepareOptions(config_options));  // Have dest but no provider.

  ASSERT_OK(mock->ConfigureFromString(
      config_options,
      "create_bucket_if_missing=true; "
      "storage_provider=Dummy:false:true; "));  // Doesn't exist but can create
  ASSERT_OK(mock->PrepareOptions(config_options));  //
  ASSERT_OK(mock->ConfigureFromString(
      config_options,
      "create_bucket_if_missing=true; "
      "storage_provider=Dummy:true:false; "));  // Exists but cannot create
  ASSERT_OK(mock->PrepareOptions(config_options));
  ASSERT_OK(mock->ConfigureFromString(
      config_options,
      "create_bucket_if_missing=true; "
      "storage_provider=Dummy:false:false; "));  // Doesn't exist and cannot
                                                 // create
  ASSERT_NOK(mock->PrepareOptions(config_options));

  ASSERT_OK(mock->ConfigureFromString(
      config_options,
      "create_bucket_if_missing=false; "
      "storage_provider=Dummy:false:false; "));  // Doesn't exist and don't
                                                 // create
  ASSERT_NOK(mock->PrepareOptions(config_options));

  ASSERT_OK(mock->ConfigureFromString(
      config_options,
      "create_bucket_if_missing=false; "
      "storage_provider=Dummy:true:false; "));  // Exists and don't create
  ASSERT_OK(mock->PrepareOptions(config_options));
}

TEST_F(CloudOptionsTest, PrepareControllerOptions) {
  CloudEnvOptions opts;
  ConfigOptions config_options;
  config_options.registry->AddLocalLibrary(RegisterTestProviderOptions,
                                           "RegisterTestProviderOptions", "");
  opts.storage_provider.reset(new DummyStorageProvider(true, true));

  std::unique_ptr<Env> mock(new MockCloudEnv(opts));
  config_options.env = mock.get();
  config_options.invoke_prepare_options = false;
  ASSERT_OK(mock->PrepareOptions(
      config_options));  // No buckets works without a controller
  ASSERT_OK(mock->ConfigureFromString(
      config_options, "bucket.source={bucket=test; object=path}"));
  ASSERT_OK(
      mock->PrepareOptions(config_options));  // Have src but no controller
  ASSERT_OK(mock->ConfigureFromString(config_options,
                                      "keep_local_log_files=false; "));
  ASSERT_NOK(
      mock->PrepareOptions(config_options));  // Don't keep and no controller
  ASSERT_OK(mock->ConfigureFromString(config_options,
                                      "keep_local_log_files=false; "
                                      "log_controller=Dummy:bad"));
  ASSERT_NOK(mock->PrepareOptions(config_options));  // Controller fails
  ASSERT_OK(mock->ConfigureFromString(config_options,
                                      "keep_local_log_files=false; "
                                      "log_controller=Dummy:good"));
  ASSERT_OK(
      mock->PrepareOptions(config_options));  // Don't keep and no controller
}

using TestCloudFactoryFunc = std::function<CloudEnv*()>;

class CloudOptionsParamTest
    : public CloudOptionsTest,
      virtual public ::testing::WithParamInterface<TestCloudFactoryFunc> {
 public:
  CloudOptionsParamTest() {
    factory_ = GetParam();
    cloud_.reset(factory_());
  }

 protected:
  TestCloudFactoryFunc factory_;
  std::unique_ptr<CloudEnv> cloud_;
};

TEST_P(CloudOptionsParamTest, GetDefaultOptionsTest) {
  ConfigOptions config_options;
  std::string opt_str;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(cloud_->ConfigureFromString(
      config_options, "keep_local_sst_files=true;keep_local_log_files=false"));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().keep_local_sst_files);
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().keep_local_log_files);
  ASSERT_OK(cloud_->ConfigureFromString(
      config_options, "keep_local_sst_files=false;keep_local_log_files=true"));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().keep_local_sst_files);
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().keep_local_log_files);
  ASSERT_OK(
      cloud_->ConfigureFromString(config_options, "validate_file_size=false"));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().validate_filesize);
  ASSERT_OK(cloud_->ConfigureFromString(config_options,
                                        "purger_periodicity_millis=1234"));
  ASSERT_EQ(cloud_->GetCloudEnvOptions().purger_periodicity_millis, 1234);
  ASSERT_OK(
      cloud_->ConfigureFromString(config_options, "request_timeout_ms=5678"));
  ASSERT_EQ(cloud_->GetCloudEnvOptions().request_timeout_ms, 5678);
  ASSERT_OK(cloud_->ConfigureFromString(config_options,
                                        "create_bucket_if_missing=false"));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().create_bucket_if_missing);
  ASSERT_OK(cloud_->ConfigureFromString(config_options, "run_purger=true"));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().run_purger);
  ASSERT_OK(cloud_->ConfigureFromString(config_options,
                                        "ephemeral_resync_on_open=true"));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().ephemeral_resync_on_open);
  ASSERT_OK(cloud_->GetOptionString(config_options, &opt_str));

  std::unique_ptr<CloudEnv> copy(factory_());
  ASSERT_FALSE(copy->Matches(config_options, cloud_.get()));
  ASSERT_OK(copy->ConfigureFromString(config_options, opt_str));
  ASSERT_TRUE(copy->Matches(config_options, cloud_.get()));
}

TEST_P(CloudOptionsParamTest, TestCloudBucketOptions) {
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(cloud_->ConfigureFromString(
      config_options,
      "bucket.source={prefix=my-prefix.;bucket=src-bucket;object=src-object;"
      "region=my-region}; "));
  ASSERT_EQ(cloud_->GetSrcBucketName(), "my-prefix.src-bucket");
  ASSERT_TRUE(cloud_->HasSrcBucket());
  ASSERT_FALSE(cloud_->HasDestBucket());
  ASSERT_EQ(cloud_->GetSrcObjectPath(), "src-object");
  ASSERT_OK(
      cloud_->ConfigureFromString(config_options,
                                  "bucket.dest={bucket=dest-bucket;object=dest-"
                                  "object;region=dest-region}"));
  ASSERT_TRUE(cloud_->HasDestBucket());
  ASSERT_EQ(cloud_->GetDestBucketName(), "rockset.dest-bucket");
  ASSERT_EQ(cloud_->GetDestObjectPath(), "dest-object");
  ASSERT_FALSE(cloud_->SrcMatchesDest());
  ASSERT_OK(cloud_->ConfigureFromString(
      config_options,
      "bucket.source={prefix=my-prefix.;bucket=;object=;region=my-region}; "));
  ASSERT_FALSE(cloud_->HasSrcBucket());
  ASSERT_FALSE(cloud_->SrcMatchesDest());
  ASSERT_OK(cloud_->ConfigureFromString(
      config_options,
      "bucket.source={prefix=rockset.;bucket=dest-bucket;object=dest-object;"
      "region=dest-region}; "));
  ASSERT_EQ(cloud_->GetSrcBucketName(), cloud_->GetDestBucketName());
  ASSERT_TRUE(cloud_->HasSrcBucket());
  ASSERT_TRUE(cloud_->SrcMatchesDest());
}

INSTANTIATE_TEST_CASE_P(ParamTest, CloudOptionsParamTest,
                        testing::Values([] { return new MockCloudEnv(); }
#ifdef USE_AWS
                                        ,
                                        [] {
                                          return new AwsEnv(Env::Default(),
                                                            CloudEnvOptions(),
                                                            nullptr);
                                        }
#endif
                                        ));

#endif  // !ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
