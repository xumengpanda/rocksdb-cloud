#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_wrapper.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
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
  ConfigOptions cfg;
  std::string opt_str;
  base.SetBucketName("test");
  base.SetRegion("local");
  base.SetObjectPath("my-path");
  ASSERT_OK(base.GetOptionString(cfg, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(opt_str, cfg));
  ASSERT_TRUE(copy.Matches(&base, cfg));
  ASSERT_EQ(copy.GetBucketName(), "rockset.test");
  ASSERT_OK(base.ConfigureFromString("prefix=new-prefix.", cfg));
  ASSERT_EQ(base.GetBucketName(), "new-prefix.test");
  ASSERT_FALSE(copy.Matches(&base, cfg));
  
  ASSERT_OK(base.ConfigureFromString("bucket=new-bucket", cfg));
  ASSERT_EQ(base.GetBucketName(), "new-prefix.new-bucket");

  ASSERT_OK(base.GetOptionString(cfg, &opt_str));
  ASSERT_OK(copy.ConfigureFromString(opt_str, cfg));
  ASSERT_TRUE(copy.Matches(&base, cfg));

  ASSERT_OK(base.ConfigureFromString("prefix=my-prefix.;bucket=my-bucket", cfg));
  ASSERT_EQ(base.GetBucketName(), "my-prefix.my-bucket");
}

// DummyStorageProvider to test that Sanitize works properly
class DummyStorageProvider : public MockStorageProvider {
private:
  bool exists_;
  bool create_;
public:
  DummyStorageProvider(bool e = true, bool c = true)
    : exists_(e),  create_(c) { }
  virtual const char *Name() const override { return "Dummy"; }
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

TEST_F(CloudOptionsTest, SanitizeBucketOptions) {
  CloudEnvOptions opts;
  ConfigOptions cfg;
  opts.storage_provider.reset(new DummyStorageProvider());
  DBOptions db_opts;
  db_opts.env = new MockCloudEnv(opts);
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts));
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.source={bucket=test}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Invalid src bucket 
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.source={object=path}", cfg));
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // Good source bucket
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.source={bucket=}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Invalid src bucket 
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.source={object=}", cfg));

  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.dest={bucket=test}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Invalid dest bucket 
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.dest={object=path}", cfg));
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // Good dest bucket
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.dest={bucket=}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Invalid dest bucket 
  ASSERT_OK(db_opts.env->ConfigureFromString("bucket.dest={object=}", cfg));

  delete db_opts.env;
}
  
TEST_F(CloudOptionsTest, SanitizeProviderOptions) {
  CloudEnvOptions opts;
  DBOptions db_opts;
  ConfigOptions cfg(db_opts);
  std::shared_ptr<ObjectLibrary> lib = cfg.registry->AddLocalLibrary("test");
  lib->Register<CloudStorageProvider>(
      "Dummy:true:true",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudStorageProvider>* guard,
         std::string*) {
        guard->reset(new DummyStorageProvider(true, true));
        return guard->get();
      });
  lib->Register<CloudStorageProvider>(
      "Dummy:true:false",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudStorageProvider>* guard,
         std::string*) {
        guard->reset(new DummyStorageProvider(true, false));
        return guard->get();
      });
  lib->Register<CloudStorageProvider>(
      "Dummy:false:true",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudStorageProvider>* guard,
         std::string*) {
        guard->reset(new DummyStorageProvider(false, true));
        return guard->get();
      });
  lib->Register<CloudStorageProvider>(
      "Dummy:false:false",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudStorageProvider>* guard,
         std::string*) {
        guard->reset(new DummyStorageProvider(false, false));
        return guard->get();
      });

  db_opts.env = new MockCloudEnv(opts);
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // No buckets works without a provider
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "bucket.source={bucket=test; object=path}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Have src but no provider.
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "bucket.source={bucket=; object=}; "
         "bucket.dest={bucket=test; object=path}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Have dest but no provider.
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "bucket.source={bucket=; object=}; "
         "bucket.dest={bucket=test; object=path}", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Have dest but no provider.
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "create_bucket_if_missing=true; " 
         "storage_provider=Dummy:false:true; ", cfg)); // Doesn't exist but can create
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // 
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "create_bucket_if_missing=true; " 
         "storage_provider=Dummy:true:false; ", cfg)); // Exists but cannot create
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); 
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "create_bucket_if_missing=true; " 
         "storage_provider=Dummy:false:false; ", cfg)); // Doesn't exist and cannot create
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts));
  
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "create_bucket_if_missing=false; " 
         "storage_provider=Dummy:false:false; ", cfg)); // Doesn't exist and don't create
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); 

  ASSERT_OK(db_opts.env->ConfigureFromString(
         "create_bucket_if_missing=false; " 
         "storage_provider=Dummy:true:false; ", cfg)); // Exists and don't create
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); 

  delete db_opts.env;
}
  
class DummyLogController : public CloudLogController {
 public:
  DummyLogController(bool valid) : valid_(valid) {
  }

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
  Status TailStream() override {
    return Status::NotSupported();
  }

  Status StartTailingStream(const std::string&) override {
    if (valid_) {
      return Status::OK();
    } else {
      return Status::NotSupported();
    }
  }
  
  void StopTailingStream() override {
  }
    
  // Creates a new cloud log writable file.
  CloudLogWritableFile* CreateWritableFile(const std::string&,
                                           const EnvOptions&) override {
    return nullptr;
  }

  const char* Name() const override { return "Dummy"; }
private:
  bool valid_;
  };
  
TEST_F(CloudOptionsTest, SanitizeControllerOptions) {
  CloudEnvOptions opts;
  DBOptions db_opts;
  ConfigOptions cfg(db_opts);
  std::shared_ptr<ObjectLibrary> lib = cfg.registry->AddLocalLibrary("test");
  lib->Register<CloudLogController>(
      "Dummy:good",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudLogController>* guard,
         std::string*) {
        guard->reset(new DummyLogController(true));
        return guard->get();
      });
  lib->Register<CloudLogController>(
      "Dummy:bad",
      [](const std::string& /*uri*/, std::unique_ptr<rocksdb::CloudLogController>* guard,
         std::string*) {
        guard->reset(new DummyLogController(false));
        return guard->get();
      });
  opts.storage_provider.reset(new DummyStorageProvider(true, true));
  db_opts.env = new MockCloudEnv(opts);

  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // No buckets works without a controller
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "bucket.source={bucket=test; object=path}", cfg));
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // Have src but no controller
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "keep_local_log_files=false; ", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Don't keep and no controller
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "keep_local_log_files=false; "
         "log_controller=Dummy:bad", cfg));
  ASSERT_NOK(db_opts.env->SanitizeOptions(db_opts)); // Controller fails
  ASSERT_OK(db_opts.env->ConfigureFromString(
         "keep_local_log_files=false; "
         "log_controller=Dummy:good", cfg));
  ASSERT_OK(db_opts.env->SanitizeOptions(db_opts)); // Don't keep and no controller
    
  delete db_opts.env;
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
  ConfigOptions cfg;
  std::string opt_str;
  ASSERT_OK(cloud_->ConfigureFromString("keep_local_sst_files=true;keep_local_log_files=false", cfg));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().keep_local_sst_files);
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().keep_local_log_files);
  ASSERT_OK(cloud_->ConfigureFromString("keep_local_sst_files=false;keep_local_log_files=true", cfg));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().keep_local_sst_files);
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().keep_local_log_files);
  ASSERT_OK(cloud_->ConfigureFromString("validate_file_size=false", cfg));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().validate_filesize);
  ASSERT_OK(cloud_->ConfigureFromString("purger_periodicity_millis=1234", cfg));
  ASSERT_EQ(cloud_->GetCloudEnvOptions().purger_periodicity_millis, 1234);
  ASSERT_OK(cloud_->ConfigureFromString("request_timeout_ms=5678", cfg));
  ASSERT_EQ(cloud_->GetCloudEnvOptions().request_timeout_ms, 5678);
  ASSERT_OK(cloud_->ConfigureFromString("create_bucket_if_missing=false", cfg));
  ASSERT_FALSE(cloud_->GetCloudEnvOptions().create_bucket_if_missing);
  ASSERT_OK(cloud_->ConfigureFromString("run_purger=true", cfg));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().run_purger);
  ASSERT_OK(cloud_->ConfigureFromString("ephemeral_resync_on_open=true", cfg));
  ASSERT_TRUE(cloud_->GetCloudEnvOptions().ephemeral_resync_on_open);
  ASSERT_OK(cloud_->GetOptionString(cfg, &opt_str));

  std::unique_ptr<CloudEnv> copy(factory_());
  ASSERT_FALSE(copy->Matches(cloud_.get(), cfg));
  ASSERT_OK(copy->ConfigureFromString(opt_str, cfg));
  ASSERT_TRUE(copy->Matches(cloud_.get(), cfg));
}
  
TEST_P(CloudOptionsParamTest,TestCloudBucketOptions) {
  ConfigOptions cfg;
  ASSERT_OK(cloud_->ConfigureFromString(
        "bucket.source={prefix=my-prefix.;bucket=src-bucket;object=src-object;region=my-region}; ", cfg));
  ASSERT_EQ(cloud_->GetSrcBucketName(), "my-prefix.src-bucket");
  ASSERT_TRUE(cloud_->HasSrcBucket());
  ASSERT_FALSE(cloud_->HasDestBucket());
  ASSERT_EQ(cloud_->GetSrcObjectPath(), "src-object");
  ASSERT_OK(cloud_->ConfigureFromString(
        "bucket.dest={bucket=dest-bucket;object=dest-object;region=dest-region}",
        cfg));
  ASSERT_TRUE(cloud_->HasDestBucket());
  ASSERT_EQ(cloud_->GetDestBucketName(), "rockset.dest-bucket");
  ASSERT_EQ(cloud_->GetDestObjectPath(), "dest-object");
  ASSERT_FALSE(cloud_->SrcMatchesDest());
  ASSERT_OK(cloud_->ConfigureFromString(
        "bucket.source={prefix=my-prefix.;bucket=;object=;region=my-region}; ", cfg));
  ASSERT_FALSE(cloud_->HasSrcBucket());
  ASSERT_FALSE(cloud_->SrcMatchesDest());
  ASSERT_OK(cloud_->ConfigureFromString(
        "bucket.source={prefix=rockset.;bucket=dest-bucket;object=dest-object;region=dest-region}; ", cfg));
  ASSERT_EQ(cloud_->GetSrcBucketName(), cloud_->GetDestBucketName());
  ASSERT_TRUE(cloud_->HasSrcBucket());
  ASSERT_TRUE(cloud_->SrcMatchesDest());
}
  

INSTANTIATE_TEST_CASE_P(
    ParamTest, CloudOptionsParamTest,
    testing::Values([]{return new MockCloudEnv(); }
#ifdef USE_AWS
                    , []{return new AwsEnv(Env::Default(), CloudEnvOptions(), nullptr);}
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
  
