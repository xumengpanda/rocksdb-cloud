//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <functional>
#include <memory>
#include <unordered_map>

#include "rocksdb/cloud/cloud_options.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CloudEnv;
class CloudLogController;
class CloudStorageProvider;

//
// The cloud environment for rocksdb. It allows configuring the rocksdb
// Environent used for the cloud.
//
struct CloudEnvOptions : public CloudOptions {
  BucketOptions src_bucket;
  BucketOptions dest_bucket;

  //
  // If true,  then sst files are stored locally and uploaded to the cloud in
  // the background. On restart, all files from the cloud that are not present
  // locally are downloaded.
  // If false, then local sst files are created, uploaded to cloud immediately,
  //           and local file is deleted. All reads are satisfied by fetching
  //           data from the cloud.
  // Default:  false
  bool keep_local_sst_files;

  // If true,  then .log and MANIFEST files are stored in a local file system.
  //           they are not uploaded to any cloud logging system.
  // If false, then .log and MANIFEST files are not stored locally, and are
  //           stored in a cloud-logging system like Kinesis or Kafka.
  // Default:  true
  bool keep_local_log_files;

  // This feature is obsolete. We upload MANIFEST to the cloud on every write.
  // uint64_t manifest_durable_periodicity_millis;

  // The time period when the purger checks and deleted obselete files.
  // This is the time when the purger wakes up, scans the cloud bucket
  // for files that are not part of any DB and then deletes them.
  // Default: 10 minutes
  uint64_t purger_periodicity_millis;

  // Validate that locally cached files have the same size as those
  // stored in the cloud.
  // Default: true
  bool validate_filesize;


  // If false, it will not attempt to create cloud bucket if it doesn't exist.
  // Default: true
  bool create_bucket_if_missing;

  // Use this to turn off the purger. You can do this if you don't use the clone
  // feature of RocksDB cloud
  // Default: true
  bool run_purger;

  // An ephemeral clone is a clone that has no destination bucket path. All
  // updates to this clone are stored locally and not uploaded to cloud.
  // It is called ephemeral because locally made updates can get lost if
  // the machines dies.
  // This flag controls whether the ephemeral db needs to be resynced to
  // the source cloud bucket at every db open time.
  // If true,  then the local ephemeral db is re-synced to the src cloud
  //           bucket every time the db is opened. Any previous writes
  //           to this ephemeral db are lost.
  // If false, then the local ephemeral db is initialized from data in the
  //           src cloud bucket only if the local copy does not exist.
  //           If the local copy of the db already exists, then no data
  //           from the src cloud bucket is copied to the local db dir.
  // Default:  false
  bool ephemeral_resync_on_open;

  // If true, we will skip the dbid verification on startup. This is currently
  // only used in tests and is not recommended setting.
  // Default: false
  bool skip_dbid_verification;


  // The number of object's metadata that are fetched in every iteration when
  // listing the results of a directory Default: 5000
  int number_objects_listed_in_one_iteration;

  // During opening, we get the size of all SST files currently in the
  // folder/bucket for bookkeeping. This operation might be expensive,
  // especially if the bucket is in the cloud. This option allows to use a
  // constant size instead. Non-negative value means use this option.
  //
  // NOTE: If users already passes an SST File Manager through
  // Options.sst_file_manager, constant_sst_file_size_in_sst_file_manager is
  // ignored.
  //
  // Default: -1, means don't use this option.
  int64_t constant_sst_file_size_in_sst_file_manager;

  // Skip listing files in the cloud in GetChildren. That means GetChildren
  // will only return files in local directory. During DB opening, RocksDB
  // makes multiple GetChildren calls, which are very expensive if we list
  // objects in the cloud.
  //
  // This option is used in remote compaction where we open the DB in a
  // temporary folder, and then the folder is deleted after the RPC is done.
  // This requires opening DB to be really fast, and it's unnecessary to cleanup
  // various things, which is what RocksDB calls GetChildren for.
  //
  // Default: false.
  bool skip_cloud_files_in_getchildren;

  CloudEnvOptions(
      bool _keep_local_sst_files = false, bool _keep_local_log_files = true,
      uint64_t _purger_periodicity_millis = 10 * 60 * 1000,
      bool _validate_filesize = true,
      bool _create_bucket_if_missing = true, 
      bool _run_purger = false, bool _ephemeral_resync_on_open = false,
      bool _skip_dbid_verification = false,
      int _number_objects_listed_in_one_iteration = 5000,
      int _constant_sst_file_size_in_sst_file_manager = -1,
      bool _skip_cloud_files_in_getchildren = false)
      : keep_local_sst_files(_keep_local_sst_files),
        keep_local_log_files(_keep_local_log_files),
        purger_periodicity_millis(_purger_periodicity_millis),
        validate_filesize(_validate_filesize),
        create_bucket_if_missing(_create_bucket_if_missing),
        run_purger(_run_purger),
        ephemeral_resync_on_open(_ephemeral_resync_on_open),
        skip_dbid_verification(_skip_dbid_verification),
        number_objects_listed_in_one_iteration(
            _number_objects_listed_in_one_iteration),
        constant_sst_file_size_in_sst_file_manager(
            _constant_sst_file_size_in_sst_file_manager),
        skip_cloud_files_in_getchildren(_skip_cloud_files_in_getchildren) {}

  // Sets result based on the value of name or alt in the environment
  // Returns true if the name/alt exists in the environment, false otherwise
  static bool GetNameFromEnvironment(const char* name, const char* alt,
                                     std::string* result);
  void TEST_Initialize(const std::string& name_prefix,
                       const std::string& object_path,
                       const std::string& region = "");
};

struct CheckpointToCloudOptions {
  int thread_count = 8;
  bool flush_memtable = false;
};

// A map of dbid to the pathname where the db is stored
typedef std::map<std::string, std::string> DbidList;

//
// The Cloud environment
//
class CloudEnv : public Env {
 protected:
  Env* base_env_;  // The underlying env
  CloudEnvOptions cloud_env_options;

  // Specifies the class responsible for writing objects to the cloud
  std::unique_ptr<CloudStorageProvider> provider_;

  // If keep_local_log_files is false, this specifies what service to use
  // for storage of write-ahead log.
  std::unique_ptr<CloudLogController> controller_;

  CloudEnv(Env* base, const CloudEnvOptions& options,
           std::unique_ptr<CloudStorageProvider> provider = nullptr,
           std::unique_ptr<CloudLogController>  controller = nullptr);
  virtual const Env* FindInstance(const std::string& name) const;

 public:
  static const std::string kAwsCloudName /* = "aws" */;
  static const std::string kEnvCloudName /* = "cloud" */;
  virtual ~CloudEnv();

  static Status CreateCloudEnv(const std::string& name, Env* base_env,
                               const CloudEnvOptions& options,
                               std::unique_ptr<CloudEnv>* result);
  static Status CreateCloudEnv(const std::string& name, Env* base_env,
                               const CloudEnvOptions& options,
                               std::unique_ptr<CloudStorageProvider> provider,
                               std::unique_ptr<CloudLogController> controller,
                               std::unique_ptr<CloudEnv>* result);
  // Returns the underlying env
  Env* GetBaseEnv() {
    return base_env_; 
  }

  // print out all options to the log
  void Dump(Logger* log) const;

  virtual const char *Name() const;
  
  std::shared_ptr<Logger> & GetInfoLogger() {
    return cloud_env_options.info_log;
  }
  
  virtual Status PreloadCloudManifest(const std::string& local_dbname) = 0;

  // Reads a file from the cloud
  virtual Status NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        std::unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) = 0;

  // Saves and retrieves the dbid->dirname mapping in cloud storage
  virtual Status SaveDbid(const std::string& bucket_name,
                          const std::string& dbid,
                          const std::string& dirname) = 0;
  virtual Status GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid,
                                std::string* dirname) = 0;
  virtual Status GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) = 0;
  virtual Status DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) = 0;

  virtual CloudStorageProvider* GetStorageProvider() const {
    return provider_.get();
  }
  virtual const CloudLogController* GetLogController() const {
    return controller_.get();
  }
  
  // The SrcBucketName identifies the cloud storage bucket and
  // GetSrcObjectPath specifies the path inside that bucket
  // where data files reside. The specified bucket is used in
  // a readonly mode by the associated DBCloud instance.
  const std::string& GetSrcBucketName() const {
    return cloud_env_options.src_bucket.GetBucketName();
  }
  const std::string& GetSrcObjectPath() const {
    return cloud_env_options.src_bucket.GetObjectPath();
  }
  bool HasSrcBucket() const { return cloud_env_options.src_bucket.IsValid(); }

  // The DestBucketName identifies the cloud storage bucket and
  // GetDestObjectPath specifies the path inside that bucket
  // where data files reside. The associated DBCloud instance
  // writes newly created files to this bucket.
  const std::string& GetDestBucketName() const {
    return cloud_env_options.dest_bucket.GetBucketName();
  }
  const std::string& GetDestObjectPath() const {
    return cloud_env_options.dest_bucket.GetObjectPath();
  }

  bool HasDestBucket() const { return cloud_env_options.dest_bucket.IsValid(); }
  bool SrcMatchesDest() const {
    if (HasSrcBucket() && HasDestBucket()) {
      return cloud_env_options.src_bucket == cloud_env_options.dest_bucket;
    } else {
      return false;
    }
  }

  // returns the options used to create this env
  const CloudEnvOptions& GetCloudEnvOptions() const {
    return cloud_env_options;
  }

  // Deletes file from a destination bucket.
  virtual Status DeleteCloudFileFromDest(const std::string& fname) = 0;
  // Copies a local file to a destination bucket.
  virtual Status CopyLocalFileToDest(const std::string& local_name,
                                     const std::string& cloud_name) = 0;

  // Transfers the filename from RocksDB's domain to the physical domain, based
  // on information stored in CLOUDMANIFEST.
  // For example, it will map 00010.sst to 00010.sst-[epoch] where [epoch] is
  // an epoch during which that file was created.
  // Files both in S3 and in the local directory have this [epoch] suffix.
  virtual std::string RemapFilename(const std::string& logical_name) const = 0;

  template <typename T>
  const T* CastAs(const std::string& name) const {
    const auto c = FindInstance(name);
    return static_cast<const T*>(c);
  }

  template <typename T>
  T* CastAs(const std::string& name) {
    auto c = const_cast<Env*>(FindInstance(name));
    return static_cast<T*>(c);
  }
};

}  // namespace ROCKSDB_NAMESPACE
