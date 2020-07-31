//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#include <memory>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class Logger;

class BucketOptions {
 private:
  std::string bucket_;  // The suffix for the bucket name
  std::string
      prefix_;  // The prefix for the bucket name.  Defaults to "rockset."
  std::string object_;  // The object path for the bucket
  std::string region_;  // The region for the bucket
  std::string name_;    // The name of the bucket (prefix_ + bucket_)
 public:
  BucketOptions();
  // Sets the name of the bucket to be the new bucket name.
  // If prefix is specified, the new bucket name will be [prefix][bucket]
  // If no prefix is specified, the bucket name will use the existing prefix
  void SetBucketName(const std::string& bucket, const std::string& prefix = "");
  const std::string& GetBucketName() const { return name_; }
  const std::string& GetObjectPath() const { return object_; }
  void SetObjectPath(const std::string& object) { object_ = object; }
  const std::string& GetRegion() const { return region_; }
  void SetRegion(const std::string& region) { region_ = region; }

  // Initializes the bucket properties for test purposes
  void TEST_Initialize(const std::string& name_prefix,
                       const std::string& object_path,
                       const std::string& region = "");
  bool IsValid() const {
    if (object_.empty() || name_.empty()) {
      return false;
    } else {
      return true;
    }
  }
};

inline bool operator==(const BucketOptions& lhs, const BucketOptions& rhs) {
  if (lhs.IsValid() && rhs.IsValid()) {
    return ((lhs.GetBucketName() == rhs.GetBucketName()) &&
            (lhs.GetObjectPath() == rhs.GetObjectPath()) &&
            (lhs.GetRegion() == rhs.GetRegion()));
  } else {
    return false;
  }
}
inline bool operator!=(const BucketOptions& lhs, const BucketOptions& rhs) {
  return !(lhs == rhs);
}

struct CloudOptions {
  CloudOptions(const std::shared_ptr<Logger>& logger = nullptr)
    : info_log(logger) {
  }
  std::shared_ptr<Logger> info_log;  // informational messages
};

} // namespace ROCKSDB_NAMESPACE
