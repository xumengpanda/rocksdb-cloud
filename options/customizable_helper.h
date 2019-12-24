// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <functional>
#include <memory>
#include <unordered_map>

#include "options/options_helper.h"
#include "rocksdb/customizable.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"

namespace rocksdb {
template <typename T>
using SharedFactoryFunc =
    std::function<bool(const std::string&, std::shared_ptr<T>*)>;

template <typename T>
using UniqueFactoryFunc =
    std::function<bool(const std::string&, std::unique_ptr<T>*)>;

template <typename T>
using StaticFactoryFunc = std::function<bool(const std::string&, T**)>;

template <typename T>
static Status LoadSharedObject(const std::string& value,
                               const SharedFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts,
                               std::shared_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (result->get() != nullptr && id == result->get()->GetId()) {
    // We have a valid object and it matches the expected one.  Configure it
    status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
  } else if (func != nullptr && func(id, result)) {  // Factory method worked
    status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
  } else if (!id.empty()) {  // Load the ID from the registry and configure it
#ifndef ROCKSDB_LITE
    status = cfg_opts.registry->NewSharedObject(id, result);
    if (status.ok()) {
      status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
    } else if (cfg_opts.ignore_unknown_objects) {
      status = Status::OK();
    }
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif                           // ROCKSDB_LITE
  } else if (opt_map.empty()) {  // No Id and no options.  Clear the object
    result->reset();
  } else {  // We have no Id but have options.  Not good
    status = Status::NotSupported("Cannot reset object ", id);
  }
  return status;
}

template <typename T>
static Status LoadUniqueObject(const std::string& value,
                               const UniqueFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts,
                               std::unique_ptr<T>* result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (result->get() != nullptr && id == result->get()->GetId()) {
    // We have a valid object and it matches the expected one.  Configure it
    status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
  } else if (func != nullptr && func(id, result)) {  // Factory method worked
    status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
  } else if (!id.empty()) {  // Load the ID from the registry and configure it
#ifndef ROCKSDB_LITE
    status = cfg_opts.registry->NewUniqueObject(id, result);
    if (status.ok()) {
      status = result->get()->ConfigureFromMap(opt_map, cfg_opts);
    } else if (cfg_opts.ignore_unknown_objects) {
      status = Status::OK();
    }
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif
  } else if (opt_map.empty()) {  // No Id and no options.  Clear the object
    result->reset();
  } else {  // We have no Id but have options.  Not good
    status = Status::NotSupported("Cannot reset object ", id);
  }
  return status;
}

template <typename T>
static Status LoadStaticObject(const std::string& value,
                               const StaticFactoryFunc<T>& func,
                               const ConfigOptions& cfg_opts, T** result) {
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(value, &id, &opt_map);
  if (!status.ok()) {
  } else if (*result != nullptr && id == (*result)->GetId()) {
    status = (*result)->ConfigureFromMap(opt_map, cfg_opts);
  } else if (func != nullptr && func(id, result)) {
    status = (*result)->ConfigureFromMap(opt_map, cfg_opts);
  } else if (!id.empty()) {
#ifndef ROCKSDB_LITE
    status = cfg_opts.registry->NewStaticObject(id, result);
    if (status.ok()) {
      status = (*result)->ConfigureFromMap(opt_map, cfg_opts);
    } else if (cfg_opts.ignore_unknown_objects) {
      status = Status::OK();
    }
#else
    status = Status::NotSupported("Cannot load object in LITE mode ", id);
#endif
  } else if (opt_map.empty()) {
    *result = nullptr;
  } else {
    status = Status::NotSupported("Cannot reset object ", id);
  }
  return status;
}

}  // namespace rocksdb
