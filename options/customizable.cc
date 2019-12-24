// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/customizable.h"

#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

namespace rocksdb {

std::string Customizable::GetOptionName(const std::string& long_name) const {
  const std::string& name = Name();
  size_t name_len = name.size();
  if (long_name.size() > name_len + 1 &&
      long_name.compare(0, name_len, name) == 0 &&
      long_name.at(name_len) == '.') {
    return long_name.substr(name_len + 1);
  } else {
    return Configurable::GetOptionName(long_name);
  }
}

Status Customizable::GetOneOption(const std::string& opt_name,
                                  const ConfigOptions& options,
                                  std::string* value) const {
  if (opt_name == kIdPropName) {
    *value = GetId();
    return Status::OK();
  } else {
    return Configurable::GetOneOption(opt_name, options, value);
  }
}

Status Customizable::ListAllOptions(
    const std::string& prefix, const ConfigOptions& options,
    std::unordered_set<std::string>* result) const {
  result->emplace(prefix + kIdPropName);
  return Configurable::ListAllOptions(prefix, options, result);
}

bool Customizable::MatchesOption(const Configurable* other,
                                 const ConfigOptions& opts,
                                 std::string* name) const {
  if (opts.sanity_level > OptionsSanityCheckLevel::kSanityLevelNone &&
      this != other) {
    const Customizable* custom = reinterpret_cast<const Customizable*>(other);
    if (GetId() != custom->GetId()) {
      *name = kIdPropName;
      return false;
    } else if (opts.sanity_level > kSanityLevelLooselyCompatible) {
      bool matches = Configurable::MatchesOption(other, opts, name);
      return matches;
    }
  }
  return true;
}

#ifndef ROCKSDB_LITE
std::string Customizable::AsString(const std::string& prefix,
                                   const ConfigOptions& options) const {
  std::string result;
  std::string parent;
  if (!options.IsShallow()) {
    if (options.IsDetached()) {
      parent = Configurable::AsString(prefix, options);
    } else {
      parent = Configurable::AsString("", options);
    }
  }
  if (parent.empty()) {
    result = GetId();
  } else {
    PrintSingleOption(prefix, kIdPropName, GetId(), options.delimiter, &result);
    result.append(parent);
  }
  return result;
}
#endif  // ROCKSDB_LITE

Status Customizable::GetOptionsMap(
    const std::string& value, std::string* id,
    std::unordered_map<std::string, std::string>* props) {
  assert(id);
  assert(props);
  Status status;
  if (value.empty() || value == kNullptrString) {
    id->clear();
  } else if (value.find('=') == std::string::npos) {
    *id = value;
#ifndef ROCKSDB_LITE
  } else {
    status = StringToMap(value, props);
    if (status.ok()) {
      auto iter = props->find(kIdPropName);
      if (iter != props->end()) {
        *id = iter->second;
        props->erase(iter);
      } else {  // Should this be an error??
        status = Status::InvalidArgument("Name property is missing");
      }
    }
#else
  } else {
    *id = value;
    props->clear();
#endif
  }
  return status;
}

}  // namespace rocksdb
