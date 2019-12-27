// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/configurable.h"
#include "rocksdb/status.h"

namespace rocksdb {
/**
 * Customizable a base class used by the rocksdb that describes a
 * standard way of configuring and creating objects.  Customizable objects
 * are configurable objects that can be created from an ObjectRegistry.
 *
 * When a Customizable is being created, the "name" property specifies
 * the name of the instance being created.
 * For custom objects, their configuration and name can be specified by:
 * [prop]={name=X;option 1 = value1[; option2=value2...]}
 *
 * [prop].name=X
 * [prop].option1 = value1
 *
 * [prop].name=X
 * X.option1 =value1
 */
class Customizable : public Configurable {
 public:
  virtual ~Customizable() {}
  // Returns the name of this class of Customizable
  virtual const char* Name() const = 0;
  // Returns an identifier for this Customizable.
  // This could be its name or something more complex (like its URL/pattern).
  // Used for pretty printing.
  virtual std::string GetId() const {
    std::string id = Name();
    return id;
  }

 protected:
  std::string GetOptionName(const std::string& long_name) const override;
  bool MatchesOption(const Configurable* other, const ConfigOptions& options,
                     std::string* name) const override;
#ifndef ROCKSDB_LITE
  std::string AsString(const std::string& prefix,
                       const ConfigOptions& options) const override;
  Status GetOneOption(const std::string& opt_name, const ConfigOptions& options,
                      std::string* value) const override;
  Status ListAllOptions(const std::string& prefix, const ConfigOptions& options,
                        std::unordered_set<std::string>* result) const override;
#endif  // ROCKSDB_LITE
 public:
  static Status GetOptionsMap(
      const std::string& opt_value, std::string* id,
      std::unordered_map<std::string, std::string>* options);
};

}  // namespace rocksdb
