// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <functional>
#include <memory>

#include "rocksdb/status.h"

namespace rocksdb {
class OptionTypeinfo;
struct ConfigOptions;

enum class OptionType {
  kBoolean,
  kInt,
  kInt32T,
  kInt64T,
  kVectorInt,
  kUInt,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
  kCompactionPri,
  kSliceTransform,
  kCompressionType,
  kCompressionOpts,
  kVectorCompressionType,
  kCompactionStopStyle,
  kChecksumType,
  kEncodingType,
  kEnv,
  kEnum,
  kStruct,
  kConfigurable,
  kCustomizable,
  kUnknown,
};

enum class OptionVerificationType {
  kNormal,
  kByName,               // The option is pointer typed so we can only verify
                         // based on it's name.
  kByNameAllowNull,      // Same as kByName, but it also allows the case
                         // where one of them is a nullptr.
  kByNameAllowFromNull,  // Same as kByName, but it also allows the case
                         // where the old option is nullptr.
  kDeprecated,           // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
  kAlias                 // This option represents is a name/shortcut for
                         // another option and should not be written or verified
                         // independently
};

enum OptionTypeFlags {
  kNone = 0x00,           // No flags
  kMutable = 0x01,        // Option is mutable
  kPointer = 0x02,        // The option is stored as a pointer
  kShared = 0x04,         // The option is stored as a shared_ptr
  kUnique = 0x08,         // The option is stored as a unique_ptr
  kEnum = 0x100,          // The option is an enum
  kStruct = 0200,         // The option is a struct
  kConfigurable = 0x400,  // The option is a ConfigurableObject
  kCustomizable = 0xC00,  // The option is a CustomizableObject
  kMStruct = kStruct | kMutable,
  kMEnum = kEnum | kMutable,
  kMConfigurable = kConfigurable | kMutable,
  kConfigurableP = kConfigurable | kPointer,
  kConfigurableS = kConfigurable | kShared,
  kConfigurableU = kConfigurable | kUnique,
  kMConfigurableP = kMConfigurable | kPointer,
  kMConfigurableS = kMConfigurable | kShared,
  kMConfigurableU = kMConfigurable | kUnique,

  kMCustomizable = kCustomizable | kMutable,
  kCustomizableP = kCustomizable | kPointer,
  kCustomizableS = kCustomizable | kShared,
  kCustomizableU = kCustomizable | kUnique,
  kMCustomizableP = kMCustomizable | kPointer,
  kMCustomizableS = kMCustomizable | kShared,
  kMCustomizableU = kMCustomizable | kUnique,
};

template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

using ParserFunc = std::function<Status(
    const std::string& /*name*/, const std::string& /*value*/,
    const ConfigOptions& /*opts*/, char* /*addr*/)>;
using StringFunc = std::function<Status(
    const std::string& /*name*/, const char* /*address*/,
    const ConfigOptions& /*opts*/, std::string* /*value*/)>;
using EqualsFunc =
    std::function<bool(const std::string& /*name*/, const char* /*address1*/,
                       const char* /*address2*/, const ConfigOptions& /*opts*/,
                       std::string* mismatch)>;

// A struct for storing constant option information such as option name,
// option type, and offset.
class OptionTypeInfo {
 public:
  // A simple "normal", non-mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type)
      : offset(_offset),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kNone),
        mutable_offset(0),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr) {}

  // A simple "normal", mutable Type "_type" at _offset
  OptionTypeInfo(int _offset, OptionType _type, int _mutable_offset)
      : offset(_offset),
        type(_type),
        verification(OptionVerificationType::kNormal),
        flags(OptionTypeFlags::kMutable),
        mutable_offset(_mutable_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset)
      : offset(_offset),
        type(_type),
        verification(_verification),
        flags(_flags),
        mutable_offset(_mutable_offset),
        parser_func(nullptr),
        string_func(nullptr),
        equals_func(nullptr) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset, const ParserFunc& _pfunc)
      : offset(_offset),
        type(_type),
        verification(_verification),
        flags(_flags),
        mutable_offset(_mutable_offset),
        parser_func(_pfunc),
        string_func(nullptr),
        equals_func(nullptr) {}

  OptionTypeInfo(int _offset, OptionType _type,
                 OptionVerificationType _verification, OptionTypeFlags _flags,
                 int _mutable_offset, const ParserFunc& _pfunc,
                 const StringFunc& _sfunc, const EqualsFunc& _efunc)
      : offset(_offset),
        type(_type),
        verification(_verification),
        flags(_flags),
        mutable_offset(_mutable_offset),
        parser_func(_pfunc),
        string_func(_sfunc),
        equals_func(_efunc) {}

  int offset;
  OptionType type;
  OptionVerificationType verification;
  OptionTypeFlags flags;  // This is a bitmask of OptionTypeFlag values
  int mutable_offset;
  ParserFunc parser_func;
  StringFunc string_func;
  EqualsFunc equals_func;

  bool IsDeprecated() const {
    return verification == OptionVerificationType::kDeprecated;
  }

  bool IsAlias() const {
    return verification == OptionVerificationType::kAlias;
  }

  bool IsMutable() const {
    return (flags & OptionTypeFlags::kMutable) == OptionTypeFlags::kMutable;
  }
  bool IsSharedPtr() const {
    return (flags & OptionTypeFlags::kShared) == OptionTypeFlags::kShared;
  }
  bool IsUniquePtr() const {
    return (flags & OptionTypeFlags::kUnique) == OptionTypeFlags::kUnique;
  }
  bool IsRawPtr() const {
    return (flags & OptionTypeFlags::kPointer) == OptionTypeFlags::kPointer;
  }

  bool IsEnum() const {
    return ((type == OptionType::kEnum) ||
            (flags & OptionTypeFlags::kEnum) == OptionTypeFlags::kEnum);
  }
  bool IsStruct() const {
    return ((type == OptionType::kStruct) ||
            (flags & OptionTypeFlags::kStruct) == OptionTypeFlags::kStruct);
  }

  bool IsConfigurable() const {
    return ((type == OptionType::kConfigurable) ||
            (flags & OptionTypeFlags::kConfigurable) ==
                OptionTypeFlags::kConfigurable);
  }

  bool IsCustomizable() const {
    return ((type == OptionType::kCustomizable) ||
            (flags & OptionTypeFlags::kCustomizable) ==
                OptionTypeFlags::kCustomizable);
  }

  template <typename T>
  const T* AsRawPointer(const char* addr) const {
    if (addr == nullptr) {
      return nullptr;
    } else if (IsUniquePtr()) {
      const std::unique_ptr<T>* ptr =
          reinterpret_cast<const std::unique_ptr<T>*>(addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      const std::shared_ptr<T>* ptr =
          reinterpret_cast<const std::shared_ptr<T>*>(addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      const T* const* ptr = reinterpret_cast<const T* const*>(addr);
      return *ptr;
    } else {
      return reinterpret_cast<const T*>(addr);
    }
  }
  template <typename T>
  T* AsRawPointer(char* addr) const {
    if (addr == nullptr) {
      return nullptr;
    } else if (IsUniquePtr()) {
      std::unique_ptr<T>* ptr = reinterpret_cast<std::unique_ptr<T>*>(addr);
      return ptr->get();
    } else if (IsSharedPtr()) {
      std::shared_ptr<T>* ptr = reinterpret_cast<std::shared_ptr<T>*>(addr);
      return ptr->get();
    } else if (IsRawPtr()) {
      T** ptr = reinterpret_cast<T**>(addr);
      return *ptr;
    } else {
      return reinterpret_cast<T*>(addr);
    }
  }

  template <typename T>
  static OptionTypeInfo Enum(
      int _offset, const std::unordered_map<std::string, T>* const map) {
    return OptionTypeInfo(
        _offset, OptionType::kEnum, OptionVerificationType::kNormal,
        OptionTypeFlags::kEnum, 0,
        [map](const std::string& name, const std::string& value,
              const ConfigOptions&, char* addr) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (ParseEnum<T>(*map, value, reinterpret_cast<T*>(addr))) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        [map](const std::string& name, const char* addr, const ConfigOptions&,
              std::string* value) {
          if (map == nullptr) {
            return Status::NotSupported("No enum mapping ", name);
          } else if (SerializeEnum<T>(*map, (*reinterpret_cast<const T*>(addr)),
                                      value)) {
            return Status::OK();
          } else {
            return Status::InvalidArgument("No mapping for enum ", name);
          }
        },
        [](const std::string&, const char* addr1, const char* addr2,
           const ConfigOptions&, std::string*) {
          return (*reinterpret_cast<const T*>(addr1) ==
                  *reinterpret_cast<const T*>(addr2));
        });
  }

  template <typename T>
  static OptionTypeInfo AsCustomS(int _offset, OptionVerificationType ovt) {
    return AsCustomS<T>(_offset, ovt, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomS(int _offset, OptionVerificationType ovt,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        OptionTypeFlags::kCustomizableS, 0,
        [](const std::string&, const std::string& value,
           const ConfigOptions& opts, char* addr) {
          auto* shared = reinterpret_cast<std::shared_ptr<T>*>(addr);
          return T::CreateFromString(value, opts, shared);
        },
        _sfunc, _efunc);
  }

  template <typename T>
  static OptionTypeInfo AsCustomU(int _offset, OptionVerificationType ovt) {
    return AsCustomU<T>(_offset, ovt, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomU(int _offset, OptionVerificationType ovt,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        OptionTypeFlags::kCustomizableU, 0,
        [](const std::string&, const std::string& value,
           const ConfigOptions& opts, char* addr) {
          auto* unique = reinterpret_cast<std::unique_ptr<T>*>(addr);
          return T::CreateFromString(value, opts, unique);
        },
        _sfunc, _efunc);
  }

  template <typename T>
  static OptionTypeInfo AsCustomP(int _offset, OptionVerificationType ovt) {
    return AsCustomP<T>(_offset, ovt, nullptr, nullptr);
  }

  template <typename T>
  static OptionTypeInfo AsCustomP(int _offset, OptionVerificationType ovt,
                                  const StringFunc& _sfunc,
                                  const EqualsFunc& _efunc) {
    return OptionTypeInfo(
        _offset, OptionType::kCustomizable, ovt,
        OptionTypeFlags::kCustomizableP, 0,
        [](const std::string&, const std::string& value,
           const ConfigOptions& opts, char* addr) {
          auto** pointer = reinterpret_cast<T**>(addr);
          return T::CreateFromString(value, opts, pointer);
        },
        _sfunc, _efunc);
  }

  Status ParseOption(const std::string& opt_name, const std::string& opt_value,
                     const ConfigOptions& options, char* opt_addr) const;
  Status SerializeOption(const std::string& opt_name, const char* opt_addr,
                         const ConfigOptions& options,
                         std::string* opt_value) const;
  bool MatchesOption(const std::string& opt_name, const char* this_offset,
                     const char* that_offset, const ConfigOptions& options,
                     std::string* mismatch) const;
};

typedef std::unordered_map<std::string, OptionTypeInfo> OptionTypeMap;

}  // namespace rocksdb
