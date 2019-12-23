// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/configurable.h"

#include "logging/logging.h"
#include "options/options_helper.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace rocksdb {

const std::string Configurable::kDefaultPrefix = "rocksdb.";

ConfigOptions::ConfigOptions()
#ifndef ROCKSDB_LITE
    : registry(ObjectRegistry::NewInstance())
#endif
{
}

#ifndef ROCKSDB_LITE
void Configurable::RegisterOptions(const std::string& name, void* opt_ptr,
                                   const OptionTypeMap* opt_map) {
  options_[name] = std::pair<void*, const OptionTypeMap*>(opt_ptr, opt_map);
}
#else
void Configurable::RegisterOptions(const std::string& name, void* opt_ptr,
                                   const OptionTypeMap*) {
  options_[name] = opt_ptr;
}
#endif  // ROCKSDB_LITE
/*********************************************************************************/
/*                                                                               */
/*       Methods for Configuring Options from Strings/Name-Value Pairs/Maps */
/*                                                                               */
/*********************************************************************************/

#ifndef ROCKSDB_LITE
/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any names properties that could not be found are returned in
 * used_opts. Returns OK if all of the properties were successfully updated.
 */
Status Configurable::DoConfigureOptions(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused_opts) {
  Status s, result, invalid;
  bool found_one = false;
  std::unordered_map<std::string, std::string> invalid_opts;
  // Go through all of the values in the input map and attempt to configure the
  // property.
  for (const auto& o : opts_map) {
    s = ConfigureOption(o.first, o.second, options);
    if (s.ok()) {
      found_one = true;
    } else if (s.IsNotFound()) {
      result = s;
      unused_opts->insert(o);
    } else if (s.IsNotSupported()) {
      invalid_opts.insert(o);
    } else {
      invalid_opts.insert(o);
      invalid = s;
    }
  }
  // While there are unused properties and we processed at least one,
  // go through the remaining unused properties and attempt to configure them.
  while (found_one && !unused_opts->empty()) {
    result = Status::OK();
    found_one = false;
    for (auto it = unused_opts->begin(); it != unused_opts->end();) {
      s = ConfigureOption(it->first, it->second, options);
      if (s.ok()) {
        found_one = true;
        it = unused_opts->erase(it);
      } else if (s.IsNotFound()) {
        result = s;
        ++it;
      } else if (s.IsNotSupported()) {
        invalid_opts.insert(*it);
        it = unused_opts->erase(it);
      } else {
        invalid_opts.insert(*it);
        it = unused_opts->erase(it);
        invalid = s;
      }
    }
  }
  if (!invalid_opts.empty()) {
    unused_opts->insert(invalid_opts.begin(), invalid_opts.end());
  }
  if (options.ignore_unknown_options || (invalid.ok() && result.ok())) {
    return Status::OK();
  } else if (!invalid.ok()) {
    return invalid;
  } else {
    return result;
  }
}
/**
 * Updates the object with the named-value property values, returning OK on
 * succcess. Any names properties that could not be found are returned in
 * used_opts. Returns OK if all of the properties were successfully updated.
 */
Status Configurable::DoConfigureFromMap(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* result) {
  std::unordered_map<std::string, std::string> unused;
  std::string curr_opts;
  if (!options.ignore_unknown_options) {
    // If we are not ignoring unused, get the defaults in case we need to reset
    GetOptionString(options, &curr_opts);
  }
  Status s = DoConfigureOptions(opts_map, options, &unused);
  if (result != nullptr && !unused.empty()) {
    result->insert(unused.begin(), unused.end());
  }
  if (options.ignore_unknown_options || s.ok()) {
    return Status::OK();
  } else {
    if (!curr_opts.empty()) {
      ConfigOptions reset = options;
      reset.ignore_unknown_options = true;
      // There are some options to reset from this current error
      ConfigureFromString(curr_opts, reset);
    }
    return s;
  }
}

Status Configurable::ConfigureFromMap(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options) {
  Status s = DoConfigureFromMap(opts_map, options, nullptr);
  return s;
}

Status Configurable::ConfigureFromMap(
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused_opts) {
  ConfigOptions unused = options;
  unused.ignore_unknown_options = true;
  return DoConfigureFromMap(opts_map, unused, unused_opts);
}

Status Configurable::ConfigureFromString(
    const std::string& opt_str, const ConfigOptions& options,
    std::unordered_map<std::string, std::string>* unused_opts) {
  std::unordered_map<std::string, std::string> opt_map;
  Status s = StringToMap(opt_str, &opt_map);
  if (s.ok()) {
    ConfigOptions unused = options;
    unused.ignore_unknown_options = true;
    s = DoConfigureFromMap(opt_map, unused, unused_opts);
  }
  return s;
}
#endif  // ROCKSDB_LITE

Status Configurable::ConfigureFromString(const std::string& opts_str,
                                         const ConfigOptions& options) {
  Status s;
  if (!opts_str.empty()) {
    if (opts_str.find(';') == std::string::npos &&
        opts_str.find('=') == std::string::npos) {
      return ParseStringOptions(opts_str, options);
    } else {
#ifndef ROCKSDB_LITE
      std::unordered_map<std::string, std::string> opt_map;

      s = StringToMap(opts_str, &opt_map);
      if (s.ok()) {
        s = DoConfigureFromMap(opt_map, options, nullptr);
      }
#else
      s = ParseStringOptions(opts_str, options);
#endif  // ROCKSDB_LITE
    }
  }
  return s;
}

/**
 * Sets the value of the named property to the input value, returning OK on
 * succcess.
 */
Status Configurable::ConfigureOption(const std::string& name,
                                     const std::string& value,
                                     const ConfigOptions& options) {
  const std::string& option_name = GetOptionName(name);
  const std::string& option_value =
      options.input_strings_escaped ? UnescapeOptionString(value) : value;
#ifndef ROCKSDB_LITE
  bool found_it = false;
  Status status;
  // Look for the name in all of the registered option maps until it is found
  for (const auto& iter : options_) {
    status = SetOption(*(iter.second.second), iter.second.first, option_name,
                       option_value, options, &found_it);
    if (found_it) {
      return status;
    }
  }
#endif
  return SetUnknown(option_name, option_value, options);
}

/**
 * Looks for the named option amongst the options for this type and sets
 * the value for it to be the input value.
 * If the name was found, found_option will be set to true and the resulting
 * status should be returned.
 */
#ifndef ROCKSDB_LITE
Status Configurable::SetOption(const OptionTypeMap& opt_map, void* opt_ptr,
                               const std::string& name,
                               const std::string& value,
                               const ConfigOptions& options,
                               bool* found_option) {
  // Look up the value in the map
  auto opt_iter = FindOption(name, opt_map);
  if (opt_iter == opt_map.end()) {
    return Status::InvalidArgument("Could not find option: ", name);
  } else {
    const auto& opt_info = opt_iter->second;
    *found_option = true;
    char* opt_addr = GetOptAddress(opt_info, opt_ptr);
    if (opt_info.IsDeprecated()) {
      return Status::OK();
    } else if (opt_addr == nullptr) {
      if (IsMutable()) {
        return Status::InvalidArgument("Option not changeable: " + name);
      } else {
        return Status::NotFound("Could not find property:", name);
      }
    } else if (name == opt_iter->first) {
      // If the name matches exactly, parse the option
      return ParseOption(opt_info, opt_addr, name, value, options);
    } else if (opt_info.IsStruct()) {
      // The option is "<struct>.<name>"
      const auto* struct_map =
          GetOptionsTypeMap(name.substr(0, opt_iter->first.size()));
      if (struct_map != nullptr) {
        return SetOption(*struct_map, opt_addr,
                         name.substr(opt_iter->first.size() + 1), value,
                         options, found_option);
      } else {
        return Status::InvalidArgument("Could not find struct: ", name);
      }
    } else {
      // The option is <config>.<name>
      Configurable* config = opt_info.AsRawPointer<Configurable>(opt_addr);
      if (value.empty()) {
        return Status::OK();
      } else if (config == nullptr) {
        return Status::NotFound("Could not find configurable: ", name);
      } else {
        return config->ConfigureOption(name.substr(opt_iter->first.size() + 1),
                                       value, options);
      }
    }
  }
}

Status Configurable::ParseOption(const OptionTypeInfo& opt_info, char* opt_addr,
                                 const std::string& opt_name,
                                 const std::string& opt_value,
                                 const ConfigOptions& options) {
  Status status = opt_info.ParseOption(opt_name, opt_value, options, opt_addr);
  if (status.ok()) {
    return status;
  } else if (opt_info.IsStruct()) {
    status = SetStruct(opt_name, opt_value, options, opt_addr);
  } else if (opt_info.IsConfigurable()) {
    Configurable* config = opt_info.AsRawPointer<Configurable>(opt_addr);
    if (opt_value.empty()) {
      return Status::OK();
    } else if (config == nullptr) {
      return Status::NotFound("Could not find configurable: ", opt_name);
    } else if (opt_value.find("=") != std::string::npos) {
      return config->ConfigureFromString(opt_value, options);
    } else {
      return config->ConfigureOption(opt_name, opt_value, options);
    }
  }
  return status;
}

Status Configurable::SetStruct(const std::string& opt_name,
                               const std::string& opt_value,
                               const ConfigOptions& options, char* opt_addr) {
  auto const* struct_map = GetOptionsTypeMap(opt_name);
  std::unordered_map<std::string, std::string> opt_map;
  Status status = StringToMap(opt_value, &opt_map);
  if (struct_map == nullptr) {
    status = Status::InvalidArgument("No map for option ", opt_name);
  } else if (status.ok()) {
    for (const auto iter : opt_map) {
      bool found_it = false;
      status = SetOption(*struct_map, opt_addr, iter.first, iter.second,
                         options, &found_it);
      if (!found_it) {
        return status;
      }
    }
  }
  return status;
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Converting Options into strings */
/*                                                                               */
/*********************************************************************************/

#ifndef ROCKSDB_LITE
std::string Configurable::ToString(const std::string& prefix,
                                   const ConfigOptions& options) const {
  std::string result = AsString(prefix, options);
  if (result.empty() || result.find('=') == std::string::npos) {
    return result;
  } else if (options.IsDetached()) {
    return result;
  } else {
    return "{" + result + "}";
  }
}

std::string Configurable::AsString(const std::string& header,
                                   const ConfigOptions& options) const {
  std::string result;
  Status s = SerializeAllOptions(header, options, &result);
  assert(s.ok());
  return result;
}

Status Configurable::GetOptionString(const ConfigOptions& options,
                                     std::string* result) const {
  assert(result);
  result->clear();
  if (options.UsePrefix()) {
    return SerializeAllOptions(GetOptionsPrefix(), options, result);
  } else {
    return SerializeAllOptions("", options, result);
  }
}

Status Configurable::GetOptionNames(
    const ConfigOptions& options,
    std::unordered_set<std::string>* result) const {
  assert(result);
  auto prefix = options.UsePrefix() ? GetOptionsPrefix() : "";
  return ListAllOptions(prefix, options, result);
}

Status Configurable::ListAllOptions(
    const std::string& prefix, const ConfigOptions& options,
    std::unordered_set<std::string>* result) const {
  Status status;
  for (auto const iter : options_) {
    status = ListOptions(*(iter.second.second), iter.second.first, prefix,
                         options, result);
    if (!status.ok()) {
      return status;
    }
  }
  return status;
}

#endif  // ROCKSDB_LITE

Status Configurable::GetOption(const std::string& name,
#ifndef ROCKSDB_LITE
                               const ConfigOptions& options,
#else
                               const ConfigOptions& /*options*/,
#endif  // ROCKSDB_LITE
                               std::string* value) const {
  const std::string& opt_name = GetOptionName(name);
  assert(value);
  value->clear();
#ifndef ROCKSDB_LITE
  return GetOneOption(opt_name, options, value);
#endif
  return Status::NotFound("Could not find option: ", opt_name);
}

#ifndef ROCKSDB_LITE
Status Configurable::GetOneOption(const std::string& opt_name,
                                  const ConfigOptions& options,
                                  std::string* value) const {
  Status status;
  bool found_it = false;
  // Look for option directly
  for (auto iter : options_) {
    const auto* opt_map = iter.second.second;
    status = SerializeSingleOption(*opt_map, iter.second.first, opt_name,
                                   options, value, &found_it);
    if (found_it) {
      return status;
    }
  }
  return Status::NotFound("Could not find option: ", opt_name);
}

Status Configurable::SerializeOption(const std::string& opt_name,
                                     const OptionTypeInfo& opt_info,
                                     const char* opt_addr,
                                     const std::string& prefix,
                                     const ConfigOptions& options,
                                     std::string* opt_value) const {
  // If the option is no longer used in rocksdb and marked as deprecated,
  // we skip it in the serialization.
  Status s = opt_info.SerializeOption(prefix, opt_addr, options, opt_value);
  if (s.ok()) {
    return s;
  } else if (opt_info.IsStruct()) {
    auto const* struct_map = GetOptionsTypeMap(opt_name);
    ConfigOptions copy = options;
    copy.string_mode ^= ConfigOptions::StringMode::kOptionPrefix;
    if (struct_map == nullptr) {
      s = Status::InvalidArgument("No map for option ", opt_name);
    } else if (options.IsDetached()) {
      s = SerializeOptions(*struct_map, opt_addr, prefix, copy, opt_value);
    } else {
      std::string value;
      copy.delimiter = ";";
      s = SerializeOptions(*struct_map, opt_addr, "", copy, &value);
      if (s.ok()) {
        *opt_value = "{" + value + "}";
      }
    }
  } else if (opt_info.IsConfigurable()) {
    const Configurable* config = opt_info.AsRawPointer<Configurable>(opt_addr);
    if (config != nullptr) {
      std::string value;
      ConfigOptions copy = options;
      if (options.IsDetached()) {
        *opt_value = config->ToString(prefix, copy);
      } else {
        copy.string_mode ^= ConfigOptions::StringMode::kOptionPrefix;
        copy.delimiter = ";";
        *opt_value = config->ToString(copy);
      }
    }
    return Status::OK();
  }
  return s;
}

Status Configurable::SerializeOptions(const OptionTypeMap& opt_map,
                                      const void* opt_ptr,
                                      const std::string& header,
                                      const ConfigOptions& options,
                                      std::string* result) const {
  for (auto opt_iter = opt_map.begin(); opt_iter != opt_map.end(); ++opt_iter) {
    const auto& opt_name = opt_iter->first;
    const auto& opt_info = opt_iter->second;
    const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
    if (opt_addr != nullptr && !opt_info.IsAlias()) {
      std::string value;
      Status s = SerializeOption(
          opt_name, opt_info, opt_addr,
          (options.IsDetached() ? header + opt_name + "." : header), options,
          &value);
      if (!s.ok()) {
        return s;
      } else if (!opt_info.IsConfigurable() && !opt_info.IsStruct()) {
        PrintSingleOption(header, opt_name, value, options.delimiter, result);
      } else if (options.IsDetached()) {
        result->append(value);
      } else if (!value.empty()) {
        PrintSingleOption(header, opt_name, value, options.delimiter, result);
      }
    }
  }
  return Status::OK();
}

Status Configurable::SerializeAllOptions(const std::string& header,
                                         const ConfigOptions& options,
                                         std::string* result) const {
  assert(result);
  for (auto const iter : options_) {
    const auto* opt_map = GetOptionsTypeMap(iter.first);
    if (opt_map != nullptr) {
      Status s = SerializeOptions(*opt_map, iter.second.first, header, options,
                                  result);
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}

Status Configurable::ListOptions(
    const OptionTypeMap& opt_map, const void* opt_ptr,
    const std::string& prefix, const ConfigOptions& options,
    std::unordered_set<std::string>* result) const {
  Status status;
  for (auto opt_iter = opt_map.begin(); opt_iter != opt_map.end(); ++opt_iter) {
    const auto& opt_name = opt_iter->first;
    const auto& opt_info = opt_iter->second;
    // If the option is no longer used in rocksdb and marked as deprecated,
    // we skip it in the serialization.
    const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
    if (opt_addr != nullptr && !opt_info.IsDeprecated() &&
        !opt_info.IsAlias()) {
      if (!options.IsDetached()) {
        result->emplace(prefix + opt_name);
      } else if (opt_info.IsStruct()) {
        ConfigOptions copy = options;
        copy.string_mode ^= ConfigOptions::StringMode::kOptionPrefix;
        const auto* struct_map = GetOptionsTypeMap(opt_name);
        if (struct_map != nullptr) {
          status = ListOptions(*struct_map, opt_ptr, prefix + opt_name + ".",
                               copy, result);
        } else {
          status = Status::NotSupported("Cannot find options map for struct ",
                                        opt_name);
        }
      } else if (opt_info.IsConfigurable()) {
        const auto* config = opt_info.AsRawPointer<Configurable>(opt_addr);
        if (config != nullptr) {
          std::unordered_set<std::string> names;
          status = config->GetOptionNames(options, &names);
          for (const auto& name : names) {
            result->emplace(prefix + opt_name + "." + name);
          }
        }
      } else {
        result->emplace(prefix + opt_name);
      }
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status::OK();
}

Status Configurable::SerializeSingleOption(
    const OptionTypeMap& opt_map, const void* opt_ptr, const std::string& name,
    const ConfigOptions& options, std::string* value, bool* found_it) const {
  // Look up the value in the map
  auto opt_iter = FindOption(name, opt_map);
  if (opt_iter != opt_map.end()) {
    const auto& opt_info = opt_iter->second;
    *found_it = true;
    const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
    if (opt_addr != nullptr) {
      if (name == opt_iter->first) {
        return SerializeOption(name, opt_info, opt_addr, "", options, value);
      } else if (opt_info.IsStruct()) {
        auto const* struct_map =
            GetOptionsTypeMap(name.substr(0, opt_iter->first.size()));
        if (struct_map != nullptr) {
          return SerializeSingleOption(*struct_map, opt_ptr,
                                       name.substr(opt_iter->first.size() + 1),
                                       options, value, found_it);
        }
      } else if (opt_info.IsConfigurable()) {
        auto const* config = opt_info.AsRawPointer<Configurable>(opt_addr);
        if (config != nullptr) {
          return config->GetOption(name.substr(opt_iter->first.size() + 1),
                                   options, value);
        }
      }
    }
  }
  return Status::NotFound("Cannot find option: ", name);
}

#endif  // ROCKSDB_LITE

void Configurable::PrintSingleOption(const std::string& prefix,
                                     const std::string& name,
                                     const std::string& value,
                                     const std::string& delimiter,
                                     std::string* result) const {
  result->append(prefix);
  result->append(name);  // Add the name
  result->append("=");
  result->append(value);
  result->append(delimiter);
}

/*********************************************************************************/
/*                                                                               */
/*       Methods for Validating and Sanitizing Configurables */
/*                                                                               */
/*********************************************************************************/

Status Configurable::SanitizeOptions() {
  Options opts;
  return Sanitize(opts, opts);
}

Status Configurable::SanitizeOptions(DBOptions& db_opts) {
  ColumnFamilyOptions cf_opts;
  return Sanitize(db_opts, cf_opts);
}

Status Configurable::SanitizeOptions(DBOptions& db_opts,
                                     ColumnFamilyOptions& cf_opts) {
  Status status = Sanitize(db_opts, cf_opts);
  if (status.ok()) {
    status = Validate(db_opts, cf_opts);
  }
  return status;
}

#ifndef ROCKSDB_LITE
Status Configurable::Sanitize(DBOptions& db_opts,
                              ColumnFamilyOptions& cf_opts) {
  Status status;
  for (auto opt_iter : options_) {
    const auto* opt_map = GetOptionsTypeMap(opt_iter.first);
    if (opt_map != nullptr) {
      for (auto map_iter = opt_map->begin(); map_iter != opt_map->end();
           ++map_iter) {
        auto& opt_info = map_iter->second;
        if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
          if (opt_info.IsConfigurable()) {
            char* opt_addr = GetOptAddress(opt_info, opt_iter.second.first);
            Configurable* config =
                opt_info.AsRawPointer<Configurable>(opt_addr);
            if (config != nullptr) {
              status = config->Sanitize(db_opts, cf_opts);
            } else if (opt_info.verification !=
                       OptionVerificationType::kByNameAllowFromNull) {
              status = Status::NotFound("Missing configurable object",
                                        map_iter->first);
            }
            if (!status.ok()) {
              return status;
            }
          }
        }
      }
    }
  }
  return status;
}
#else
Status Configurable::Sanitize(DBOptions&, ColumnFamilyOptions&) {
  return Status::OK();
}
#endif  // ROCKSDB_LITE

Status Configurable::ValidateOptions() const {
  Options opts;
  return ValidateOptions(opts, opts);
}

Status Configurable::ValidateOptions(const DBOptions& db_opts) const {
  ColumnFamilyOptions cf_opts;
  return Validate(db_opts, cf_opts);
}

Status Configurable::ValidateOptions(const DBOptions& db_opts,
                                     const ColumnFamilyOptions& cf_opts) const {
  return Validate(db_opts, cf_opts);
}

#ifndef ROCKSDB_LITE
Status Configurable::Validate(const DBOptions& db_opts,
                              const ColumnFamilyOptions& cf_opts) const {
  Status status;
  for (auto opt_iter : options_) {
    const auto* opt_map = opt_iter.second.second;
    const auto* opt_ptr = opt_iter.second.first;
    if (opt_map != nullptr) {
      for (auto map_iter = opt_map->begin(); map_iter != opt_map->end();
           ++map_iter) {
        auto& opt_info = map_iter->second;
        if (!opt_info.IsDeprecated() && !opt_info.IsAlias()) {
          if (opt_info.IsConfigurable()) {
            const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
            const Configurable* config =
                opt_info.AsRawPointer<Configurable>(opt_addr);
            if (config != nullptr) {
              status = config->Validate(db_opts, cf_opts);
            } else if (opt_info.verification !=
                       OptionVerificationType::kByNameAllowFromNull) {
              status = Status::NotFound("Missing configurable object",
                                        map_iter->first);
            }
            if (!status.ok()) {
              return status;
            }
          }
        }
      }
    }
  }
  return status;
}
#else
Status Configurable::Validate(const DBOptions&,
                              const ColumnFamilyOptions&) const {
  return Status::OK();
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Comparing Configurables */
/*                                                                               */
/*********************************************************************************/

bool Configurable::Matches(const Configurable* other,
                           const ConfigOptions& options) const {
  std::string name;
  return Matches(other, options, &name);
}

bool Configurable::Matches(const Configurable* other,
                           const ConfigOptions& options,
                           std::string* name) const {
  assert(name);
  name->clear();
  if (this == other || options.IsCheckDisabled()) {
    return true;
  } else if (other != nullptr) {
    return MatchesOption(other, options, name);
  } else {
    return false;
  }
}

bool Configurable::MatchesOption(const Configurable* that,
                                 const ConfigOptions& options,
                                 std::string* name) const {
  assert(name != nullptr);
  if (this == that || options.IsCheckDisabled()) {
    return true;
  } else {
#ifndef ROCKSDB_LITE
    for (auto const iter : options_) {
      const char* this_option =
          reinterpret_cast<const char*>(iter.second.first);
      const char* that_option = that->GetOptions<const char>(iter.first);
      const auto* opt_map = GetOptionsTypeMap(iter.first);
      if (opt_map != nullptr &&
          !OptionsAreEqual(*opt_map, GetOptionsSanityCheckLevel(iter.first),
                           IsMutable(), this_option, that, that_option, options,
                           name)) {
        return false;
      }
    }
    return true;
#endif  // ROCKSDB_LITE
    return false;
  }
}

OptionsSanityCheckLevel Configurable::GetSanityLevelForOption(
    const std::unordered_map<std::string, OptionsSanityCheckLevel>* map,
    const std::string& name) const {
  if (map != nullptr) {
    auto iter = map->find(name);
    if (iter != map->end()) {
      return iter->second;
    }
  }
  return OptionsSanityCheckLevel::kSanityLevelExactMatch;
}

#ifndef ROCKSDB_LITE

bool Configurable::VerifyOptionEqual(const std::string& opt_name,
                                     const OptionTypeInfo& opt_info,
                                     const char* this_offset,
                                     const char* that_offset,
                                     const ConfigOptions& options) const {
  std::string this_value, that_value;
  if (opt_info.verification != OptionVerificationType::kByName &&
      opt_info.verification != OptionVerificationType::kByNameAllowNull &&
      opt_info.verification != OptionVerificationType::kByNameAllowFromNull) {
    return false;
  } else if (opt_info.string_func != nullptr) {
    if (!opt_info.string_func(opt_name, this_offset, options, &this_value)
             .ok() ||
        !opt_info.string_func(opt_name, that_offset, options, &that_value)
             .ok()) {
      return false;
    }
  } else if (!SerializeOption(opt_name, opt_info, this_offset, "", options,
                              &this_value)
                  .ok() ||
             !SerializeOption(opt_name, opt_info, that_offset, "", options,
                              &that_value)
                  .ok()) {
    return false;
  }
  if (opt_info.verification == OptionVerificationType::kByNameAllowFromNull &&
      that_value == kNullptrString) {
    return true;
  } else if (opt_info.verification ==
                 OptionVerificationType::kByNameAllowNull &&
             this_value == kNullptrString) {
    return true;
  } else if (this_value != that_value) {
    return false;
  } else {
    return true;
  }
}

bool Configurable::IsConfigEqual(const std::string& /* opt_name */,
                                 const OptionTypeInfo& /* opt_info */,
                                 const Configurable* this_config,
                                 const Configurable* that_config,
                                 const ConfigOptions& options,
                                 std::string* mismatch) const {
  if (this_config == that_config) {
    return true;
  } else if (this_config != nullptr && that_config != nullptr) {
    return this_config->Matches(that_config, options, mismatch);
  } else {
    return false;
  }
}

bool Configurable::OptionIsEqual(const std::string& opt_name,
                                 const OptionTypeInfo& opt_info,
                                 const char* this_offset,
                                 const char* that_offset,
                                 const ConfigOptions& options,
                                 std::string* bad_name) const {
  std::string mismatch;
  if (opt_info.MatchesOption(opt_name, this_offset, that_offset, options,
                             &mismatch)) {
    return true;
  } else if (opt_info.IsStruct()) {
    const auto opt_map = GetOptionsTypeMap(opt_name);
    if (opt_map != nullptr &&
        OptionsAreEqual(*opt_map, GetOptionsSanityCheckLevel(opt_name), false,
                        this_offset, nullptr, that_offset, options,
                        &mismatch)) {
      return true;
    }
  } else if (opt_info.IsConfigurable()) {
    const auto* this_config = opt_info.AsRawPointer<Configurable>(this_offset);
    const auto* that_config = opt_info.AsRawPointer<Configurable>(that_offset);
    if (IsConfigEqual(opt_name, opt_info, this_config, that_config, options,
                      &mismatch)) {
      return true;
    }
  }
  *bad_name = opt_name;
  if (!mismatch.empty()) {
    bad_name->append("." + mismatch);
  }
  return false;
}

bool Configurable::OptionsAreEqual(
    const OptionTypeMap& opt_map,
    const std::unordered_map<std::string, OptionsSanityCheckLevel>* opt_level,
    bool only_check_mutables, const char* this_option, const Configurable* that,
    const char* that_option, const ConfigOptions& options,
    std::string* bad_name) const {
  *bad_name = "";
  if (this_option == that_option) {
    return true;
  } else if (this_option == nullptr || that_option == nullptr) {
    return false;
  } else {
    for (auto& pair : opt_map) {
      const auto& opt_info = pair.second;
      // We skip checking deprecated variables as they might
      // contain random values since they might not be initialized
      if (only_check_mutables && !opt_info.IsMutable()) {
        // If we are only checking mutables and this option is not mutable,
        // skip it
        continue;
      } else if ((!opt_info.IsDeprecated() && !opt_info.IsAlias())) {
        OptionsSanityCheckLevel level =
            GetSanityLevelForOption(opt_level, pair.first);
        if (options.IsCheckEnabled(level)) {
          const char* that_offset;
          const char* this_offset = this->GetOptAddress(opt_info, this_option);
          if (that != nullptr) {  // Structs will have that == nullptr
            that_offset = that->GetOptAddress(opt_info, that_option);
          } else {
            that_offset = this->GetOptAddress(opt_info, that_option);
          }
          if (level < options.sanity_level) {
            ConfigOptions copy = options;
            copy.sanity_level = level;
            if (!OptionIsEqual(pair.first, opt_info, this_offset, that_offset,
                               copy, bad_name) &&
                !VerifyOptionEqual(pair.first, opt_info, this_offset,
                                   that_offset, copy)) {
              return false;
            }
          } else if (!OptionIsEqual(pair.first, opt_info, this_offset,
                                    that_offset, options, bad_name) &&
                     !VerifyOptionEqual(pair.first, opt_info, this_offset,
                                        that_offset, options)) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
#endif  // ROCKSDB_LITE

/*********************************************************************************/
/*                                                                               */
/*       Methods for Retrieving Options from Configurables */
/*                                                                               */
/*********************************************************************************/

const void* Configurable::GetOptionsPtr(const std::string& name) const {
  const auto& iter = options_.find(name);
  if (iter != options_.end()) {
#ifndef ROCKSDB_LITE
    return iter->second.first;
#else
    return iter->second;
#endif  // ROCKSDB_LITE
  }
  return nullptr;
}

#ifndef ROCKSDB_LITE
const OptionTypeMap* Configurable::GetOptionsTypeMap(
    const std::string& name) const {
  const auto& iter = options_.find(name);
  if (iter != options_.end()) {
    return iter->second.second;
  }
  assert(nullptr);
  return nullptr;
}
#endif

/*********************************************************************************/
/*                                                                               */
/*       Methods for Logging Configurables */
/*                                                                               */
/*********************************************************************************/

void Configurable::DumpOptions(Logger* logger, const std::string& indent,
#ifndef ROCKSDB_LITE
                               const ConfigOptions& options
#else
                               const ConfigOptions& /*options*/
#endif
) const {
  std::string value = GetPrintableOptions();
  if (!value.empty()) {
    ROCKS_LOG_HEADER(logger, "%s Options: %s\n", indent.c_str(), value.c_str());
  } else {
#ifndef ROCKSDB_LITE
    for (auto const iter : options_) {
      std::string header = iter.first;
      if (options.UsePrefix()) {
        header.append("::" + GetOptionsPrefix());
      } else {
        header.append(".");
      }
      const auto* opt_map = GetOptionsTypeMap(iter.first);
      const auto* opt_ptr = iter.second.first;
      if (opt_map != nullptr) {
        for (auto opt_iter = opt_map->begin(); opt_iter != opt_map->end();
             ++opt_iter) {
          auto& opt_name = opt_iter->first;
          auto& opt_info = opt_iter->second;
          // If the option is no longer used in rocksdb and marked as
          // deprecated, we skip it in the serialization.
          const char* opt_addr = GetOptAddress(opt_info, opt_ptr);
          if (opt_addr != nullptr && !opt_info.IsAlias()) {
            if (opt_info.IsConfigurable() && options.IsDetached()) {
              // Nested options on individual lines
              const Configurable* config =
                  opt_info.AsRawPointer<Configurable>(opt_addr);
              if (config != nullptr) {
                ROCKS_LOG_HEADER(logger, "%s%s%s Options:\n", indent.c_str(),
                                 header.c_str(), opt_name.c_str());
                config->Dump(logger, indent + "  ", options);
              }
            } else {
              Status s = SerializeOption(opt_name, opt_info, opt_addr, "",
                                         options, &value);
              if (s.ok()) {
                ROCKS_LOG_HEADER(logger, "%s%s%s: %s\n", indent.c_str(),
                                 header.c_str(), opt_name.c_str(),
                                 value.c_str());
              }
            }
          }
        }
      }
    }
#endif  // ROCKSDB_LITE
  }
}

/*********************************************************************************/
/*                                                                               */
/*       Configurable Support and Utility Methods */
/*                                                                               */
/*********************************************************************************/

std::string Configurable::GetOptionName(const std::string& long_name) const {
  auto& prefix = GetOptionsPrefix();
  auto prefix_len = prefix.length();
  if (long_name.compare(0, prefix_len, prefix) == 0) {
    return long_name.substr(prefix_len);
  } else {
    return long_name;
  }
}

#ifndef ROCKSDB_LITE

const char* Configurable::GetOptAddress(const OptionTypeInfo& opt_info,
                                        const void* ptr) const {
  if (!IsMutable()) {
    return reinterpret_cast<const char*>(ptr) + opt_info.offset;
  } else if (opt_info.IsMutable()) {
    return reinterpret_cast<const char*>(ptr) + opt_info.mutable_offset;
  } else {
    return nullptr;
  }
}

// Returns the offset of ptr for the given option
// If the configurable is mutable, returns the mutable offset (if any).
// Otherwise returns the standard one
char* Configurable::GetOptAddress(const OptionTypeInfo& opt_info,
                                  void* ptr) const {
  if (!IsMutable()) {
    return reinterpret_cast<char*>(ptr) + opt_info.offset;
  } else if (opt_info.IsMutable()) {
    return reinterpret_cast<char*>(ptr) + opt_info.mutable_offset;
  } else {
    return nullptr;
  }
}

OptionTypeMap::const_iterator Configurable::FindOption(
    const std::string& option, const OptionTypeMap& options_map) const {
  auto iter = options_map.find(option);  // Look up the value in the map
  if (iter == options_map.end()) {       // Didn't find the option in the map
    auto idx = option.find(".");         // Look for a separator
    if (idx > 0 && idx != std::string::npos) {  // We found a separator
      auto siter =
          options_map.find(option.substr(0, idx));  // Look for the short name
      if (siter != options_map.end()) {             // We found the short name
        if (siter->second.IsConfigurable() ||  // The object is a configurable
            siter->second.IsStruct()) {        // Or the object is a struct
          return siter;                        // Return the short name value
        }
      }
    }
  }
  return iter;
}
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
