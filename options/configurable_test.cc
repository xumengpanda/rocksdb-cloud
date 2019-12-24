//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/configurable.h"

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "options/configurable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
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
class StringLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[1000];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    string_.append(buffer);
  }
  const std::string& str() const { return string_; }
  void clear() { string_.clear(); }

 private:
  std::string string_;
};

enum SimpleEnum { kSimpleA, kSimpleB };

static const std::unordered_map<std::string, int> simple_enum_map = {
    {"A", SimpleEnum::kSimpleA},
    {"B", SimpleEnum::kSimpleB},
};

struct TestOptions {
  int i = 0;
  bool b = false;
  bool d = true;
  SimpleEnum e = SimpleEnum::kSimpleA;
  std::string s = "";
  std::string u = "";
  std::unique_ptr<Configurable> unique;
  std::shared_ptr<Configurable> shared;
  Configurable* config = nullptr;
};

static OptionTypeMap simple_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct TestOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bool",
     {offsetof(struct TestOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"string",
     {offsetof(struct TestOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

static OptionTypeMap enum_option_info = {
#ifndef ROCKSDB_LITE
    {"enum",
     OptionTypeInfo::Enum(offsetof(struct TestOptions, e), &simple_enum_map)}
#endif
};

static OptionTypeMap inherited_option_info = {
#ifndef ROCKSDB_LITE
    {"unique",
     {offsetof(struct TestOptions, unique), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kMConfigurableU,
      offsetof(struct TestOptions, unique)}},
    {"shared",
     {offsetof(struct TestOptions, shared), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableS, 0}},
    {"pointer",
     {offsetof(struct TestOptions, config), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableP, 0}},
#endif  // ROCKSDB_LITE
};

static OptionTypeMap custom_option_info = {
#ifndef ROCKSDB_LITE
    {"shared", OptionTypeInfo::AsCustomS<TableFactory>(
                   offsetof(struct TestOptions, shared),
                   OptionVerificationType::kByName)},
#endif
};

static OptionTypeMap all_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct TestOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
      offsetof(struct TestOptions, i)}},
    {"bool",
     {offsetof(struct TestOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"string",
     {offsetof(struct TestOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"enum",
     OptionTypeInfo::Enum(offsetof(struct TestOptions, e), &simple_enum_map)},
    {"unique",
     {offsetof(struct TestOptions, unique), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kMConfigurableU,
      offsetof(struct TestOptions, unique)}},
    {"shared",
     {offsetof(struct TestOptions, shared), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableS, 0}},
    {"pointer",
     {offsetof(struct TestOptions, config), OptionType::kConfigurable,
      OptionVerificationType::kNormal, OptionTypeFlags::kConfigurableP, 0}},
#endif  // ROCKSDB_LITE
};

static OptionTypeMap struct_option_info = {
#ifndef ROCKSDB_LITE
    {"struct",
     {0, OptionType::kStruct, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable, 0}},
#endif  // ROCKSDB_LITE
};

enum TestConfigMode {
  kDefaultMode = 0,   // Use no inner nested configurations
  kSharedMode = 0x1,  // Use shared configuration
  kUniqueMode = 0x2,  // Use unique configuration
  kRawPtrMode = 0x4,  // Use pointer configuration
  kNestedMode = (kSharedMode | kUniqueMode | kRawPtrMode),
  kMutableMode = 0x8,  // Configuration is mutable
};

class SimpleConfigurable : public Configurable {
 public:
 protected:
  std::string name_;
  std::string prefix_;
  TestOptions options_;
  int mode_;

 public:
  static Configurable* Create(const std::string& name = "simple",
                              const OptionTypeMap* map = &simple_option_info,
                              int mode = TestConfigMode::kDefaultMode) {
    return new SimpleConfigurable(name, mode, map);
  }

  SimpleConfigurable(const std::string& name, int mode,
                     const OptionTypeMap* map)
      : name_(name), mode_(mode) {
    prefix_ = "test." + name + ".";
    RegisterOptions(name, &options_, map);
    if ((mode & TestConfigMode::kUniqueMode) != 0) {
      options_.unique.reset(SimpleConfigurable::Create("unique"));
    }
    if ((mode & TestConfigMode::kSharedMode) != 0) {
      options_.shared.reset(SimpleConfigurable::Create("shared"));
    }
    if ((mode & TestConfigMode::kRawPtrMode) != 0) {
      options_.config = SimpleConfigurable::Create("pointer");
    }
  }
  ~SimpleConfigurable() override { delete options_.config; }

 protected:
  const std::string& GetOptionsPrefix() const override { return prefix_; }

  Status Validate(const DBOptions& db_opts,
                  const ColumnFamilyOptions& cf_opts) const override {
    if (!options_.b) {
      return Status::InvalidArgument("Sanitized must be true");
    } else {
      return Configurable::Validate(db_opts, cf_opts);
    }
  }

  Status Sanitize(DBOptions& db_opts, ColumnFamilyOptions& cf_opts) override {
    options_.b = true;
    return Configurable::Sanitize(db_opts, cf_opts);
  }

  bool IsMutable() const override {
    return (mode_ & TestConfigMode::kMutableMode) != 0;
  }
};  // End class SimpleConfigurable

#ifndef ROCKSDB_LITE
class StructConfigurable : public SimpleConfigurable {
 public:
  static Configurable* Create(const std::string& name = "simple-struct",
                              const OptionTypeMap* map = &simple_option_info,
                              int mode = TestConfigMode::kDefaultMode) {
    return new StructConfigurable(name, map, mode);
  }

  StructConfigurable(const std::string& name, const OptionTypeMap* map,
                     unsigned char mode)
      : SimpleConfigurable(name, mode, &struct_option_info), struct_map(map) {}

 protected:
  const OptionTypeMap* GetOptionsTypeMap(
      const std::string& options) const override {
    if (options == "struct") {
      return struct_map;
    } else {
      return Configurable::GetOptionsTypeMap(options);
    }
  }

 private:
  const OptionTypeMap* struct_map;
};
#endif  // ROCKSDB_LITE

class InheritedConfigurable : public SimpleConfigurable {
 public:
  static Configurable* Create(
      const std::string& name = "inherited",
      unsigned char mode = TestConfigMode::kNestedMode,
      const OptionTypeMap* map = &inherited_option_info) {
    return new InheritedConfigurable(name, mode, map);
  }

  InheritedConfigurable(const std::string& name, unsigned char mode,
                        const OptionTypeMap* map)
      : SimpleConfigurable(name, mode, &simple_option_info) {
    RegisterOptions("InheritedOptions", &options_, map);
  }
};

using ConfigTestFactoryFunc = std::function<Configurable*()>;

class ConfigurableTest : public testing::Test {
 public:
  ConfigurableTest() {}

  ConfigOptions options_;
};

class ConfigurableParamTest
    : public ConfigurableTest,
      virtual public ::testing::WithParamInterface<
          std::pair<std::string, ConfigTestFactoryFunc> > {
 public:
  ConfigurableParamTest() {
    configuration_ = GetParam().first;
    factory_ = GetParam().second;
    object_.reset(factory_());
  }
  void TestConfigureOptions(const ConfigOptions& opts);
  ConfigTestFactoryFunc factory_;
  std::string configuration_;
  std::unique_ptr<Configurable> object_;
};

TEST_F(ConfigurableTest, GetOptionsPtrTest) {
  std::string opt_str;
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  ASSERT_NE(configurable->GetOptions<TestOptions>("simple"), nullptr);
  ASSERT_EQ(configurable->GetOptions<TestOptions>("bad-opt"), nullptr);
}

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE
TEST_F(ConfigurableTest, ConfigureFromMapTest) {
  std::unordered_map<std::string, std::string> options_map = {
      {"int", "1"}, {"bool", "true"}, {"string", "string"}};
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  ASSERT_OK(configurable->ConfigureFromMap(options_map, options_));
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->i, 1);
  ASSERT_EQ(opts->b, true);
  ASSERT_EQ(opts->s, "string");
}

TEST_F(ConfigurableTest, ConfigureFromStringTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  ASSERT_OK(
      configurable->ConfigureFromString("int=1;bool=true;string=s", options_));
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->i, 1);
  ASSERT_EQ(opts->b, true);
  ASSERT_EQ(opts->s, "s");
}

TEST_F(ConfigurableTest, ConfigureIgnoreTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  std::unordered_map<std::string, std::string> options_map = {{"unused", "u"}};
  ConfigOptions ignore = options_;
  ignore.ignore_unknown_options = true;
  ASSERT_NOK(configurable->ConfigureFromMap(options_map, options_));
  ASSERT_OK(configurable->ConfigureFromMap(options_map, ignore));
  ASSERT_NOK(configurable->ConfigureFromString("unused=u", options_));
  ASSERT_OK(configurable->ConfigureFromString("unused=u", ignore));
}

TEST_F(ConfigurableTest, ConfigureNestedOptionsTest) {
  std::unique_ptr<Configurable> base, copy;
  std::string opt_str;

  base.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                        TestConfigMode::kNestedMode));
  copy.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                        TestConfigMode::kNestedMode));
  ASSERT_OK(
      base->ConfigureFromString("shared={int=10; string=10};"
                                "unique={int=20; string=20};"
                                "pointer={int=30; string=30};",
                                options_));
  ASSERT_OK(base->GetOptionString(options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));

  ConfigOptions prefix = options_;
  prefix.string_mode = ConfigOptions::StringMode::kOptionPrefix;
  ASSERT_OK(
      base->ConfigureFromString("shared={int=11; string=11};"
                                "unique={int=21; string=21};"
                                "pointer={int=31; string=31};",
                                prefix));
  ASSERT_OK(base->GetOptionString(prefix, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, prefix));
  ASSERT_TRUE(base->Matches(copy.get(), prefix));

  ConfigOptions detached = options_;
  detached.string_mode = ConfigOptions::StringMode::kOptionDetached;
  ASSERT_OK(
      base->ConfigureFromString("shared={int=12; string=12};"
                                "unique={int=22; string=22};"
                                "pointer={int=32; string=32};",
                                detached));
  ASSERT_OK(base->GetOptionString(detached, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, detached));
  ASSERT_TRUE(base->Matches(copy.get(), detached));

  ASSERT_OK(
      base->ConfigureFromString("shared={int=13; string=12};"
                                "unique={int=23; string=23};"
                                "pointer={int=33; string=33};",
                                detached));
  detached.string_mode |= ConfigOptions::StringMode::kOptionPrefix;
  ASSERT_OK(base->GetOptionString(detached, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, detached));
  ASSERT_TRUE(base->Matches(copy.get(), detached));
}

TEST_F(ConfigurableTest, GetOptionsTest) {
  std::unique_ptr<Configurable> simple;

  simple.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                          TestConfigMode::kNestedMode));
  int i = 11;
  for (auto opt : {"", "shared.", "unique.", "pointer."}) {
    std::string value;
    std::string expected = ToString(i);
    std::string opt_name = opt;
    ASSERT_OK(simple->ConfigureOption(opt_name + "int", expected, options_));
    ASSERT_OK(simple->GetOption(opt_name + "int", options_, &value));
    ASSERT_EQ(expected, value);
    ASSERT_OK(simple->ConfigureOption(opt_name + "string", expected, options_));
    ASSERT_OK(simple->GetOption(opt_name + "string", options_, &value));
    ASSERT_EQ(expected, value);

    ASSERT_NOK(simple->ConfigureOption(opt_name + "bad", expected, options_));
    ASSERT_NOK(simple->GetOption("bad option", options_, &value));
    ASSERT_TRUE(value.empty());
    i += 11;
  }
}

TEST_F(ConfigurableTest, ConfigureBadOptionsTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  auto* opts = configurable->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_OK(configurable->ConfigureOption("int", "42", options_));
  ASSERT_EQ(opts->i, 42);
  ASSERT_NOK(configurable->ConfigureOption("int", "fred", options_));
  ASSERT_NOK(configurable->ConfigureOption("bool", "fred", options_));
  ASSERT_NOK(configurable->ConfigureFromString("int=33;unused=u", options_));
  ASSERT_EQ(opts->i, 42);
}

TEST_F(ConfigurableTest, InvalidOptionTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  std::unordered_map<std::string, std::string> options_map = {
      {"bad-option", "bad"}};
  ASSERT_NOK(configurable->ConfigureFromMap(options_map, options_));
  ASSERT_NOK(configurable->ConfigureFromString("bad-option=bad", options_));
  ASSERT_NOK(configurable->ConfigureOption("bad-option", "bad", options_));
}

TEST_F(ConfigurableTest, ValidateOptionTest) {
  std::unique_ptr<Configurable> configurable(SimpleConfigurable::Create());
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;
  ASSERT_OK(configurable->ConfigureOption("bool", "false", options_));
  ASSERT_NOK(configurable->ValidateOptions(db_opts, cf_opts));
  ASSERT_OK(configurable->SanitizeOptions(db_opts, cf_opts));
  ASSERT_OK(configurable->ValidateOptions(db_opts, cf_opts));
}

TEST_F(ConfigurableTest, DeprecatedOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo>
      deprecated_option_info = {
          {"deprecated",
           {offsetof(struct TestOptions, b), OptionType::kBoolean,
            OptionVerificationType::kDeprecated, OptionTypeFlags::kNone, 0}}};
  std::unique_ptr<Configurable> orig;
  orig.reset(SimpleConfigurable::Create("simple", &deprecated_option_info));
  auto* opts = orig->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  opts->d = true;
  ASSERT_OK(orig->ConfigureOption("deprecated", "false", options_));
  ASSERT_TRUE(opts->d);
  ASSERT_OK(orig->ConfigureFromString("deprecated=false", options_));
  ASSERT_TRUE(opts->d);
}

TEST_F(ConfigurableTest, AliasOptionsTest) {
  static std::unordered_map<std::string, OptionTypeInfo> alias_option_info = {
      {"bool",
       {offsetof(struct TestOptions, b), OptionType::kBoolean,
        OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
      {"alias",
       {offsetof(struct TestOptions, b), OptionType::kBoolean,
        OptionVerificationType::kAlias, OptionTypeFlags::kNone, 0}}};
  std::unique_ptr<Configurable> orig;
  orig.reset(SimpleConfigurable::Create("simple", &alias_option_info));
  auto* opts = orig->GetOptions<TestOptions>("simple");
  ASSERT_NE(opts, nullptr);
  ASSERT_OK(orig->ConfigureOption("bool", "false", options_));
  ASSERT_FALSE(opts->b);
  ASSERT_OK(orig->ConfigureOption("alias", "true", options_));
  ASSERT_TRUE(opts->b);
  std::string opts_str;
  ASSERT_OK(orig->GetOptionString(options_, &opts_str));
  ASSERT_EQ(opts_str.find("alias"), std::string::npos);

  ASSERT_OK(orig->ConfigureOption("bool", "false", options_));
  ASSERT_FALSE(opts->b);
  ASSERT_OK(orig->GetOption("alias", options_, &opts_str));
  ASSERT_EQ(opts_str, "false");
}

TEST_F(ConfigurableTest, NestedUniqueConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(SimpleConfigurable::Create("outer", &all_option_info,
                                          TestConfigMode::kUniqueMode));
  auto* outer = simple->GetOptions<TestOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->unique, nullptr);
  ASSERT_OK(simple->ConfigureFromString("int=24;string=outer", options_));
  ASSERT_OK(
      simple->ConfigureFromString("unique={int=42;string=nested}", options_));
  auto* inner = outer->unique->GetOptions<TestOptions>("unique");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedSharedConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(SimpleConfigurable::Create("outer", &all_option_info,
                                          TestConfigMode::kSharedMode));
  ASSERT_OK(simple->ConfigureFromString("int=24;string=outer", options_));
  ASSERT_OK(
      simple->ConfigureFromString("shared={int=42;string=nested}", options_));
  auto* outer = simple->GetOptions<TestOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->shared, nullptr);
  auto* inner = outer->shared->GetOptions<TestOptions>("shared");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, NestedRawConfigTest) {
  std::unique_ptr<Configurable> simple;
  simple.reset(SimpleConfigurable::Create("outer", &all_option_info,
                                          TestConfigMode::kRawPtrMode));
  ASSERT_OK(simple->ConfigureFromString("int=24;string=outer", options_));
  ASSERT_OK(
      simple->ConfigureFromString("pointer={int=42;string=nested}", options_));
  auto* outer = simple->GetOptions<TestOptions>("outer");
  ASSERT_NE(outer, nullptr);
  ASSERT_NE(outer->config, nullptr);
  auto* inner = outer->config->GetOptions<TestOptions>("pointer");
  ASSERT_NE(inner, nullptr);
  ASSERT_EQ(outer->i, 24);
  ASSERT_EQ(outer->s, "outer");
  ASSERT_EQ(inner->i, 42);
  ASSERT_EQ(inner->s, "nested");
}

TEST_F(ConfigurableTest, MatchesTest) {
  std::unique_ptr<Configurable> base, copy;
  base.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                        TestConfigMode::kNestedMode));
  copy.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                        TestConfigMode::kNestedMode));
  ASSERT_OK(base->ConfigureFromString(
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}",
      options_));
  ASSERT_OK(copy->ConfigureFromString(
      "int=11;string=outer;unique={int=22;string=u};shared={int=33;string=s}",
      options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));
  ASSERT_OK(base->ConfigureOption("shared", "int=44", options_));
  std::string mismatch;
  ASSERT_FALSE(base->Matches(copy.get(), options_, &mismatch));
  ASSERT_EQ(mismatch, "shared.int");
  std::string c1value, c2value;
  ASSERT_OK(base->GetOption(mismatch, options_, &c1value));
  ASSERT_OK(copy->GetOption(mismatch, options_, &c2value));
  ASSERT_NE(c1value, c2value);
}

TEST_F(ConfigurableTest, MutableOptionsTest) {
  std::unique_ptr<Configurable> base, copy;
  base.reset(SimpleConfigurable::Create(
      "simple", &all_option_info,
      TestConfigMode::kMutableMode | TestConfigMode::kUniqueMode));
  std::string opt_str;
  ASSERT_OK(
      base->ConfigureOption("int", "24", options_));  // This one is mutable
  ASSERT_NOK(
      base->ConfigureOption("string", "s", options_));  // This one is not
  ASSERT_OK(base->ConfigureOption("unique", "int=42;string=nested", options_));
  ASSERT_OK(base->GetOption("int", options_,
                            &opt_str));  // Can get the mutable option
  ASSERT_NOK(
      base->GetOption("string", options_, &opt_str));  // Cannot get this one
  ASSERT_OK(base->GetOptionString(options_, &opt_str));
  ASSERT_EQ(opt_str.find("test.outer.string"), std::string::npos);
  copy.reset(SimpleConfigurable::Create(
      "simple", &all_option_info,
      TestConfigMode::kMutableMode | TestConfigMode::kUniqueMode));
  ASSERT_OK(copy->ConfigureFromString(opt_str, options_));
  std::string mismatch;
  ASSERT_TRUE(base->Matches(copy.get(), options_));

  copy.reset(SimpleConfigurable::Create("simple", &all_option_info,
                                        TestConfigMode::kUniqueMode));
  ASSERT_OK(copy->ConfigureFromString(opt_str, options_));
  ASSERT_OK(copy->ConfigureOption("string", "s", options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));
  copy->Matches(base.get(), options_, &mismatch);
  ASSERT_FALSE(copy->Matches(base.get(), options_));

  ConfigOptions prefix = options_;
  prefix.string_mode = ConfigOptions::StringMode::kOptionPrefix;
  ASSERT_OK(copy->GetOptionString(prefix, &opt_str));
  ASSERT_NOK(base->ConfigureFromString(opt_str, prefix));
  prefix.ignore_unknown_options = true;
  ASSERT_OK(base->ConfigureFromString(opt_str, prefix));
  ASSERT_TRUE(base->Matches(copy.get(), prefix));
}

TEST_F(ConfigurableTest, ConfigureStructTest) {
  std::unique_ptr<Configurable> base(StructConfigurable::Create());
  std::unique_ptr<Configurable> copy(StructConfigurable::Create());
  std::string opt_str, value;
  std::unordered_set<std::string> names;

  ASSERT_OK(base->ConfigureFromString("struct={int=10; string=10}", options_));
  ASSERT_OK(base->GetOptionString(options_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));
  ASSERT_OK(base->GetOptionNames(options_, &names));
  ASSERT_EQ(names.size(), 1);
  ASSERT_EQ(*(names.begin()), "struct");
  ASSERT_OK(base->ConfigureFromString("struct={int=20; string=20}", options_));
  ASSERT_OK(base->GetOption("struct", options_, &value));
  ASSERT_OK(copy->ConfigureOption("struct", value, options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));

  ASSERT_NOK(base->ConfigureFromString("struct={int=10; string=10; bad=11}",
                                       options_));
  ASSERT_OK(base->ConfigureOption("struct.int", "42", options_));
  ASSERT_NOK(base->ConfigureOption("struct.bad", "42", options_));
  ASSERT_NOK(base->GetOption("struct.bad", options_, &value));
  ASSERT_OK(base->GetOption("struct.int", options_, &value));
  ASSERT_EQ(value, "42");
  ConfigOptions detached = options_;
  detached.string_mode = ConfigOptions::StringMode::kOptionDetached;
  ASSERT_OK(base->GetOptionString(detached, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, detached));
  ASSERT_TRUE(base->Matches(copy.get(), detached));
}

TEST_F(ConfigurableTest, ConfigurableEnumTest) {
  std::unique_ptr<Configurable> base, copy;
  base.reset(SimpleConfigurable::Create("e", &enum_option_info));
  copy.reset(SimpleConfigurable::Create("e", &enum_option_info));

  std::string opts_str;

  ASSERT_OK(base->ConfigureFromString("enum=B", options_));
  ASSERT_FALSE(base->Matches(copy.get(), options_));
  ASSERT_OK(base->GetOptionString(options_, &opts_str));
  ASSERT_OK(copy->ConfigureFromString(opts_str, options_));
  ASSERT_TRUE(base->Matches(copy.get(), options_));
  ASSERT_NOK(base->ConfigureOption("enum", "bad", options_));
  ASSERT_NOK(base->ConfigureOption("unknown", "bad", options_));
}

void ConfigurableParamTest::TestConfigureOptions(const ConfigOptions& opts) {
  std::unique_ptr<Configurable> base, copy;
  std::unordered_set<std::string> names;
  std::string opt_str;

  base.reset(factory_());
  copy.reset(factory_());

  ASSERT_OK(base->ConfigureFromString(configuration_, opts));

  ASSERT_OK(base->GetOptionString(opts, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, opts));
  ASSERT_OK(copy->GetOptionString(opts, &opt_str));
  ASSERT_TRUE(base->Matches(copy.get(), opts));

  copy.reset(factory_());
  ASSERT_OK(base->GetOptionNames(opts, &names));
  std::unordered_map<std::string, std::string> unused;
  bool found_one = false;
  for (auto name : names) {
    std::string value;
    ASSERT_OK(base->GetOption(name, opts, &value));
    if (copy->ConfigureOption(name, value, opts).ok()) {
      found_one = true;
    } else {
      unused[name] = value;
    }
  }
  ASSERT_TRUE(found_one || names.empty());
  while (found_one && !unused.empty()) {
    found_one = false;
    for (auto iter = unused.begin(); iter != unused.end();) {
      if (copy->ConfigureOption(iter->first, iter->second, opts).ok()) {
        found_one = true;
        iter = unused.erase(iter);
      } else {
        ++iter;
      }
    }
  }
  ASSERT_EQ(0, unused.size());
  std::string mismatch;
  ASSERT_TRUE(base->Matches(copy.get(), opts, &mismatch));
}

TEST_P(ConfigurableParamTest, GetDefaultOptionsTest) {
  TestConfigureOptions(options_);
}

TEST_P(ConfigurableParamTest, ConfigureDetachedOptionsTest) {
  ConfigOptions opts = options_;
  opts.string_mode = ConfigOptions::StringMode::kOptionDetached;
  TestConfigureOptions(opts);
}

TEST_P(ConfigurableParamTest, ConfigurePrefixedOptionsTest) {
  ConfigOptions opts = options_;
  opts.string_mode = ConfigOptions::StringMode::kOptionPrefix;
  TestConfigureOptions(opts);
}

TEST_P(ConfigurableParamTest, ConfigurePrefixDetachedOptionsTest) {
  ConfigOptions opts = options_;
  opts.string_mode = (ConfigOptions::StringMode::kOptionPrefix |
                      ConfigOptions::StringMode::kOptionDetached);
  TestConfigureOptions(opts);
}

TEST_P(ConfigurableParamTest, ConfigureFromPropsTest) {
  std::string opt_str;
  std::unordered_set<std::string> names;
  std::unique_ptr<Configurable> copy(factory_());

  ASSERT_OK(object_->ConfigureFromString(configuration_, options_));
  ConfigOptions prefix = options_;
  prefix.string_mode = (ConfigOptions::StringMode::kOptionPrefix |
                        ConfigOptions::StringMode::kOptionDetached);
  prefix.delimiter = "\n";
  ASSERT_OK(object_->GetOptionString(prefix, &opt_str));
  std::istringstream iss(opt_str);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    copy_map[name] = value;
  }
  ASSERT_OK(copy->ConfigureFromMap(copy_map, prefix));
  ASSERT_TRUE(object_->Matches(copy.get(), prefix));
}

TEST_P(ConfigurableParamTest, DumpOptionsTest) {
  std::string opt_str;
  StringLogger logger;
  ConfigOptions cfg = options_;
  ASSERT_OK(object_->ConfigureFromString(configuration_, cfg));
  object_->Dump(&logger, cfg);
  logger.clear();
  cfg.string_mode = ConfigOptions::StringMode::kOptionDetached;
  object_->Dump(&logger, cfg);
  logger.clear();
  object_->Dump(&logger, cfg);
  logger.clear();
  cfg.string_mode = (ConfigOptions::StringMode::kOptionDetached |
                     ConfigOptions::StringMode::kOptionPrefix);
  object_->Dump(&logger, cfg);
}
static Configurable* SimpleFactory() {
  return SimpleConfigurable::Create("simple");
}

static Configurable* UniqueFactory() {
  return InheritedConfigurable::Create("simple", TestConfigMode::kUniqueMode);
}
static Configurable* SharedFactory() {
  return InheritedConfigurable::Create("simple", TestConfigMode::kSharedMode);
}

static Configurable* NestedFactory() {
  return InheritedConfigurable::Create("simple", TestConfigMode::kNestedMode);
}

static Configurable* MutableFactory() {
  return SimpleConfigurable::Create(
      "simple", &all_option_info,
      TestConfigMode::kMutableMode | TestConfigMode::kUniqueMode);
}
static Configurable* StructFactory() { return StructConfigurable::Create(); }

static Configurable* MutableStructFactory() {
  return StructConfigurable::Create(
      "simple", &all_option_info,
      TestConfigMode::kMutableMode | TestConfigMode::kUniqueMode);
}

static Configurable* ThreeDeepFactory() {
  Configurable* simple = SimpleConfigurable::Create(
      "simple", &all_option_info, TestConfigMode::kDefaultMode);
  auto* options = simple->GetOptions<TestOptions>("simple");
  options->unique.reset(
      InheritedConfigurable::Create("inner", TestConfigMode::kNestedMode));
  return simple;
}

static Configurable* CustomTableFactory() {
  return InheritedConfigurable::Create("simple", TestConfigMode::kDefaultMode,
                                       &custom_option_info);
}

INSTANTIATE_TEST_CASE_P(
    ParamTest, ConfigurableParamTest,
    testing::Values(std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=42;bool=true;string=s", SimpleFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=42;unique={int=33;string=unique}", MutableFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "struct={int=33;bool=true;string=s;}", StructFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "struct={int=42;unique={int=33;string=unique}}",
                        MutableStructFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=33;bool=true;string=outer;"
                        "shared={int=42;string=shared}",
                        SharedFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=33;bool=true;string=outer;"
                        "unique={int=42;string=unique}",
                        UniqueFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=11;bool=true;string=outer;"
                        "pointer={int=22;string=pointer};"
                        "unique={int=33;string=unique};"
                        "shared={int=44;string=shared}",
                        NestedFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=11;bool=true;string=outer;"
                        "unique={int=22;string=inner;"
                        "unique={int=33;string=unique}};",
                        ThreeDeepFactory),
                    std::pair<std::string, ConfigTestFactoryFunc>(
                        "int=11;bool=true;string=outer;"
                        "shared={id=PlainTable}",
                        CustomTableFactory)));
#endif  // ROCKSDB_LITE

}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
