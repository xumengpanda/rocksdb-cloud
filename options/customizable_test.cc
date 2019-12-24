//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/customizable.h"

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "options/customizable_helper.h"
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

class TestCustomizable : public Customizable {
 public:
  TestCustomizable(const std::string& name)
      : name_(name), prefix_(kDefaultPrefix + Type() + "." + name_ + ".") {}
  const char* Name() const override { return name_.c_str(); }
  const std::string& GetOptionsPrefix() const override { return prefix_; }
  static const char* Type() { return "test.custom"; }
  static Status CreateFromString(const std::string& value,
                                 const ConfigOptions& opts,
                                 std::unique_ptr<TestCustomizable>* result);
  static Status CreateFromString(const std::string& value,
                                 const ConfigOptions& opts,
                                 std::shared_ptr<TestCustomizable>* result);
  static Status CreateFromString(const std::string& value,
                                 const ConfigOptions& opts,
                                 TestCustomizable** result);
protected:
  const std::string name_;
private:
  const std::string prefix_;
};

struct AOptions {
  int i = 0;
  bool b = false;
};

static OptionTypeMap a_option_info = {
#ifndef ROCKSDB_LITE
    {"int",
     {offsetof(struct AOptions, i), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bool",
     {offsetof(struct AOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};
class ACustomizable : public TestCustomizable {
 public:
  ACustomizable(const std::string& id) : TestCustomizable("A"), id_(id) {
    RegisterOptions("A", &opts_, &a_option_info);
  }
  std::string GetId() const override { return id_; }
 private:
  AOptions opts_;
  const std::string id_;
};

#ifndef ROCKSDB_LITE
static int A_count = 0;
const FactoryFunc<TestCustomizable>& a_func =
    ObjectLibrary::Default()->Register<TestCustomizable>(
        "A.*",
        [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
           std::string* /* msg */) {
          guard->reset(new ACustomizable(name));
          A_count++;
          return guard->get();
        });
#endif  // ROCKSDB_LITE

struct BOptions {
  std::string s;
  bool b = false;
};

static OptionTypeMap b_option_info = {
#ifndef ROCKSDB_LITE
    {"string",
     {offsetof(struct BOptions, s), OptionType::kString,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bool",
     {offsetof(struct BOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

class BCustomizable : public TestCustomizable {
 private:
 public:
  BCustomizable(const std::string& name) : TestCustomizable(name) {
    RegisterOptions(name, &opts_, &b_option_info);
  }

 private:
  BOptions opts_;
};

static bool LoadSharedB(const std::string& id,
                        std::shared_ptr<TestCustomizable>* result) {
  if (id == "B") {
    result->reset(new BCustomizable(id));
    return true;
  } else {
    return false;
  }
}
Status TestCustomizable::CreateFromString(
              const std::string& value,
              const ConfigOptions& opts,
              std::shared_ptr<TestCustomizable>* result) {
  return LoadSharedObject<TestCustomizable>(value, LoadSharedB, opts, result);
}
  
Status TestCustomizable::CreateFromString(
              const std::string& value,
              const ConfigOptions& opts,
              std::unique_ptr<TestCustomizable>* result) {
  return LoadUniqueObject<TestCustomizable>(
             value,
              [](const std::string&id,
                 std::unique_ptr<TestCustomizable> *u) {
                if (id == "B") {
                  u->reset(new BCustomizable(id));
                  return true;
                } else {
                  return false;
                }
              }, opts, result);
}
  
Status TestCustomizable::CreateFromString(
              const std::string& value,
              const ConfigOptions& opts,
              TestCustomizable** result) {
  return LoadStaticObject<TestCustomizable>(
              value,
              [](const std::string&id,
                 TestCustomizable** ptr) {
                if (id == "B") {
                  *ptr = new BCustomizable(id);
                  return true;
                } else {
                  return false;
                }
              }, opts, result);
}

  
#ifndef ROCKSDB_LITE
const FactoryFunc<TestCustomizable>& s_func =
    ObjectLibrary::Default()->Register<TestCustomizable>(
        "S", [](const std::string& name,
                std::unique_ptr<TestCustomizable>* /* guard */,
                std::string* /* msg */) { return new BCustomizable(name); });
#endif  // ROCKSDB_LITE

struct SimpleOptions {
  bool b = true;
  std::unique_ptr<TestCustomizable> cu;
  std::shared_ptr<TestCustomizable> cs;
  TestCustomizable* cp = nullptr;
};

static OptionTypeMap simple_option_info = {
#ifndef ROCKSDB_LITE
    {"bool",
     {offsetof(struct SimpleOptions, b), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"unique",
     OptionTypeInfo::AsCustomU<TestCustomizable>(offsetof(struct SimpleOptions, cu),
                                                 OptionVerificationType::kNormal)
    }, 
    {"shared",
     OptionTypeInfo::AsCustomS<TestCustomizable>(offsetof(struct SimpleOptions, cs),
                                                 OptionVerificationType::kNormal)
    }, 
    {"pointer",
     OptionTypeInfo::AsCustomP<TestCustomizable>(offsetof(struct SimpleOptions, cp),
                                                 OptionVerificationType::kNormal)
    },
#endif  // ROCKSDB_LITE
};


class SimpleConfigurable : public Configurable {
 private:
  SimpleOptions simple_;
  const std::string prefix_;

 public:
  SimpleConfigurable() : prefix_("rocksdb.test.simple.") {
    RegisterOptions("simple", &simple_, &simple_option_info);
  }

 protected:
  const std::string& GetOptionsPrefix() const override { return prefix_; }
};

class CustomizableTest : public testing::Test {
 public:
  ConfigOptions cfgopts_;
};

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE
// Tests that a Customizable can be created by:
//    - a simple name
//    - a XXX.id option
//    - a property with a name
TEST_F(CustomizableTest, CreateByNameTest) {
  ObjectLibrary::Default()->Register<TestCustomizable>(
      "TEST.*",
      [](const std::string& name, std::unique_ptr<TestCustomizable>* guard,
         std::string* /* msg */) {
        guard->reset(new TestCustomizable(name));
        return guard->get();
      });
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_OK(configurable->ConfigureFromString("unique={id=TEST_1}", cfgopts_));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_1");
  ASSERT_OK(configurable->ConfigureFromString("unique.id=TEST_2", cfgopts_));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_2");
  ASSERT_OK(configurable->ConfigureFromString("unique=TEST_3", cfgopts_));
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "TEST_3");
}

TEST_F(CustomizableTest, ToStringTest) {
  std::unique_ptr<TestCustomizable> custom(new TestCustomizable("test"));
  ASSERT_EQ(custom->ToString(cfgopts_), "test");
  ConfigOptions detached = cfgopts_;
  detached.string_mode = ConfigOptions::StringMode::kOptionDetached;
  ASSERT_EQ(custom->ToString(detached), "test");
}

TEST_F(CustomizableTest, SimpleConfigureTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=B;string=s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(opt_map, cfgopts_));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  ASSERT_OK(configurable->GetOptionString(cfgopts_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(opt_str, cfgopts_));
  ASSERT_TRUE(configurable->Matches(copy.get(), cfgopts_));
}

static void GetMapFromProperties(
    const std::string& props,
    std::unordered_map<std::string, std::string>* map) {
  std::istringstream iss(props);
  std::unordered_map<std::string, std::string> copy_map;
  std::string line;
  map->clear();
  for (int line_num = 0; std::getline(iss, line); line_num++) {
    std::string name;
    std::string value;
    ASSERT_OK(
        RocksDBOptionsParser::ParseStatement(&name, &value, line, line_num));
    (*map)[name] = value;
  }
}

TEST_F(CustomizableTest, ConfigureFromPropsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"},
      {"unique.A.int", "1"},
      {"unique.A.bool", "true"},
      {"shared.id", "B"},
      {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(opt_map, cfgopts_));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::vector<uint32_t> modes = {
      ConfigOptions::StringMode::kOptionNone,
      ConfigOptions::StringMode::kOptionDetached,
      (ConfigOptions::StringMode::kOptionDetached |
       ConfigOptions::StringMode::kOptionPrefix),
  };
  for (uint32_t mode : modes) {
    std::string opt_str;
    ConfigOptions opts = cfgopts_;
    opts.string_mode = mode;
    opts.delimiter = "\n";
    std::unordered_map<std::string, std::string> props;
    ASSERT_OK(configurable->GetOptionString(opts, &opt_str));
    GetMapFromProperties(opt_str, &props);
    std::unique_ptr<Configurable> copy(new SimpleConfigurable());
    ASSERT_OK(copy->ConfigureFromMap(props, opts));
    ASSERT_TRUE(configurable->Matches(copy.get(), opts));
  }
}

TEST_F(CustomizableTest, ConfigureFromShortTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
  };
  std::unique_ptr<Configurable> configurable(new SimpleConfigurable());
  ASSERT_OK(configurable->ConfigureFromMap(opt_map, cfgopts_));
  SimpleOptions* simple = configurable->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), "A");
  std::string opt_str;
  ConfigOptions detached = cfgopts_;
  detached.string_mode = ConfigOptions::StringMode::kOptionDetached;
  ASSERT_OK(configurable->GetOptionString(detached, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(opt_str, detached));
  ASSERT_TRUE(configurable->Matches(copy.get(), detached));
}

TEST_F(CustomizableTest, MatchesOptionsTest) {
  std::unordered_map<std::string, std::string> opt_map = {
      {"unique", "id=A;int=1;bool=true"},
      {"shared", "id=A;int=1;bool=true"},
  };
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  std::unique_ptr<Configurable> c2(new SimpleConfigurable());
  ASSERT_OK(c1->ConfigureFromMap(opt_map, cfgopts_));
  ASSERT_OK(c2->ConfigureFromMap(opt_map, cfgopts_));
  ASSERT_TRUE(
      c1->Matches(c2.get(), cfgopts_));
  SimpleOptions* simple = c1->GetOptions<SimpleOptions>("simple");
  ASSERT_TRUE(simple->cu->Matches(simple->cs.get(), cfgopts_));
  ASSERT_OK(simple->cu->ConfigureOption("int", "2", cfgopts_));
  ASSERT_FALSE(simple->cu->Matches(simple->cs.get(), cfgopts_));
  ASSERT_FALSE(c1->Matches(c2.get(), cfgopts_));
  ConfigOptions loosely = cfgopts_;
  loosely.sanity_level = OptionsSanityCheckLevel::kSanityLevelLooselyCompatible;
  ASSERT_TRUE(c1->Matches(c2.get(), loosely));
  ASSERT_TRUE(simple->cu->Matches(simple->cs.get(), loosely));

  ASSERT_OK(c1->ConfigureOption("shared", "id=B;string=3", cfgopts_));
  ASSERT_TRUE(c1->Matches(c2.get(), loosely));
  ASSERT_FALSE(c1->Matches(c2.get(), cfgopts_));
  ASSERT_FALSE(simple->cs->Matches(simple->cu.get(), loosely));
  simple->cs.reset();
  ASSERT_TRUE(c1->Matches(c2.get(), loosely));
  ASSERT_FALSE(c1->Matches(c2.get(), cfgopts_));
}

// Tests that we can initialize a customizable from its options
TEST_F(CustomizableTest, ConfigureStandaloneCustomTest) {
  std::unique_ptr<TestCustomizable> base, copy;
  ASSERT_OK(cfgopts_.registry->NewUniqueObject<TestCustomizable>("A", &base));
  ASSERT_OK(cfgopts_.registry->NewUniqueObject<TestCustomizable>("A", &copy));
  ASSERT_OK(base->ConfigureFromString("int=33;bool=true", cfgopts_));
  std::string opt_str;
  ASSERT_OK(base->GetOptionString(cfgopts_, &opt_str));
  ASSERT_OK(copy->ConfigureFromString(opt_str, cfgopts_));
  ASSERT_TRUE(base->Matches(copy.get(),cfgopts_));
}

// Tests that we fail appropriately if the pattern is not registered
TEST_F(CustomizableTest, BadNameTest) {
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  Status s = c1->ConfigureFromString("unique.shared.id=bad name", cfgopts_);
  ASSERT_NOK(c1->ConfigureFromString("unique.shared.id=bad name", cfgopts_));
  ConfigOptions ignore = cfgopts_;
  ignore.ignore_unknown_options = true;
  ASSERT_OK(c1->ConfigureFromString("unique.shared.id=bad name", ignore));
}

// Tests that we fail appropriately if a bad option is passed to the underlying
// configurable
TEST_F(CustomizableTest, BadOptionTest) {
  std::unique_ptr<Configurable> c1(new SimpleConfigurable());
  ConfigOptions ignore = cfgopts_;
  ignore.ignore_unknown_options = true;

  ASSERT_NOK(c1->ConfigureFromString("A.int=11", cfgopts_));
  ASSERT_NOK(c1->ConfigureFromString("shared={id=B;int=1}", cfgopts_));
  ASSERT_OK(
      c1->ConfigureFromString("shared={id=A;string=s}", ignore));
  ASSERT_NOK(c1->ConfigureFromString("B.int=11", cfgopts_));
  ASSERT_OK(c1->ConfigureFromString("B.int=11", ignore));
  ASSERT_NOK(c1->ConfigureFromString("A.string=s", cfgopts_));
  ASSERT_OK(c1->ConfigureFromString("A.string=s", ignore));
  // Test as detached
  ASSERT_NOK(c1->ConfigureFromString("shared.id=A;A.string=b}", cfgopts_));
  ASSERT_OK(c1->ConfigureFromString("shared.id=A;A.string=s}", ignore));
}

// Tests that different IDs lead to different objects
TEST_F(CustomizableTest, UniqueIdTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  ASSERT_OK(base->ConfigureFromString("unique={id=A_1;int=1;bool=true}",
                                      cfgopts_));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), std::string("A_1"));
  std::string opt_str;
  ASSERT_OK(base->GetOptionString(cfgopts_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(opt_str, cfgopts_));
  ASSERT_TRUE(base->Matches(copy.get(), cfgopts_));
  ASSERT_OK(base->ConfigureFromString("unique={id=A_2;int=1;bool=true}",
                                      cfgopts_));
  ASSERT_FALSE(base->Matches(copy.get(), cfgopts_));
  ASSERT_EQ(simple->cu->GetId(), std::string("A_2"));
}

// Tests that we only get a new customizable when it changes
TEST_F(CustomizableTest, NewCustomizableTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  A_count = 0;
  ASSERT_OK(base->ConfigureFromString("unique={id=A_1;int=1;bool=true}",
                                      cfgopts_));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(A_count, 1);  // Created one A
  ASSERT_OK(base->ConfigureFromString("unique={id=A_1;int=1;bool=false}",
                                      cfgopts_));
  ASSERT_EQ(A_count, 1);  // Still only created one A
  ASSERT_OK(base->ConfigureFromString("unique={id=A_2;int=1;bool=false}",
                                      cfgopts_));
  ASSERT_EQ(A_count, 2);  // Created another A
  ASSERT_OK(base->ConfigureFromString("unique=", cfgopts_));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 2);
}

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
