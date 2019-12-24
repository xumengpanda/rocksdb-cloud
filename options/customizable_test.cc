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

#include "cache/lru_cache.h"
#include "db/db_test_util.h"
#include "monitoring/statistics.h"
#include "options/customizable_helper.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "rocksdb/convenience.h"
#include "rocksdb/statistics.h"
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
    const std::string& value, const ConfigOptions& opts,
    std::shared_ptr<TestCustomizable>* result) {
  return LoadSharedObject<TestCustomizable>(value, LoadSharedB, opts, result);
}

Status TestCustomizable::CreateFromString(
    const std::string& value, const ConfigOptions& opts,
    std::unique_ptr<TestCustomizable>* result) {
  return LoadUniqueObject<TestCustomizable>(
      value,
      [](const std::string& id, std::unique_ptr<TestCustomizable>* u) {
        if (id == "B") {
          u->reset(new BCustomizable(id));
          return true;
        } else {
          return false;
        }
      },
      opts, result);
}

Status TestCustomizable::CreateFromString(const std::string& value,
                                          const ConfigOptions& opts,
                                          TestCustomizable** result) {
  return LoadStaticObject<TestCustomizable>(
      value,
      [](const std::string& id, TestCustomizable** ptr) {
        if (id == "B") {
          *ptr = new BCustomizable(id);
          return true;
        } else {
          return false;
        }
      },
      opts, result);
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
     OptionTypeInfo::AsCustomU<TestCustomizable>(
         offsetof(struct SimpleOptions, cu), OptionVerificationType::kNormal)},
    {"shared",
     OptionTypeInfo::AsCustomS<TestCustomizable>(
         offsetof(struct SimpleOptions, cs), OptionVerificationType::kNormal)},
    {"pointer",
     OptionTypeInfo::AsCustomP<TestCustomizable>(
         offsetof(struct SimpleOptions, cp), OptionVerificationType::kNormal)},
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
      {"unique.id", "A"}, {"unique.A.int", "1"},    {"unique.A.bool", "true"},
      {"shared.id", "B"}, {"shared.B.string", "s"},
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
  ASSERT_TRUE(c1->Matches(c2.get(), cfgopts_));
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
  ASSERT_TRUE(base->Matches(copy.get(), cfgopts_));
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
  ASSERT_OK(c1->ConfigureFromString("shared={id=A;string=s}", ignore));
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
  ASSERT_OK(
      base->ConfigureFromString("unique={id=A_1;int=1;bool=true}", cfgopts_));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(simple->cu->GetId(), std::string("A_1"));
  std::string opt_str;
  ASSERT_OK(base->GetOptionString(cfgopts_, &opt_str));
  std::unique_ptr<Configurable> copy(new SimpleConfigurable());
  ASSERT_OK(copy->ConfigureFromString(opt_str, cfgopts_));
  ASSERT_TRUE(base->Matches(copy.get(), cfgopts_));
  ASSERT_OK(
      base->ConfigureFromString("unique={id=A_2;int=1;bool=true}", cfgopts_));
  ASSERT_FALSE(base->Matches(copy.get(), cfgopts_));
  ASSERT_EQ(simple->cu->GetId(), std::string("A_2"));
}

// Tests that we only get a new customizable when it changes
TEST_F(CustomizableTest, NewCustomizableTest) {
  std::unique_ptr<Configurable> base(new SimpleConfigurable());
  A_count = 0;
  ASSERT_OK(
      base->ConfigureFromString("unique={id=A_1;int=1;bool=true}", cfgopts_));
  SimpleOptions* simple = base->GetOptions<SimpleOptions>("simple");
  ASSERT_NE(simple, nullptr);
  ASSERT_NE(simple->cu, nullptr);
  ASSERT_EQ(A_count, 1);  // Created one A
  ASSERT_OK(
      base->ConfigureFromString("unique={id=A_1;int=1;bool=false}", cfgopts_));
  ASSERT_EQ(A_count, 1);  // Still only created one A
  ASSERT_OK(
      base->ConfigureFromString("unique={id=A_2;int=1;bool=false}", cfgopts_));
  ASSERT_EQ(A_count, 2);  // Created another A
  ASSERT_OK(base->ConfigureFromString("unique=", cfgopts_));
  ASSERT_EQ(simple->cu, nullptr);
  ASSERT_EQ(A_count, 2);
}

class MockFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  const char* Name() const override { return "Test"; }
  virtual FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions&, const BlockBuilder&) const override {
    return nullptr;
  }
};

class MockFilterPolicy : public FilterPolicy {
 public:
  const char* Name() const override { return "Test"; }
  void CreateFilter(const Slice*, int, std::string*) const override {}

  bool KeyMayMatch(const Slice&, const Slice&) const override { return false; }
};

class MockCache : public LRUCache {
 public:
  MockCache(const LRUCacheOptions& options = LRUCacheOptions())
      : LRUCache(options) {}
  const char* Name() const override { return "Test"; }
};

class TestStatistics : public StatisticsImpl {
 public:
  TestStatistics() : StatisticsImpl(nullptr) {}
  const char* Name() const override { return "Test"; }
};

extern "C" {
void RegisterLocalTests(ObjectLibrary& library, const std::string& /*arg*/) {
  library.Register<TableFactory>(
      "MockTable",
      [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new mock::MockTableFactory());
        return guard->get();
      });
  library.Register<MemTableRepFactory>(
      "SpecialSkipListFactory",
      [](const std::string& /*uri*/, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new SpecialSkipListFactory(100));
        return guard->get();
      });
  library.Register<FlushBlockPolicyFactory>(
      "Test", [](const std::string& /*uri*/,
                 std::unique_ptr<FlushBlockPolicyFactory>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new MockFlushBlockPolicyFactory());
        return guard->get();
      });
  library.Register<const Comparator>(
      "SimpleSuffixReverseComparator",
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard */,
         std::string* /* errmsg */) {
        return new test::SimpleSuffixReverseComparator();
      });
  library.Register<MergeOperator>(
      "ChanglingMergeOperator",
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new test::ChanglingMergeOperator("Changling"));
        return guard->get();
      });
  library.Register<const CompactionFilter>(
      "ChanglingCompactionFilter",
      [](const std::string& /*uri*/,
         std::unique_ptr<const CompactionFilter>* /*guard*/,
         std::string* /* errmsg */) {
        return new test::ChanglingCompactionFilter("Changling");
      });
  library.Register<CompactionFilterFactory>(
      "ChanglingCompactionFilterFactory",
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilterFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new test::ChanglingCompactionFilterFactory("Changling"));
        return guard->get();
      });
  library.Register<FilterPolicy>(
      "Test",
      [](const std::string& /*uri*/, std::unique_ptr<FilterPolicy>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockFilterPolicy());
        return guard->get();
      });
  library.Register<Cache>(
      "Test", [](const std::string& /*uri*/, std::unique_ptr<Cache>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new MockCache());
        return guard->get();
      });
  library.Register<Statistics>(
      "Test", [](const std::string& /*uri*/, std::unique_ptr<Statistics>* guard,
                 std::string* /* errmsg */) {
        guard->reset(new TestStatistics());
        return guard->get();
      });
}
}  // end extern "C"

class LoadCustomizableTest : public testing::Test {
 public:
  LoadCustomizableTest() { cfg_opts_.registry = db_opts_.object_registry; }
  std::shared_ptr<ObjectLibrary> RegisterTests(const std::string& arg) {
    auto library = db_opts_.object_registry->AddLocalLibrary(arg);
    library->Register(RegisterLocalTests, arg);
    return library;
  };

 protected:
  DBOptions db_opts_;
  ColumnFamilyOptions cf_opts_;
  ConfigOptions cfg_opts_;
};

TEST_F(LoadCustomizableTest, LoadTableMockFactory) {
  std::shared_ptr<TableFactory> factory;
  ASSERT_NOK(TableFactory::CreateFromString("MockTable", cfg_opts_, &factory));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "table_factory={id=MockTable}", cfg_opts_, &cf_opts_));
  RegisterTests("test");
  ASSERT_OK(TableFactory::CreateFromString("MockTable", cfg_opts_, &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_EQ(factory->Name(), std::string("MockTable"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "table_factory={id=MockTable}", cfg_opts_, &cf_opts_));
  ASSERT_NE(cf_opts_.table_factory, nullptr);
  ASSERT_EQ(cf_opts_.table_factory->Name(), std::string("MockTable"));
}

TEST_F(LoadCustomizableTest, LoadMemTableRepFactoryTest) {
  std::shared_ptr<MemTableRepFactory> mtrf;
  ASSERT_NOK(MemTableRepFactory::CreateFromString("SpecialSkipListFactory",
                                                  cfg_opts_, &mtrf));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "memtable_factory={id=SpecialSkipListFactory}", cfg_opts_,
      &cf_opts_));
  RegisterTests("test");
  ASSERT_OK(MemTableRepFactory::CreateFromString("SpecialSkipListFactory",
                                                 cfg_opts_, &mtrf));
  ASSERT_NE(mtrf, nullptr);
  ASSERT_EQ(mtrf->Name(), std::string("SpecialSkipListFactory"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "memtable_factory={id=SpecialSkipListFactory}", cfg_opts_,
      &cf_opts_));
  ASSERT_NE(cf_opts_.memtable_factory, nullptr);
  ASSERT_EQ(cf_opts_.memtable_factory->Name(),
            std::string("SpecialSkipListFactory"));
}

TEST_F(LoadCustomizableTest, LoadFlushBlockPolicyTest) {
  std::shared_ptr<FlushBlockPolicyFactory> factory;
  ASSERT_NOK(
      FlushBlockPolicyFactory::CreateFromString("Test", cfg_opts_, &factory));
  ASSERT_OK(FlushBlockPolicyFactory::CreateFromString(
      "FlushBlockBySizePolicyFactory", cfg_opts_, &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_EQ(factory->Name(), std::string("FlushBlockBySizePolicyFactory"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "block_based_table_factory={flush_block_policy_factory={id=Test}}",
      cfg_opts_, &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "block_based_table_factory={flush_block_policy_factory={id="
      "FlushBlockBySizePolicyFactory}}",
      cfg_opts_, &cf_opts_));
  auto const* options =
      cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
          TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->flush_block_policy_factory, nullptr);
  ASSERT_EQ(options->flush_block_policy_factory->Name(),
            std::string("FlushBlockBySizePolicyFactory"));
  RegisterTests("test");
  ASSERT_OK(
      FlushBlockPolicyFactory::CreateFromString("Test", cfg_opts_, &factory));
  ASSERT_NE(factory, nullptr);
  ASSERT_EQ(factory->Name(), std::string("Test"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "block_based_table_factory={flush_block_policy_factory={id=Test}}",
      cfg_opts_, &cf_opts_));
  options = cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
      TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->flush_block_policy_factory, nullptr);
  ASSERT_EQ(options->flush_block_policy_factory->Name(), std::string("Test"));
}

TEST_F(LoadCustomizableTest, LoadComparatorTest) {
  const Comparator* comparator = nullptr;
  ASSERT_NOK(Comparator::CreateFromString("SimpleSuffixReverseComparator",
                                          cfg_opts_, &comparator));
  ASSERT_OK(Comparator::CreateFromString("leveldb.BytewiseComparator",
                                         cfg_opts_, &comparator));
  ASSERT_NE(comparator, nullptr);
  ASSERT_EQ(comparator->Name(), std::string("leveldb.BytewiseComparator"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "comparator={id=SimpleSuffixReverseComparator}", cfg_opts_,
      &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "comparator={id=leveldb.BytewiseComparator}", cfg_opts_,
      &cf_opts_));
  RegisterTests("test");
  ASSERT_OK(Comparator::CreateFromString("SimpleSuffixReverseComparator",
                                         cfg_opts_, &comparator));
  ASSERT_NE(comparator, nullptr);
  ASSERT_EQ(comparator->Name(), std::string("SimpleSuffixReverseComparator"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "comparator={id=SimpleSuffixReverseComparator}", cfg_opts_,
      &cf_opts_));
  ASSERT_NE(cf_opts_.comparator, nullptr);
  ASSERT_EQ(cf_opts_.comparator->Name(),
            std::string("SimpleSuffixReverseComparator"));
}

TEST_F(LoadCustomizableTest, LoadMergeOperatorTest) {
  std::shared_ptr<MergeOperator> op;
  ASSERT_NOK(MergeOperator::CreateFromString("ChanglingMergeOperator",
                                             cfg_opts_, &op));
  ASSERT_OK(MergeOperator::CreateFromString("max", cfg_opts_, &op));
  ASSERT_NE(op, nullptr);
  ASSERT_EQ(op->Name(), std::string("MaxOperator"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "merge_operator={id=ChanglingMergeOperator}", cfg_opts_,
      &cf_opts_));
  ASSERT_EQ(cf_opts_.merge_operator, nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "merge_operator={id=max}", cfg_opts_, &cf_opts_));

  RegisterTests("test");

  ASSERT_OK(MergeOperator::CreateFromString("ChanglingMergeOperator", cfg_opts_,
                                            &op));
  ASSERT_NE(op, nullptr);
  ASSERT_EQ(op->Name(), std::string("ChanglingMergeOperator"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "merge_operator={id=ChanglingMergeOperator}", cfg_opts_,
      &cf_opts_));
  ASSERT_NE(cf_opts_.merge_operator, nullptr);
  ASSERT_EQ(cf_opts_.merge_operator->Name(),
            std::string("ChanglingMergeOperator"));
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterTest) {
  const CompactionFilter* cf = nullptr;
  ASSERT_NOK(CompactionFilter::CreateFromString("ChanglingCompactionFilter",
                                                cfg_opts_, &cf));
  ASSERT_OK(CompactionFilter::CreateFromString(
      "RemoveEmptyValueCompactionFilter", cfg_opts_, &cf));
  ASSERT_NE(cf, nullptr);
  ASSERT_EQ(cf->Name(), std::string("RemoveEmptyValueCompactionFilter"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "compaction_filter={id=ChanglingCompactionFilter}", cfg_opts_,
      &cf_opts_));
  ASSERT_EQ(cf_opts_.compaction_filter, nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "compaction_filter={id=RemoveEmptyValueCompactionFilter}",
      cfg_opts_, &cf_opts_));
  RegisterTests("test");

  ASSERT_OK(CompactionFilter::CreateFromString("ChanglingCompactionFilter",
                                               cfg_opts_, &cf));
  ASSERT_NE(cf, nullptr);
  ASSERT_EQ(cf->Name(), std::string("ChanglingCompactionFilter"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "compaction_filter={id=ChanglingCompactionFilter}", cfg_opts_,
      &cf_opts_));
  ASSERT_NE(cf_opts_.compaction_filter, nullptr);
  ASSERT_EQ(cf_opts_.compaction_filter->Name(),
            std::string("ChanglingCompactionFilter"));
}

TEST_F(LoadCustomizableTest, LoadCompactionFilterFactoryTest) {
  std::shared_ptr<CompactionFilterFactory> cff;
  ASSERT_NOK(CompactionFilterFactory::CreateFromString(
      "ChanglingCompactionFilterFactory", cfg_opts_, &cff));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "compaction_filter_factory={id=ChanglingCompactionFilterFactory}",
      cfg_opts_, &cf_opts_));
  ASSERT_EQ(cf_opts_.compaction_filter_factory, nullptr);
  RegisterTests("test");

  ASSERT_OK(CompactionFilterFactory::CreateFromString(
      "ChanglingCompactionFilterFactory", cfg_opts_, &cff));
  ASSERT_NE(cff, nullptr);
  ASSERT_EQ(cff->Name(), std::string("ChanglingCompactionFilterFactory"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "compaction_filter_factory={id=ChanglingCompactionFilterFactory}",
      cfg_opts_, &cf_opts_));
  ASSERT_NE(cf_opts_.compaction_filter_factory, nullptr);
  ASSERT_EQ(cf_opts_.compaction_filter_factory->Name(),
            std::string("ChanglingCompactionFilterFactory"));
}

TEST_F(LoadCustomizableTest, LoadFilterPolicyTest) {
  std::shared_ptr<const FilterPolicy> policy;
  ASSERT_NOK(FilterPolicy::CreateFromString("Test", cfg_opts_, &policy));
  ASSERT_OK(FilterPolicy::CreateFromString("rocksdb.BuiltinBloomFilter",
                                           cfg_opts_, &policy));
  ASSERT_NE(policy, nullptr);
  ASSERT_EQ(policy->Name(), std::string("rocksdb.BuiltinBloomFilter"));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "block_based_table_factory={filter_policy=Test}", cfg_opts_,
      &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "block_based_table_factory={filter_policy=rocksdb.BuiltinBloomFilter}",
      cfg_opts_, &cf_opts_));
  auto const* options =
      cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
          TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->filter_policy, nullptr);
  ASSERT_EQ(options->filter_policy->Name(),
            std::string("rocksdb.BuiltinBloomFilter"));
  RegisterTests("test");
  ASSERT_OK(FilterPolicy::CreateFromString("Test", cfg_opts_, &policy));
  ASSERT_NE(policy, nullptr);
  ASSERT_EQ(policy->Name(), std::string("Test"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "block_based_table_factory={filter_policy=Test}", cfg_opts_,
      &cf_opts_));
  options = cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
      TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->filter_policy, nullptr);
  ASSERT_EQ(options->filter_policy->Name(), std::string("Test"));
}

TEST_F(LoadCustomizableTest, LoadCacheTest) {
  std::shared_ptr<Cache> cache;
  ASSERT_NOK(Cache::CreateFromString("Test", cfg_opts_, &cache));
  ASSERT_OK(Cache::CreateFromString(Cache::kLRUCacheName, cfg_opts_, &cache));
  ASSERT_NE(cache, nullptr);
  ASSERT_EQ(cache->Name(), Cache::kLRUCacheName);
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      cf_opts_, "block_based_table_factory={block_cache=Test}", cfg_opts_,
      &cf_opts_));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_, "block_based_table_factory={block_cache=LRUCache}", cfg_opts_,
      &cf_opts_));
  auto const* options =
      cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
          TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->block_cache, nullptr);
  ASSERT_EQ(options->block_cache->Name(), Cache::kLRUCacheName);

  RegisterTests("test");
  ASSERT_OK(Cache::CreateFromString("Test", cfg_opts_, &cache));
  ASSERT_NE(cache, nullptr);
  ASSERT_EQ(cache->Name(), std::string("Test"));

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      cf_opts_,
      "table_factory={id=BlockBasedTable;"
      "block_cache=Test;block_cache_compressed=LRUCache}",
      cfg_opts_, &cf_opts_));
  options = cf_opts_.table_factory->GetOptions<BlockBasedTableOptions>(
      TableFactory::kBlockBasedTableOpts);
  ASSERT_NE(options, nullptr);
  ASSERT_NE(options->block_cache, nullptr);
  ASSERT_EQ(options->block_cache->Name(), std::string("Test"));
}

TEST_F(LoadCustomizableTest, LoadStatisticsTest) {
  std::shared_ptr<Statistics> stats;
  ASSERT_NOK(Statistics::CreateFromString("Test", cfg_opts_, &stats));
  ASSERT_OK(Statistics::CreateFromString("BasicStatistics", cfg_opts_, &stats));
  ASSERT_NE(stats, nullptr);
  ASSERT_EQ(stats->Name(), std::string("BasicStatistics"));

  ASSERT_NOK(GetDBOptionsFromString(db_opts_, "statistics=Test", &db_opts_));
  ASSERT_OK(GetDBOptionsFromString(db_opts_, "statistics=BasicStatistics",
                                   &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("BasicStatistics"));

  RegisterTests("test");

  ASSERT_OK(Statistics::CreateFromString("Test", cfg_opts_, &stats));
  ASSERT_NE(stats, nullptr);
  ASSERT_EQ(stats->Name(), std::string("Test"));

  ASSERT_OK(GetDBOptionsFromString(db_opts_, "statistics=Test", &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("Test"));

  ASSERT_OK(GetDBOptionsFromString(
      db_opts_, "statistics={id=Test;inner=BasicStatistics}", &db_opts_));
  ASSERT_NE(db_opts_.statistics, nullptr);
  ASSERT_EQ(db_opts_.statistics->Name(), std::string("Test"));
  auto* inner = db_opts_.statistics->GetOptions<std::shared_ptr<Statistics>>(
      "StatisticsOptions");
  ASSERT_NE(inner, nullptr);
  ASSERT_NE(inner->get(), nullptr);
  ASSERT_EQ(inner->get()->Name(), std::string("BasicStatistics"));
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
