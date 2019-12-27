// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/object_registry.h"

#include <memory>
#include <unordered_map>

#include "logging/logging.h"
#include "options/options_helper.h"
#include "rocksdb/env.h"
#include "util/string_util.h"

namespace rocksdb {
#ifndef ROCKSDB_LITE
static const std::string kDefaultLibraryName = "default";
static const std::string kLocalLibraryName = "local";
static const std::string kDynamicLibraryName = "dynamic";

std::string ObjectLibrary::AsString() const {
  return (std::string("id=") + Name());
}

std::string ObjectLibrary::ToString() const { return "{" + AsString() + "}"; }

// Looks through the "type" factories for one that matches "name".
// If found, returns the pointer to the Entry matching this name.
// Otherwise, nullptr is returned
const ObjectLibrary::Entry *ObjectLibrary::FindEntry(
    const std::string &type, const std::string &name) const {
  auto entries = entries_.find(type);
  if (entries != entries_.end()) {
    for (const auto &entry : entries->second) {
      if (entry->matches(name)) {
        return entry.get();
      }
    }
  }
  return nullptr;
}

void ObjectLibrary::AddEntry(const std::string &type,
                             std::unique_ptr<Entry> &entry) {
  auto &entries = entries_[type];
  entries.emplace_back(std::move(entry));
}

void ObjectLibrary::Dump(Logger *logger) const {
  for (const auto &iter : entries_) {
    bool printed_one = false;
    ROCKS_LOG_HEADER(logger, "    Registered factories for type[%s] ",
                     iter.first.c_str());
    for (const auto &e : iter.second) {
      ROCKS_LOG_HEADER(logger, "%c %s", (printed_one) ? ',' : ':',
                       e->Name().c_str());
      printed_one = true;
    }
  }
  ROCKS_LOG_HEADER(logger, "\n");
}

class DefaultObjectLibrary : public ObjectLibrary {
 public:
  const char *Name() const override { return kDefaultLibraryName.c_str(); }
};

// Returns the Default singleton instance of the ObjectLibrary
// This instance will contain most of the "standard" registered objects
std::shared_ptr<ObjectLibrary> &ObjectLibrary::Default() {
  static std::shared_ptr<ObjectLibrary> instance =
      std::make_shared<DefaultObjectLibrary>();
  return instance;
}

std::shared_ptr<ObjectRegistry> ObjectRegistry::NewInstance() {
  std::shared_ptr<ObjectRegistry> instance = std::make_shared<ObjectRegistry>();
  return instance;
}

ObjectRegistry::ObjectRegistry() {
  libraries_.push_back(ObjectLibrary::Default());
}

// Searches (from back to front) the libraries looking for the
// an entry that matches this pattern.
// Returns the entry if it is found, and nullptr otherwise
const ObjectLibrary::Entry *ObjectRegistry::FindEntry(
    const std::string &type, const std::string &name) const {
  for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
    const auto *entry = iter->get()->FindEntry(type, name);
    if (entry != nullptr) {
      return entry;
    }
  }
  return nullptr;
}

void ObjectRegistry::Dump(Logger *logger) const {
  for (auto iter = libraries_.crbegin(); iter != libraries_.crend(); ++iter) {
    iter->get()->Dump(logger);
  }
}

const std::string ObjectRegistry::ToString() const {
  std::string result = " | ";
  for (const auto library : libraries_) {
    result.append(library->ToString());
    result.append(" | ");
  }
  return "{" + result + "}";
}

Status ObjectRegistry::ConfigureFromString(Env *env, const std::string &input) {
  Status s;
  std::string opts = trim(input);
  while (opts.size() > 2 && opts[0] == '{' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }
  std::vector<std::string> libs = ParseVector(opts, '|');
  for (const auto lib : libs) {
    std::unordered_map<std::string, std::string> props;
    s = StringToMap(lib, &props);
    if (s.ok() && props.size() > 0) {
      auto id = props.find("id");
      if (id == props.end()) {
        s = Status::InvalidArgument("Bad library type");
      } else if (id->second == kDynamicLibraryName) {
        auto name = props.find("library");
        auto method = props.find("method");
        auto arg = props.find("arg");
        if (name == props.end() || method == props.end()) {
          s = Status::InvalidArgument("Dynamic library with no name or method");
        } else {
          std::shared_ptr<DynamicLibrary> library;
          s = env->LoadLibrary(name->second, "", &library);
          if (s.ok()) {
            s = AddDynamicLibrary(library, method->second,
                                  arg != props.end() ? arg->second : "");
          }
        }
      } else if (id->second == kLocalLibraryName) {
        auto name = props.find("name");
        if (name != props.end()) {
          AddLocalLibrary(name->second);
        } else {
          s = Status::InvalidArgument("Local library with no name");
        }
      } else if (id->second != kDefaultLibraryName) {
        s = Status::InvalidArgument("Unknown library type:", id->second);
      }
    }
    if (!s.ok()) {
      return s;
    }
  }  // End for all libs
  return s;
}

class LocalObjectLibrary : public ObjectLibrary {
 public:
  LocalObjectLibrary(const std::string &name) : name_(name) {}
  const char *Name() const override { return kLocalLibraryName.c_str(); }

 protected:
  std::string AsString() const override {
    return ObjectLibrary::AsString() + "; name=" + name_;
  }

 private:
  const std::string name_;
};

std::shared_ptr<ObjectLibrary> ObjectRegistry::AddLocalLibrary(
    const std::string &name) {
  auto library = std::make_shared<LocalObjectLibrary>(name);
  libraries_.emplace_back(library);
  return library;
}

class DynamicObjectLibrary : public ObjectLibrary {
 public:
  DynamicObjectLibrary(const std::shared_ptr<DynamicLibrary> &library,
                       const std::string &method, const std::string &arg)
      : library_(library), method_(method), arg_(arg) {}
  const char *Name() const override { return kDynamicLibraryName.c_str(); }
  ~DynamicObjectLibrary() override {
    // Force the entries to be cleared before the library is closed...
    entries_.clear();
  }

 protected:
  std::string AsString() const override {
    return (ObjectLibrary::AsString() + "; library=" + library_->Name() +
            "; method=" + method_ + "; arg=" + arg_);
  }

 private:
  const std::shared_ptr<DynamicLibrary> library_;
  const std::string method_;
  const std::string arg_;
};

Status ObjectRegistry::AddDynamicLibrary(
    const std::shared_ptr<DynamicLibrary> &dyn_lib, const std::string &method,
    const std::string &arg) {
  std::function<void *(void *, const char *)> function;
  RegistrarFunc registrar;
  Status s = dyn_lib->LoadFunction(method, &registrar);
  if (s.ok()) {
    std::shared_ptr<ObjectLibrary> obj_lib;
    obj_lib.reset(new DynamicObjectLibrary(dyn_lib, method, arg));
    obj_lib->Register(registrar, arg);
    libraries_.emplace_back(obj_lib);
  }
  return s;
}

#endif  // ROCKSDB_LITE
}  // namespace rocksdb
