//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#include "cloud/azure/azure_env.h"
#include <unistd.h>
#include <fstream>
#include <iostream>
#include "cloud/filename.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

#ifdef USE_AZURE

#include "cloud/db_cloud_impl.h"

using namespace azure::storage;

namespace rocksdb {

const std::string pathsep = "/";

// types of rocksdb files
const std::string sst = ".sst";
const std::string ldb = ".ldb";
const std::string log = ".log";

#if defined(OS_WIN)
const wchar_t* xdb_size = L"__xdb__size";

static inline std::string&& xdb_to_utf8string(std::string&& value) {
  return std::move(value);
}

static inline const std::string& xdb_to_utf8string(const std::string& value) {
  return value;
}

static inline std::string xdb_to_utf8string(const utf16string& value) {
  return utility::conversions::to_utf8string(value);
}

static inline utf16string xdb_to_utf16string(const std::string& value) {
  return utility::conversions::to_utf16string(value);
}

static inline const utf16string& xdb_to_utf16string(const utf16string& value) {
  return value;
}

static inline utf16string&& xdb_to_utf16string(utf16string&& value) {
  return std::move(value);
}

static inline utf16string xdb_utf8_to_utf16(const std::string& s) {
  return utility::conversions::utf8_to_utf16(s);
}

#else
const char* xdb_size = "__xdb__size";

#define xdb_to_utf8string(x) (x)

#define xdb_to_utf16string(x) (x)

#define xdb_utf8_to_utf16(x) (x)

#endif

static size_t XdbGetUniqueId(const cloud_page_blob& page_blob, char* id,
                             size_t max_size) {
  const std::string path = xdb_to_utf8string(page_blob.uri().path());
  size_t len = path.size() > max_size ? max_size : path.size();
  memcpy(id, path.c_str(), len);
  return len;
}

// Is this a sst file, i.e. ends in ".sst" or ".ldb"
inline bool IsSstFile(const std::string& pathname) {
  if (pathname.size() < sst.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - sst.size();
  if ((memcmp(ptr, sst.c_str(), sst.size()) == 0) ||
      (memcmp(ptr, ldb.c_str(), ldb.size()) == 0)) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix
inline bool IsWalFile(const std::string& pathname) {
  if (pathname.size() < log.size()) {
    return false;
  }
  const char* ptr = pathname.c_str() + pathname.size() - log.size();
  if (memcmp(ptr, log.c_str(), log.size()) == 0) {
    return true;
  }
  return false;
}

bool IsManifestFile(const std::string& pathname) {
  // extract last component of the path
  std::string fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = pathname.substr(offset + 1, pathname.size());
  } else {
    fname = pathname;
  }
  if (fname.find("MANIFEST") == 0) {
    return true;
  }
  return false;
}

bool __attribute__((unused)) IsIdentityFile(const std::string& pathname) {
  // extract last component of the path
  std::string fname;
  size_t offset = pathname.find_last_of(pathsep);
  if (offset != std::string::npos) {
    fname = pathname.substr(offset + 1, pathname.size());
  } else {
    fname = pathname;
  }
  if (fname.find("IDENTITY") == 0) {
    return true;
  }
  return false;
}

// A log file has ".log" suffix or starts with 'MANIFEST"
inline bool IsLogFile(const std::string& pathname) {
  return IsWalFile(pathname) || IsManifestFile(pathname);
}

static void GetFileType(const std::string& fname, bool* sstFile, bool* logFile,
                        bool* manifest, bool* identity) {
  *logFile = false;
  if (manifest) *manifest = false;

  *sstFile = IsSstFile(fname);
  if (!*sstFile) {
    *logFile = IsLogFile(fname);
    if (manifest) {
      *manifest = IsManifestFile(fname);
    }
    if (identity) {
      *identity = IsIdentityFile(fname);
    }
  }
}

class AzureReadableFile : virtual public SequentialFile,
                          virtual public RandomAccessFile {
 public:
  AzureReadableFile(AzureEnv* env, const std::string& bucket_prefix,
                    const std::string& fname, bool is_file = true)
      : env_(env), fname_(fname), offset_(0), file_size_(0), is_file_(is_file) {
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[azure] AzureReadableFile opening file %s", fname_.c_str());
    assert(!is_file_ || IsSstFile(fname) || IsManifestFile(fname) ||
           IsIdentityFile(fname));
  }

  AzureReadableFile(cloud_page_blob& page_blob)
      : offset_(0), _page_blob(page_blob) {
    try {
      _page_blob.download_attributes();
      std::string size = xdb_to_utf8string(_page_blob.metadata()[xdb_size]);
      file_size_ = size.empty() ? -1 : std::stoll(size);
    } catch (const azure::storage::storage_exception& e) {
      file_size_ = -1;
    }
  }

  ~AzureReadableFile() {}

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    return ReadContents(&offset_, n, result, scratch);
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    return ReadContents(&offset, n, result, scratch);
  }

  virtual Status status() { return status_; }

  Status Skip(uint64_t n) {
    offset_ += n;
    return Status::OK();
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return xdb_to_utf8string(_page_blob.name()).c_str(); }

 private:
  Status ReadContents(uint64_t* origin, size_t n, Slice* result,
                      char* scratch) const {
    try {
      uint64_t offset = *origin;
      uint64_t page_offset = (offset >> 9) << 9;
      uint64_t sz = file_size_ - offset;
      if (sz > n) sz = n;
      if (sz <= 0) {
        *result = Slice(scratch, 0);
        return Status::OK();
      }
      size_t cursor = offset - page_offset;
      assert(cursor <= 512);
      size_t nz = ((sz >> 9) + 1 + ((cursor > 0) ? 1 : 0)) << 9;
      std::vector<page_range> pages =
          _page_blob.download_page_ranges(page_offset, nz);
      char* target = scratch;
      size_t remain = sz;
      size_t r = 0;
      for (std::vector<page_range>::iterator it = pages.begin();
           it < pages.end(); it++) {
        concurrency::streams::istream blobstream =
            (const_cast<cloud_page_blob&>(_page_blob)).open_read();
        blobstream.seek(it->start_offset(), std::ios_base::beg);
        concurrency::streams::stringstreambuf buffer;
        blobstream.read(buffer, it->end_offset() - it->start_offset()).wait();
        const char* src = buffer.collection().c_str();
        size_t bsize = buffer.size();
        size_t len = remain < bsize ? remain : bsize - cursor;
        assert(cursor + len <= bsize);
        memcpy(target, src + cursor, len);
        cursor = 0;
        remain -= len;
        target += len;
        r += len;
        if (remain <= 0) break;
      }
      *result = Slice(scratch, r);
      *origin = offset + r;
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
    }
    return Status::IOError(Status::kNone);
  }

 private:
  AzureEnv* env_;
  std::string fname_;
  uint64_t offset_;
  uint64_t file_size_;
  bool is_file_;  // is this a file or dir?
  Status status_;
  cloud_page_blob _page_blob;
};

class AzureWritableFile : public WritableFile {
 public:
  AzureWritableFile(AzureEnv* env, const std::string& local_fname,
                    const std::string& bucket_prefix,
                    const std::string& cloud_fname, const EnvOptions& options,
                    const CloudEnvOptions cloud_env_options)
      : env_(env), fname_(local_fname) {
    assert(IsSstFile(fname_) || IsManifestFile(fname_));

    // Is this a manifest file?
    is_manifest_ = IsManifestFile(fname_);

    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[azure] AzureWritableFile bucket %s opened local file %s "
        "cloud file %s manifest %d",
        bucket_prefix.c_str(), fname_.c_str(), cloud_fname.c_str(),
        is_manifest_);
  }

  AzureWritableFile(cloud_page_blob& page_blob)
      : _page_blob(page_blob),
        _bufoffset(0),
        _pageindex(0),
        _size(0),
        _iofail(false) {
    _page_blob.create(4 * 1024);
    std::string name = xdb_to_utf8string(_page_blob.name());
  }

  ~AzureWritableFile() {}

  virtual Status Append(const char* src, size_t size) {
    size_t remain = size;
    while (remain > 0) {
      size_t cap = _buf_size - _bufoffset;
      char* target = _buffer + _bufoffset;
      int len = (int)(remain > cap ? cap : remain);
      memcpy(target, src, len);
      target += len;
      _bufoffset += len;
      src += len;
      _size += len;
      if (cap < _page_size) {
        Status s = Flush();
        if (!s.ok()) {
          return s;
        }
      }
      remain -= len;
    }
    return Status::OK();
  }

  virtual Status Flush() {
    if (!_page_blob.exists()) return Status::NotFound();
    int numpages = _bufoffset / _page_size;
    int remain = _bufoffset % _page_size;
    int len = (numpages + (remain > 0 ? 1 : 0)) * _page_size;
    if (len == 0) return Status::OK();
    try {
      if ((CurrSize() + _buf_size) >= Capacity()) {
        Expand();
      }
      std::vector<char> buffer;
      buffer.assign(&_buffer[0], &_buffer[len]);
      concurrency::streams::istream page_stream =
          concurrency::streams::bytestream::open_istream(buffer);
      _page_blob.upload_pages(page_stream, _pageindex * _page_size,
                              utility::string_t(U("")));
      if (remain > 0) {
        memcpy(_buffer, _buffer + numpages * _page_size, remain);
      }
      _bufoffset = remain;
      _pageindex += numpages;
      return Status::OK();
    } catch (const azure::storage::storage_exception& e) {
      if (!_iofail) {
        _iofail = true;
      } else {
        return Status::Aborted();
      }
    }
    return Status::NoSpace();
  }

  Status Append(const Slice& data) { return Append(data.data(), data.size()); }

  Status PositionedAppend(const Slice& /* data */, uint64_t /* offset */) {
    return Status::NotSupported();
  }

  Status InvalidateCache(size_t offset, size_t length) { return Status::OK(); }

  Status Truncate(uint64_t size) {
    try {
      if (_page_blob.exists()) {
        Sync();
        _size = size;
        _page_blob.resize(((size >> 9) + 1) << 9);
      }
    } catch (const azure::storage::storage_exception& e) {
    }
    return Status::OK();
  }

  Status Close() { return Status::OK(); }

  Status Sync() {
    try {
      if (_page_blob.exists()) {
        Flush();
        _page_blob.metadata().reserve(1);
        _page_blob.metadata()[xdb_size] =
            xdb_to_utf16string(std::to_string(CurrSize()));
        _page_blob.upload_metadata();
      }
    } catch (const azure::storage::storage_exception& e) {
    }
    return Status::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const {
    return XdbGetUniqueId(_page_blob, id, max_size);
  }

  const char* Name() { return xdb_to_utf8string(_page_blob.name()).c_str(); }

 private:
  inline uint64_t CurrSize() const { return _size; }

  inline uint64_t Capacity() const { return _page_blob.properties().size(); }

  inline void Expand() {
    uint64_t size = (((_size + _buf_size) >> 9) + 1) << 9;
    _page_blob.resize(size * 2);
  }

 private:
  const static int _page_size = 512;
  const static int _buf_size = 1024 * 2 * _page_size;
  AzureEnv* env_;
  std::string fname_;
  bool is_manifest_;
  cloud_page_blob _page_blob;
  int _bufoffset;
  uint64_t _pageindex;
  uint64_t _size;
  bool _iofail;
  char _buffer[_buf_size + _page_size];
};

AzureEnv::AzureEnv(Env* underlying_env, const std::string& src_bucket_prefix,
                   const std::string& src_object_prefix,
                   const std::string& src_bucket_connect_string,
                   const std::string& dest_bucket_prefix,
                   const std::string& dest_object_prefix,
                   const std::string& dest_bucket_connect_string,
                   const CloudEnvOptions& _cloud_env_options,
                   std::shared_ptr<Logger> info_log)
    : CloudEnvImpl(CloudType::kAzure, underlying_env),
      info_log_(info_log),
      cloud_env_options(_cloud_env_options),
      src_bucket_prefix_(src_bucket_prefix),
      src_object_prefix_(src_object_prefix),
      dest_bucket_prefix_(dest_bucket_prefix),
      dest_object_prefix_(dest_object_prefix),
      running_(true),
      has_src_bucket_(false),
      has_dest_bucket_(false),
      has_two_unique_buckets_(false) {
  src_bucket_prefix_ = trim(src_bucket_prefix_);
  src_object_prefix_ = trim(src_object_prefix_);
  dest_bucket_prefix_ = trim(dest_bucket_prefix_);
  dest_object_prefix_ = trim(dest_object_prefix_);

  base_env_ = underlying_env;

  if (!GetSrcBucketPrefix().empty()) {
    has_src_bucket_ = true;
  }
  if (!GetDestBucketPrefix().empty()) {
    has_dest_bucket_ = true;
  }

  // Do we have two unique buckets?
  if (has_src_bucket_ && has_dest_bucket_ &&
      ((GetSrcBucketPrefix() != GetDestBucketPrefix()) ||
       (GetSrcObjectPrefix() != GetDestObjectPrefix()))) {
    has_two_unique_buckets_ = true;
  }

  if (has_two_unique_buckets_) {
    if (src_bucket_connect_string == dest_bucket_connect_string) {
      // alls good
    } else {
      create_bucket_status_ =
          Status::InvalidArgument("Two different regions not supported");
      Log(InfoLogLevel::ERROR_LEVEL, info_log,
          "[azure] NewAzureEnv Buckets %s, %s in two different regions %s, "
          "%s "
          "is not supported",
          src_bucket_prefix_.c_str(), dest_bucket_prefix_.c_str(),
          src_bucket_connect_string.c_str(),
          dest_bucket_connect_string.c_str());
      return;
    }
  }

  try {
    cloud_storage_account storage_account = cloud_storage_account::parse(
        xdb_to_utf16string(src_bucket_connect_string));
    auto blob_client = storage_account.create_cloud_blob_client();
    src_container_ = blob_client.get_container_reference(
        xdb_to_utf16string(src_bucket_prefix));
    src_container_.create_if_not_exists();
    storage_account = cloud_storage_account::parse(
        xdb_to_utf16string(dest_bucket_connect_string));
    blob_client = storage_account.create_cloud_blob_client();
    dest_container_ = blob_client.get_container_reference(
        xdb_to_utf16string(src_bucket_prefix));
    dest_container_.create_if_not_exists();
    create_bucket_status_ = Status::OK();
  } catch (const azure::storage::storage_exception& e) {
    create_bucket_status_ = Status::InvalidArgument();
    Log(InfoLogLevel::ERROR_LEVEL, info_log,
        "[azure] NewAzureEnv Unable to create environment %s", e.what());
  }
}

AzureEnv::~AzureEnv() {
  running_ = false;
  StopPurger();
  if (tid_.joinable()) {
    tid_.join();
  }
}

Status AzureEnv::status() { return create_bucket_status_; }
Status AzureEnv::NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) {
  return base_env_->NewLogger(fname, result);
}

Status AzureEnv::DeleteDbid(const std::string& bucket_prefix,
                            const std::string& dbid) {
  // fetch the list all all dbids
  std::string dbidkey = dbid_registry_ + dbid;
  return Status::OK();
}

//
// prepends the configured src object path name
//
std::string AzureEnv::srcname(const std::string& localname) {
  assert(!src_bucket_prefix_.empty());
  return src_object_prefix_ + "/" + basename(localname);
}

//
// prepends the configured dest object path name
//
std::string AzureEnv::destname(const std::string& localname) {
  assert(!dest_bucket_prefix_.empty());
  return dest_object_prefix_ + "/" + basename(localname);
}

inline std::string GetBucket(const std::string& bucket_prefix) {
  return "rockset." + bucket_prefix;
}

cloud_blob_container& AzureEnv::GetContainer(const std::string& name) {
  return dest_container_;
}

Status AzureEnv::DeleteBlob(const std::string& bucket_prefix,
                            const std::string& fname) {
  try {
    auto container = GetContainer(GetDestBucketPrefix());
    cloud_page_blob page_blob =
        container.get_page_blob_reference(xdb_to_utf16string(fname));
    page_blob.delete_blob();
    return Status::OK();
  } catch (const azure::storage::storage_exception& e) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[xdb] EnvXdb DeleteBlob %s target with exception %s\n",
        xdb_to_utf8string(fname).c_str(), e.what());
    return Status::IOError();
  }
}

Status AzureEnv::DeleteFile(const std::string& fname) {
  Status st;
  // Get file type
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;

  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);

  if (has_dest_bucket_ && (sstfile || manifest || identity)) {
    st = DeleteBlob(GetDestBucketPrefix(), destname(fname));
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[azure] DeleteFile DeleteBlob file %s error %s", fname.c_str(),
          st.ToString().c_str());
      return st;
    }
    // delete from local
    st = base_env_->DeleteFile(fname);
  } else {
    st = base_env_->DeleteFile(fname);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "[azure] DeleteFile file %s %s",
      fname.c_str(), st.ToString().c_str());
  return st;
}

Status AzureEnv::RenameFile(const std::string& src, const std::string& target) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[azure] RenameFile src '%s' target '%s'", src.c_str(), target.c_str());

  // Get file type of target
  bool logfile;
  bool sstfile;
  bool manifestfile;
  bool idfile;
  GetFileType(target, &sstfile, &logfile, &manifestfile, &idfile);

  if (sstfile) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[aws] RenameFile source sstfile %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (logfile) {
    // Rename should never be called on log files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] RenameFile source logfile %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (manifestfile) {
    // Rename should never be called on manifest files as well
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[aws] RenameFile source manifest %s %s is not supported", src.c_str(),
        target.c_str());
    assert(0);
    return Status::NotSupported(Slice(src), Slice(target));

  } else if (!idfile || !has_dest_bucket_) {
    return base_env_->RenameFile(src, target);
  }
  // Only ID file should come here
  assert(idfile);
  assert(has_dest_bucket_);
  assert(basename(target) == "IDENTITY");

  Status st = Status::OK();  // SaveIdentitytoAzure(src, destname(target));

  // Do the rename on local filesystem too
  if (st.ok()) {
    st = base_env_->RenameFile(src, target);
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[azure] RenameFile src %s target %s: %s", src.c_str(), target.c_str(),
      st.ToString().c_str());
  return st;
}

Status AzureEnv::FileExists(const std::string& fname) { return Status::OK(); }

Status AzureEnv::UnlockFile(FileLock* lock) { return Status::OK(); }

Status AzureEnv::LockFile(const std::string& fname, FileLock** lock) {
  *lock = nullptr;
  return Status::OK();
}

Status AzureEnv::EmptyBucket(const std::string& bucket_prefix) {
  return Status::OK();
}

Status AzureEnv::GetChildren(const std::string& path,
                             std::vector<std::string>* result) {
  return Status::OK();
}

Status AzureEnv::NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) {
  return Status::OK();
}

Status AzureEnv::DeleteDir(const std::string& dirname) { return Status::OK(); }

Status AzureEnv::GetDbidList(const std::string& bucket_prefix,
                             DbidList* dblist) {
  return Status::OK();
}

Status AzureEnv::SaveDbid(const std::string& dbid, const std::string& dirname) {
  return Status::OK();
}

Status AzureEnv::GetPathForDbid(const std::string& bucket_prefix,
                                const std::string& dbid, std::string* dirname) {
  return Status::OK();
}

Status AzureEnv::GetFileSize(const std::string& fname, uint64_t* size) {
  return Status::OK();
}

Status AzureEnv::NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) {
  *result = nullptr;
  Status st;
  bool logfile;
  bool sstfile;
  bool manifest;
  bool identity;
  GetFileType(fname, &sstfile, &logfile, &manifest, &identity);
  return Status::OK();
}

Status AzureEnv::CreateDir(const std::string& dirname) { return Status::OK(); }

Status AzureEnv::CreateDirIfMissing(const std::string& dirname) {
  return Status::OK();
}

Status AzureEnv::NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) {
  return Status::OK();
}

Status AzureEnv::NewSequentialFileCloud(const std::string& bucket_prefix,
                                        const std::string& fname,
                                        unique_ptr<SequentialFile>* result,
                                        const EnvOptions& options) {
  *result = nullptr;

  AzureReadableFile* f = new AzureReadableFile(this, bucket_prefix, fname);
  Status st = f->status();
  if (!st.ok()) {
    delete f;
  } else {
    result->reset(dynamic_cast<SequentialFile*>(f));
  }
  return st;
}

Status AzureEnv::GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) {
  return Status::OK();
}
}
#endif  // USE_AZURE
