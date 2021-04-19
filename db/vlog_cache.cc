
#include "vlog_cache.h"

#include <util/mutexlock.h>

#include "filename.h"

namespace leveldb {
namespace vlog {

Status VlogCache::Get(const uint64_t offset, const uint64_t size,
                      std::string* value) {
  const char* scratch;
  size_t len;
  Slice result;
  Status s;
  if ((scratch = cache_.Find(offset, &len)) != nullptr) {
    value->assign(scratch, len);
  } else {
    char buf[1 << 16];
    if (size <= (1 << 16)) {
      scratch = buf;
    } else {
      scratch = new char[size];
    }
    MutexLock l(&mutex_);
    file_->Jump(offset);
    s = file_->Read(size, &result, const_cast<char*>(scratch));
    Slice k, v;
    assert(result[0] == kTypeValue);
    result.remove_prefix(1);
    if (GetLengthPrefixedSlice(&result, &k) &&
        GetLengthPrefixedSlice(&result, &v)) {
      //      std::fprintf(stdout,
      //                   "fetch: file_numb is %llu, pos is %llu, size is %llu,
      //                   key " "is %s, val is %s\n", file_numb, pos, size,
      //                   k.data(), v.data());
      //      fflush(stdout);
      value->assign(v.data(), v.size());
      if (v.size() <= Cache::MaxCacheSize) {
        cache_.Insert(offset, v.data(), v.size());
      }
    } else {
      s = Status::Corruption("failed to decode value from vlog");
    }
    if (size > (1 << 16)) {
      delete[] scratch;
    }
  }

  return s;
}

VlogCache::VlogCache(const std::string& dbname, const Options& options,
                     const uint32_t log_number, int entries) {
  Status s =
      options.env->NewSequentialFile(LogFileName(dbname, log_number), &file_);
  assert(s.ok());
}

VlogCache::~VlogCache() { delete file_; }

}  // namespace vlog
}  // namespace leveldb