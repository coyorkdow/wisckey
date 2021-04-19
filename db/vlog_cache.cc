
#include "vlog_cache.h"

#include <util/mutexlock.h>

#include "filename.h"

namespace leveldb {
namespace vlog {

Status VlogCache::Get(const uint64_t offset, const uint64_t size,
                      std::string* value) {
  const char* scratch;
  Slice result;
  Status s;
  mutex_.Lock();
  if ((scratch = cache_.Find(offset)) != nullptr) {
    result = Slice(scratch, size);
  } else {
    scratch = new char[size];
    file_->Jump(offset);
    s = file_->Read(size, &result, const_cast<char*>(scratch));
    cache_.Insert(offset, scratch);
  }
  mutex_.Unlock();
  if (!s.ok()) {
    return s;
  }

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
  } else {
    s = Status::Corruption("failed to decode value from vlog");
  }

  return s;
}

VlogCache::VlogCache(const std::string& dbname, const Options& options,
                     const uint32_t log_number, int entries)
    : cache_(entries) {
  Status s =
      options.env->NewSequentialFile(LogFileName(dbname, log_number), &file_);
  assert(s.ok());
}

VlogCache::~VlogCache() { delete file_; }

}  // namespace vlog
}  // namespace leveldb