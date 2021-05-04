
#include "vlog_fetcher.h"

#include <util/mutexlock.h>

#include "filename.h"

namespace leveldb {
namespace vlog {

Status VlogFetcher::Get(const uint64_t offset, const uint64_t size,
                        std::string* value) {
  const char* scratch;
  size_t len;
  Slice result;
  Status s;
  // It seems that additional cache is useless for the cost of insert is
  // remarkable.

  //  if ((scratch = cache_.Find(offset, &len)) != nullptr) {
  //    value->assign(scratch, len);
  //  } else {
  char buf[1 << 16];
  bool need_deallocate = false;
  bool in_buffer = false;
  bool locked = false;

  while (!my_info_->critical_is_locked.compare_exchange_weak(
      locked, true, std::memory_order_acq_rel)) {
    locked = false;
  }
  if (offset >= my_info_->head_) {
    assert(offset - my_info_->head_ < my_info_->size_);
    scratch = &my_info_->buffer_[offset - my_info_->head_];
    result = Slice(scratch, size);
    in_buffer = true;
  }
  my_info_->critical_is_locked.store(false, std::memory_order_release);

  if (!in_buffer) {
    if (size <= (1 << 16)) {
      scratch = buf;
    } else {
      scratch = new char[size];
      need_deallocate = true;
    }
    s = file_->Read(offset, size, &result, const_cast<char*>(scratch));
  }

  Slice k, v;
  assert(result[0] == kTypeValue);
  result.remove_prefix(1);
  if (GetLengthPrefixedSlice(&result, &k) &&
      GetLengthPrefixedSlice(&result, &v)) {
    value->assign(v.data(), v.size());
    //    if (v.size() <= Cache::MaxCacheSize) {
    //      cache_.Insert(offset, v.data(), v.size());
    //    }
  } else {
    s = Status::Corruption("failed to decode value from vlog");
  }
  if (need_deallocate) {
    delete[] scratch;
  }
  //  }

  return s;
}

VlogFetcher::VlogFetcher(const std::string& dbname, const Options& options,
                         const uint32_t log_number, int entries) {
  Status s = options.env->NewNonMmapRandomAccessFile(
      LogFileName(dbname, log_number), &file_);
  assert(s.ok());
}

VlogFetcher::~VlogFetcher() { delete file_; }

}  // namespace vlog
}  // namespace leveldb