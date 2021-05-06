
#include "vlog_fetcher.h"

#include <util/mutexlock.h>

#include "filename.h"

namespace leveldb {
namespace vlog {

inline Status Parse(Slice* r, std::string* value) {
  Slice k, v;
  assert((*r)[0] == kTypeValue);
  r->remove_prefix(1);
  if (GetLengthPrefixedSlice(r, &k) && GetLengthPrefixedSlice(r, &v)) {
    value->assign(v.data(), v.size());
    return Status::OK();
  } else {
    return Status::Corruption("failed to decode value from vlog");
  }
}

Status VlogFetcher::Get(const uint64_t offset, const uint64_t size,
                        std::string* value) {
  const char* scratch;
  Slice result;
  Status s;

  // It seems that additional cache is useless for the cost of insert is
  // remarkable.

  char buf[1 << 16];
  bool need_deallocate = false;
  bool in_buffer = false;

  my_info_->rwlock_->SharedLock();
  if (offset >= my_info_->head_) {
    assert(offset - my_info_->head_ < my_info_->size_);
    scratch = &my_info_->buffer_[offset - my_info_->head_];
    result = Slice(scratch, size);
    s = Parse(&result, value);
    in_buffer = true;
  }
  my_info_->rwlock_->SharedUnlock();

  if (!in_buffer) {
    if (size <= (1 << 16)) {
      scratch = buf;
    } else {
      scratch = new char[size];
      need_deallocate = true;
    }
    file_->Read(offset, size, &result, const_cast<char*>(scratch));
    s = Parse(&result, value);
  }

  if (need_deallocate) {
    delete[] scratch;
  }

  return s;
}

VlogFetcher::VlogFetcher(const std::string& dbname, const Options& options,
                         const uint32_t log_number) {
  Status s = options.env->NewNonMmapRandomAccessFile(
      LogFileName(dbname, log_number), &file_);
  assert(s.ok());
}

VlogFetcher::~VlogFetcher() {
  delete file_;
}

}  // namespace vlog
}  // namespace leveldb