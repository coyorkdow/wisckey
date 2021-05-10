
#include "db/vlog_manager.h"

#include "db/vlog_reader.h"

#include "util/coding.h"

#include "filename.h"

namespace leveldb {
namespace vlog {

VlogManager::VlogManager(uint64_t clean_threshold)
    : clean_threshold_(clean_threshold), cur_vlog_(0) {}

VlogManager::~VlogManager() {
  for (auto& it : manager_) {
    if (it.first == cur_vlog_) {
      WLock l(it.second->rwlock_);
      it.second->vlog_write_->Flush();
    }
    delete it.second->vlog_fetch_;
    delete it.second->vlog_write_->dest_;
    delete it.second->vlog_write_;
    delete it.second;
  }
}

bool VlogManager::Empty() const { return manager_.empty(); }

void VlogManager::AddVlog(const std::string& dbname, const Options& options,
                          uint64_t vlog_numb) {
  if (!Empty()) {
    assert(manager_.count(cur_vlog_));
    VlogInfo* old = manager_[cur_vlog_];
    WLock l(old->rwlock_);
    old->vlog_write_->Flush();
  }
  VlogInfo* v = new VlogInfo;
  v->vlog_write_ = new VWriter;
  Status s = options.env->NewAppendableFile(VlogFileName(dbname, vlog_numb),
                                            &v->vlog_write_->dest_);
  assert(s.ok());
  // VlogFetcher must initialize after WritableFile is created;
  v->vlog_fetch_ = new VlogFetcher(dbname, options, vlog_numb, &v->head_);
  v->vlog_write_->my_info_ = v;
  v->vlog_fetch_->my_info_ = v;
  v->count_ = 0;
  assert(!manager_.count(vlog_numb));
  manager_[vlog_numb] = v;
  cur_vlog_ = vlog_numb;
}

void VlogManager::SetCurrentVlog(uint64_t vlog_numb) { cur_vlog_ = vlog_numb; }

size_t VlogManager::GetCurrentVlogHead() const {
  assert(manager_.count(cur_vlog_));
  VlogInfo* cur = manager_.find(cur_vlog_)->second;
  return cur->size_ + cur->head_;
}

Status VlogManager::FetchValueFromVlog(Slice addr, std::string* value) {
  Status s;
  uint64_t file_numb, offset, size;
  // address is <vlog_number, vlog_offset, size>
  if (!GetVarint64(&addr, &file_numb))
    return Status::Corruption("parse size false in RealValue");
  if (!GetVarint64(&addr, &offset))
    return Status::Corruption("parse file_numb false in RealValue");
  if (!GetVarint64(&addr, &size))
    return Status::Corruption("parse pos false in RealValue");

  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(file_numb);
  if (iter == manager_.end() || iter->second->vlog_fetch_ == nullptr) {
    s = Status::Corruption("can not find vlog");
  } else {
    VlogFetcher* cache = iter->second->vlog_fetch_;
    s = cache->Get(offset, size, value);
  }

  return s;
}
Status VlogManager::AddRecord(const Slice& slice) {
  std::map<uint64_t, VlogInfo*>::const_iterator iter = manager_.find(cur_vlog_);
  assert(iter != manager_.end());
  assert(iter->second != nullptr);
  return iter->second->vlog_write_->AddRecord(slice);
}

Status VlogManager::Sync() {
  assert(manager_.count(cur_vlog_));
  VlogInfo* cur = manager_[cur_vlog_];
  WLock l(cur->rwlock_);
  return cur->vlog_write_->Flush();
}

}  // namespace vlog
}  // namespace leveldb
