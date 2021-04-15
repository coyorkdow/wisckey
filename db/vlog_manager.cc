#include "db/vlog_manager.h"

#include "db/vlog_reader.h"

#include "util/coding.h"

namespace leveldb {
namespace vlog {

VlogManager::VlogManager(uint64_t clean_threshold)
    : clean_threshold_(clean_threshold), now_vlog_(0) {}

VlogManager::~VlogManager() {
  for (auto& it : manager_) {
    delete it.second.vlog_;
  }
}

void VlogManager::AddVlog(uint64_t vlog_numb, VReader* vlog) {
  VlogInfo v;
  v.vlog_ = vlog;
  v.count_ = 0;
  manager_[vlog_numb] = v;
  now_vlog_ = vlog_numb;
}

void VlogManager::SetCurrentVlog(uint64_t vlog_numb) { now_vlog_ = vlog_numb; }

//与GetVlogsToClean对应
void VlogManager::RemoveCleaningVlog(uint64_t vlog_numb) {
  std::map<uint64_t, VlogInfo>::const_iterator iter = manager_.find(vlog_numb);
  delete iter->second.vlog_;
  manager_.erase(iter);
  cleaning_vlog_set_.erase(vlog_numb);
}

void VlogManager::AddDropCount(uint64_t vlog_numb) {
  std::map<uint64_t, VlogInfo>::iterator iter = manager_.find(vlog_numb);
  if (iter != manager_.end()) {
    iter->second.count_++;
    if (iter->second.count_ >= clean_threshold_ && vlog_numb != now_vlog_) {
      cleaning_vlog_set_.insert(vlog_numb);
    }
  }  //否则说明该vlog已经clean过了
}

std::set<uint64_t> VlogManager::GetVlogsToClean(uint64_t clean_threshold) {
  std::set<uint64_t> res;
  for (auto& it : manager_) {
    if (it.second.count_ >= clean_threshold && it.first != now_vlog_)
      res.insert(it.first);
  }
  return res;
}

uint64_t VlogManager::GetVlogToClean() {
  std::set<uint64_t>::iterator iter = cleaning_vlog_set_.begin();
  assert(iter != cleaning_vlog_set_.end());
  return *iter;
}

VReader* VlogManager::GetVlog(uint64_t vlog_numb) {
  std::map<uint64_t, VlogInfo>::const_iterator iter = manager_.find(vlog_numb);
  if (iter == manager_.end()) {
    return nullptr;
  } else {
    return iter->second.vlog_;
  }
}

bool VlogManager::HasVlogToClean() { return !cleaning_vlog_set_.empty(); }

bool VlogManager::Encode(std::string& val) {
  val.clear();
  uint64_t size = manager_.size();
  if (size == 0) return false;
  char buf[8];
  for (auto& it : manager_) {
    EncodeFixed64(buf, (it.second.count_ << 16) | it.first);
    val.append(buf, 8);
  }
  return true;
}

bool VlogManager::Decode(std::string& val) {
  Slice input(val);
  while (!input.empty()) {
    uint64_t code = DecodeFixed64(input.data());
    uint64_t file_numb = code & 0xffff;
    size_t count = code >> 16;
    //检查manager_现在是否还有该vlog，因为有可能已经删除了
    if (manager_.count(file_numb) > 0) {
      manager_[file_numb].count_ = count;
      if (count >= clean_threshold_ && file_numb != now_vlog_) {
        cleaning_vlog_set_.insert(file_numb);
      }
    }
    input.remove_prefix(8);
  }
  return true;
}

bool VlogManager::NeedRecover(uint64_t vlog_numb) {
  std::map<uint64_t, VlogInfo>::iterator iter = manager_.find(vlog_numb);
  if (iter != manager_.end()) {
    assert(iter->second.count_ >= clean_threshold_);
    return true;
  } else {
    return false;  //不需要recoverclean,即没有清理一半的vlog
  }
}

}  // namespace vlog
}  // namespace leveldb
