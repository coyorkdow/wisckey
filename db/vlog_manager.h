#ifndef STORAGE_LEVELDB_DB_VLOG_MANAGER_H_
#define STORAGE_LEVELDB_DB_VLOG_MANAGER_H_

#include "db/vlog_reader.h"
#include <map>
#include <set>

namespace leveldb {
namespace vlog {
// Header is checksum (4 bytes), length (8 bytes).
static const int kVHeaderSize = 4 + 8;

class VlogManager {
 public:
  struct VlogInfo {
    VReader* vlog_;
    uint64_t count_;  //代表该vlog文件垃圾kv的数量
  };

  explicit VlogManager(uint64_t clean_threshold);
  ~VlogManager();
  // vlog一定要是new出来的，vlog_manager的析构函数会delete它
  void AddVlog(uint64_t vlog_numb, VReader* vlog);
  void RemoveCleaningVlog(uint64_t vlog_numb);

  VReader* GetVlog(uint64_t vlog_numb);
  void AddDropCount(uint64_t vlog_numb);
  bool HasVlogToClean();
  uint64_t GetDropCount(uint64_t vlog_numb) {
    return manager_[vlog_numb].count_;
  }
  std::set<uint64_t> GetVlogsToClean(uint64_t clean_threshold);
  uint64_t GetVlogToClean();
  void SetCurrentVlog(uint64_t vlog_numb);
  bool Encode(std::string& val);
  bool Decode(std::string& val);
  bool NeedRecover(uint64_t vlog_numb);

 private:
  std::map<uint64_t, VlogInfo> manager_;
  std::set<uint64_t> cleaning_vlog_set_;
  uint64_t clean_threshold_;
  uint64_t now_vlog_;
};

}  // namespace vlog
}  // namespace leveldb

#endif
