
#ifndef STORAGE_LEVELDB_DB_VLOG_CACHE_H_
#define STORAGE_LEVELDB_DB_VLOG_CACHE_H_

#include "db/dbformat.h"
#include "db/vlog_manager.h"
#include <cstdint>
#include <leveldb/env.h>
#include <list>
#include <map>
#include <string>
#include <util/mutexlock.h>

#include "leveldb/cache.h"
#include "leveldb/table.h"

#include "port/port.h"

#include "vlog_manager.h"

namespace leveldb {
namespace vlog {

class VlogInfo;
class VlogManager;

class VlogFetcher {
 public:
  VlogFetcher(const std::string& dbname, const Options& options,
              uint32_t log_number);

  ~VlogFetcher();

  Status Get(uint64_t offset, uint64_t size, std::string* value);

  friend class VlogManager;

 private:
  VlogInfo* my_info_;

  RandomAccessFile* file_;
};
}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_CACHE_H_