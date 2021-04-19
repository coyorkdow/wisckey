
#ifndef STORAGE_LEVELDB_DB_VLOG_CACHE_H_
#define STORAGE_LEVELDB_DB_VLOG_CACHE_H_

#include "db/dbformat.h"
#include <cstdint>
#include <leveldb/env.h>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/table.h"

#include "port/port.h"

namespace leveldb {
namespace vlog {

class VlogCache {
 public:
  VlogCache(const std::string& dbname, const Options& options,
            uint32_t log_number, int entries);

  ~VlogCache();

  Status Get(uint64_t offset, uint64_t size, std::string *value);

  static const int buffer_size = 1 << 16;

 private:
  SequentialFile* file_;
  char buffer_[buffer_size];
  Cache* cache_;

  port::Mutex mutex_;
};
}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_CACHE_H_