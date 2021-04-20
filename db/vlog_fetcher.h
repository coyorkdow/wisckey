
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
  class Cache {
   public:
    static const size_t MaxCacheSize = 64 - 2 - 1;

    Cache() {
      for (Line& line : line_) {
        line[0] = 0;
        line[1] = 0;
        line[2] = 0;
      }
    }

    inline const char* Find(uint32_t key, size_t* size) {
      char* dst = line_[key & mask_];
      if ((dst[2] & (1 << 7)) == 0 || DecodeFixed16(dst) != (key >> 16)) {
        return nullptr;
      }
      const uint8_t* const ptr = reinterpret_cast<const uint8_t*>(dst) + 2;
      *size = (*ptr) - (1 << 7);
      return dst + 3;
    }

    // size of value must be less or equal than MaxCacheSize;
    inline void Insert(uint32_t key, const char* value, size_t size) {
      char* dst = line_[key & mask_];
      EncodeFixed16(dst, key >> 16);
      dst[2] = size | (1 << 7);
      memcpy(dst + 3, value, size);
    }

   private:
    using Line = char[64];
    Line line_[1 << 16];

    static const uint32_t mask_ = (1 << 16) - 1;
  };

 public:
  VlogFetcher(const std::string& dbname, const Options& options,
              uint32_t log_number, int entries);

  ~VlogFetcher();

  Status Get(uint64_t offset, uint64_t size, std::string* value);

  friend class VlogManager;
  ;

 private:
  VlogInfo* my_info_;

  SequentialFile* file_;
  Cache cache_;

  port::Mutex mutex_;
};
}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_CACHE_H_