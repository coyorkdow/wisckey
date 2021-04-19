
#ifndef STORAGE_LEVELDB_DB_VLOG_CACHE_H_
#define STORAGE_LEVELDB_DB_VLOG_CACHE_H_

#include "db/dbformat.h"
#include <cstdint>
#include <leveldb/env.h>
#include <list>
#include <map>
#include <string>
#include <util/mutexlock.h>

#include "leveldb/cache.h"
#include "leveldb/table.h"

#include "port/port.h"

namespace leveldb {
namespace vlog {

class VlogCache {
  class LRUHandle {
   public:
    explicit LRUHandle(uint32_t k, const char* v) : key(k), value(v) {}
    ~LRUHandle() { delete[] value; }
    const char* const value;
    const uint32_t key;
  };

  class HashTable {
   public:
    void Get(uint32_t key, std::list<LRUHandle*>::iterator** iter) {
      uint32_t mod = key & mask_;
      auto it = bucket_[mod].find(key);
      if (it == bucket_[mod].end()) {
        *iter = nullptr;
      } else {
        *iter = &it->second;
      }
    }
    bool Put(uint32_t key, std::list<LRUHandle*>::iterator iter) {
      return bucket_[key & mask_].insert(std::make_pair(key, iter)).second;
    }
    void Erase(uint32_t key) { bucket_[key & mask_].erase(key); }

   private:
    static const uint32_t mask_ = (1 << 13) - 1;
    std::map<uint32_t, std::list<LRUHandle*>::iterator> bucket_[mask_ + 1];
  };

  class Cache {
   public:
    explicit Cache(uint32_t capacity) : cap_(capacity), size_(0) {
      assert(capacity != 0 && capacity % 2 == 0);
    }

    const char* Find(uint32_t key) {
      std::list<LRUHandle*>::iterator* iter;
      hash_table_.Get(key, &iter);
      if (iter == nullptr) {
        return nullptr;
      }
      LRUHandle* handle = **iter;
      cache_.erase(*iter);
      *iter = cache_.insert(cache_.end(), handle);
      hash_table_.Put(key, *iter);
      return handle->value;
    }

    void Insert(uint32_t key, const char* const value) {
      LRUHandle* handle = new LRUHandle(key, value);
      if (size_ == cap_) {
        LRUHandle* victim = cache_.front();
        hash_table_.Erase(victim->key);
        cache_.pop_front();
        delete victim;
      } else {
        size_++;
      }
      hash_table_.Put(key, cache_.insert(cache_.end(), handle));
    }

   private:
    std::list<LRUHandle*> cache_;
    HashTable hash_table_;
    const uint32_t cap_;
    uint32_t size_;
  };

 public:
  VlogCache(const std::string& dbname, const Options& options,
            uint32_t log_number, int entries);

  ~VlogCache();

  Status Get(uint64_t offset, uint64_t size, std::string* value);

 private:
  SequentialFile* file_ GUARDED_BY(mutex_);
  Cache cache_;

  port::Mutex mutex_;
};
}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_CACHE_H_