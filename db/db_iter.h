// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_ITER_H_
#define STORAGE_LEVELDB_DB_DB_ITER_H_

#include "db/dbformat.h"
#include <atomic>
#include <cstdint>

#include "leveldb/db.h"

namespace leveldb {

class DBImpl;

class IterCache {
 public:
  IterCache() : valid_(false), sequence(0) {}
  ~IterCache() = default;
  IterCache(const IterCache& r)
      : key_(r.key_),
        addr_(r.addr_),
        val_(r.val_),
        valid_(r.valid_),
        status(r.status),
        sequence(r.sequence.load(std::memory_order_relaxed)) {}
  std::string key_;
  std::string addr_;
  std::string val_;
  bool valid_;
  Status status;
  std::atomic<uint64_t> sequence;
};

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_ITER_H_
