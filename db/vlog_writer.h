// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VLOG_WRITER_H_
#define STORAGE_LEVELDB_DB_VLOG_WRITER_H_

#include "db/log_format.h"
#include "db/vlog_manager.h"
#include <cstdint>

#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;
class VlogInfo;
class VlogManager;

namespace vlog {

class VWriter {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit VWriter(WritableFile* dest);

  ~VWriter();

  Status AddRecord(const Slice& slice);

  friend class VlogManager;

 private:
  VWriter() = default;

  VlogInfo* my_info_;
  WritableFile* dest_;
  // No copying allowed
  VWriter(const VWriter&);
  void operator=(const VWriter&);
};

}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VLOG_WRITER_H_
