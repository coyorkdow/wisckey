// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VLOG_READER_H_
#define STORAGE_LEVELDB_DB_VLOG_READER_H_

#include "db/log_format.h"
#include <cstdint>

#include "leveldb/slice.h"
#include "leveldb/status.h"

#include "port/port.h"

namespace leveldb {

class SequentialFile;

namespace vlog {
class VReader {
 public:
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  VReader(SequentialFile* file, Reporter* reporter, bool checksum,
          uint64_t initial_offset = 0);

  ~VReader();

  bool ReadRecord(Slice* record, std::string* scratch);
  bool DeallocateDiskSpace(uint64_t offset, size_t len);

 private:
  port::Mutex mutex_;
  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;
  char* const backing_store_;
  Slice buffer_;
  bool eof_;  // Last Read() indicated EOF by returning <
  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);
  // No copying allowed
  VReader(const VReader&);
  void operator=(const VReader&);
};

}  // namespace vlog
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
