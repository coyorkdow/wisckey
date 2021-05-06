// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/vlog_writer.h"

#include "db/vlog_manager.h"
#include <cstdint>

#include "leveldb/env.h"

#include "util/coding.h"
#include "util/crc32c.h"

#include "dbformat.h"

namespace leveldb {
namespace vlog {

VWriter::VWriter(WritableFile* dest) : dest_(dest) {}

VWriter::~VWriter() = default;

Status VWriter::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();
  char head[kVHeaderSize];
  uint32_t crc = crc32c::Extend(0, ptr, left);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(head, crc);
  EncodeFixed64(&head[4], left);

  Status s;

  if (my_info_->size_ + kVHeaderSize + left > WriteBufferSize) {
    WLock l(my_info_->rwlock_);
    if (!(s = dest_->SyncedAppend(Slice(my_info_->buffer_, my_info_->size_)))
             .ok()) {
      return s;
    }
    my_info_->head_ += my_info_->size_;
    my_info_->size_ = 0;
    if (kVHeaderSize + left > WriteBufferSize) {
      s = dest_->SyncedAppend(Slice(head, kVHeaderSize));
      my_info_->head_ += kVHeaderSize;
      s = dest_->SyncedAppend(Slice(ptr, left));
      my_info_->head_ += left;
    } else {
      memcpy(my_info_->buffer_ + my_info_->size_, head, kVHeaderSize);
      my_info_->size_ += kVHeaderSize;
      memcpy(my_info_->buffer_ + my_info_->size_, ptr, left);
      my_info_->size_ += left;
    }
    return s;
  }

  memcpy(my_info_->buffer_ + my_info_->size_, head, kVHeaderSize);
  my_info_->size_ += kVHeaderSize;
  memcpy(my_info_->buffer_ + my_info_->size_, ptr, left);
  my_info_->size_ += left;

  return s;
}

}  // namespace vlog
}  // namespace leveldb
