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
  char buf[kVHeaderSize];
  uint32_t crc = crc32c::Extend(0, ptr, left);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);
  EncodeFixed64(&buf[4], left);
  Status s = dest_->Append(Slice(buf, kVHeaderSize));
  assert(s.ok());
  if (s.ok()) {
    //    std::string t;
    //    t.push_back(static_cast<char>(kTypeDeletion));
    //    dest_->Append(t);
    s = dest_->Append(Slice(ptr, left));
    assert(s.ok());
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  return s;
}

}  // namespace vlog
}  // namespace leveldb
