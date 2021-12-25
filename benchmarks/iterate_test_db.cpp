//
// Created by YorkDow Co on 2021/12/19.
//
#include <cassert>

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

#include "util/random.h"

// Number of key/values to place in database
static int FLAGS_num = 1000000;

void ReadSequential(leveldb::DB* db) {
  int reads_ = FLAGS_num;
  leveldb::Iterator* iter = db->NewIterator(leveldb::ReadOptions());
  int i = 0;
  int64_t bytes = 0;
  for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
    //      bytes += iter->key().size() + iter->value().size();
    ++i;
  }
  bytes += iter->datasize();
  delete iter;
}

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status =
      leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
  for (int i = 0; i < 5; i++) {
    ReadSequential(db);
  }
  delete db;
}