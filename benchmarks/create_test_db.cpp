//
// Created by YorkDow Co on 2021/12/19.
//
#include <cassert>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "util/random.h"

// Common key prefix length.
static int FLAGS_key_prefix = 16;

// Number of key/values to place in database
static int FLAGS_num = 1000000;

static double FLAGS_compression_ratio = 0.5;

leveldb::WriteOptions write_options_;

leveldb::Slice RandomString(leveldb::Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
  }
  return *dst;
}

leveldb::Slice CompressibleString(leveldb::Random* rnd,
                                  double compressed_fraction, size_t len,
                                  std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomString(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return *dst;
}

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    leveldb::Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  leveldb::Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return leveldb::Slice(data_.data() + pos_ - len, len);
  }
};

class KeyBuffer {
 public:
  KeyBuffer() {
    assert(FLAGS_key_prefix < sizeof(buffer_));
    memset(buffer_, 'a', FLAGS_key_prefix);
  }
  KeyBuffer& operator=(KeyBuffer& other) = delete;
  KeyBuffer(KeyBuffer& other) = delete;

  void Set(int k) {
    std::snprintf(buffer_ + FLAGS_key_prefix,
                  sizeof(buffer_) - FLAGS_key_prefix, "%016d", k);
  }

  leveldb::Slice slice() const {
    return leveldb::Slice(buffer_, FLAGS_key_prefix + 16);
  }

 private:
  char buffer_[1024]{};
};

leveldb::Random r(998244353);

void DoWrite(leveldb::DB* db, bool seq) {
  int entries_per_batch_ = 1;
  int num_ = FLAGS_num;
  int value_size_ = 100;
  RandomGenerator gen;
  leveldb::WriteBatch batch;
  leveldb::Status s;
  int64_t bytes = 0;
  KeyBuffer key;
  for (int i = 0; i < num_; i += entries_per_batch_) {
    batch.Clear();
    for (int j = 0; j < entries_per_batch_; j++) {
      const int k = seq ? i + j : r.Uniform(FLAGS_num);
      key.Set(k);
      batch.Put(key.slice(), gen.Generate(value_size_));
    }
    s = db->Write(write_options_, &batch);
    if (!s.ok()) {
      std::fprintf(stderr, "put error: %s\n", s.ToString().c_str());
      std::exit(1);
    }
  }
}

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status =
      leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
  DoWrite(db, true);
  DoWrite(db, false);
  DoWrite(db, false);
  delete db;
}