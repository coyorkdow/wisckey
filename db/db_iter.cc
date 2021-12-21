// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include <queue>
#include <thread>
#include <vector>

#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBAddrIter : public Iterator {
  friend class ConcurrenceDBIter;

 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBAddrIter(DBImpl* db, const Comparator* cmp, Iterator* iter,
             SequenceNumber s, uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBAddrIter(const DBAddrIter&) = delete;
  DBAddrIter& operator=(const DBAddrIter&) = delete;

  ~DBAddrIter() override { delete iter_; }
  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  size_t bytes_until_read_sampling_;
};

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

class TaskQueue {
 public:
  TaskQueue() : head_(0), tail_(0), con_(&mutex_) { que_.resize(512); }
  bool Empty() const { return head_ == tail_; }
  std::pair<uint64_t, uint64_t> Pop() { return que_[tail_++ % que_.size()]; }
  void Push(uint64_t i, uint64_t seq) {
    que_[head_++ % que_.size()] = std::make_pair(i, seq);
  }
  void TryExpand() {
    if (head_ - tail_ == que_.size()) {
      que_.resize(que_.size() * 2);
    }
  }
  std::vector<std::pair<uint64_t, uint64_t>> que_;
  size_t head_;
  size_t tail_;
  port::Mutex mutex_;
  port::CondVar con_;
};

class ConcurrenceDBIter : public Iterator {
  friend class DBImpl;
  static constexpr size_t MAX_SIZE = 1024;

 public:
  ConcurrenceDBIter(DBImpl* db, const Comparator* cmp, Iterator* iter,
                    SequenceNumber s, uint32_t seed)
      : dbIter_(db, cmp, iter, s, seed),
        front_(1ULL << 63),
        back_(1ULL << 63),
        cur_index_(1ULL << 63),
        tot_tasks_(0),
        completed_tasks_(0),
        data_size_(0),
        closing_(false) {
    buffer_queue_.resize(MAX_SIZE);
    for (int i = 0; i < 32; i++) {
      threads_.emplace_back(Worker, this, i);
    }
  }

  ConcurrenceDBIter(const ConcurrenceDBIter&) = delete;
  ConcurrenceDBIter& operator=(const ConcurrenceDBIter&) = delete;

  ~ConcurrenceDBIter() override {
    while (completed_tasks_.load(std::memory_order_acquire) != tot_tasks_)
      ;
    closing_.store(true, std::memory_order_release);
    sched_yield();
    task_ques_.con_.SignalAll();
    for (auto& t : threads_) t.join();
  }

  bool Valid() const override {
    return buffer_queue_[cur_index_ % MAX_SIZE].valid_;
  }
  Slice key() const override {
    size_t i = cur_index_ % MAX_SIZE;
    assert(buffer_queue_[i].valid_);
    return buffer_queue_[i].key_;
  }

  uint64_t datasize() const override {
    while (completed_tasks_.load(std::memory_order_acquire) != tot_tasks_)
      ;
    return data_size_;
  }

  Slice value() const override {
    size_t i = cur_index_ % MAX_SIZE;
    assert(buffer_queue_[i].valid_);
    while (buffer_queue_[i].sequence.load(std::memory_order_acquire) !=
           cur_index_)
      ;
    return buffer_queue_[i].val_;
  }

  Status status() const override {
    return buffer_queue_[cur_index_ % MAX_SIZE].status;
  }

  void Next() override {
    cur_index_++;
    if (cur_index_ == back_) {
      for (uint64_t s = cur_index_; s < cur_index_ + 256; s++) {
        dbIter_.Next();
        if (!GetValue(back_++ % MAX_SIZE, s)) break;
      }
      while (back_ - front_ > MAX_SIZE) front_++;
    }
  }

  void Prev() override {
    if (cur_index_ == front_) {
      for (uint64_t s = cur_index_ - 1; s >= cur_index_ - 256; s--) {
        dbIter_.Prev();
        if (!GetValue(--front_ % MAX_SIZE, s)) break;
      }
      while (back_ - front_ > MAX_SIZE) back_--;
    }
    cur_index_--;
  }

  void Seek(const Slice& target) override {
    dbIter_.Seek(target);
    AfterSeek();
  }

  void SeekToFirst() override {
    dbIter_.SeekToFirst();
    AfterSeek();
  }

  void SeekToLast() override {
    dbIter_.SeekToLast();
    AfterSeek();
  }

 private:
  DBAddrIter dbIter_;
  std::vector<IterCache> buffer_queue_;
  size_t cur_index_;
  size_t back_;
  size_t front_;

  void AfterSeek() {
    front_ = 1ULL << 63;
    back_ = 1ULL << 63;
    cur_index_ = 1ULL << 63;
    while (completed_tasks_.load(std::memory_order_acquire) != tot_tasks_)
      ;
    completed_tasks_ = 0;
    tot_tasks_ = 0;
    GetValue(back_++ % MAX_SIZE, cur_index_);
  }

  static void Worker(ConcurrenceDBIter* iter, int q) {
    auto& queue = iter->task_ques_;
    auto db = iter->dbIter_.db_;
    while (true) {
      queue.mutex_.Lock();
      while (queue.Empty()) {
        if (iter->closing_.load(std::memory_order_acquire)) {
          break;
        };
        queue.con_.Wait();
      }

      if (iter->closing_.load(std::memory_order_acquire)) {
        break;
      }

      auto cur = queue.Pop();
      auto& item = iter->buffer_queue_[cur.first];
      uint64_t seq = cur.second;
      queue.mutex_.Unlock();

      db->Fetch(item.addr_, &item.val_);
      item.sequence.store(seq, std::memory_order_release);
      iter->data_size_.fetch_add(item.val_.size(), std::memory_order_release);
      iter->completed_tasks_.fetch_add(1, std::memory_order_relaxed);
    }
    queue.mutex_.Unlock();
  }

  bool GetValue(size_t i, uint64_t seq) {
    buffer_queue_[i].sequence = 0;
    if (!dbIter_.valid_) {
      buffer_queue_[i].valid_ = false;
      return false;
    }
    buffer_queue_[i].valid_ = true;
    buffer_queue_[i].key_ = dbIter_.key().ToString();
    data_size_.fetch_add(buffer_queue_[i].key_.size(),
                         std::memory_order_relaxed);
    if (dbIter_.direction_ == DBAddrIter::kForward) {
      buffer_queue_[i].addr_ = dbIter_.iter_->value().ToString();
    } else {
      buffer_queue_[i].addr_ = dbIter_.saved_value_;
    }
    tot_tasks_++;
    task_ques_.mutex_.Lock();
    if (task_ques_.Empty()) {
      task_ques_.con_.Signal();
    }
    task_ques_.TryExpand();
    task_ques_.Push(i, seq);
    task_ques_.mutex_.Unlock();
    return true;
  }

  std::vector<std::thread> threads_;
  TaskQueue task_ques_;
  std::atomic<bool> closing_;

  uint64_t tot_tasks_;

  alignas(64) std::atomic<uint64_t> completed_tasks_;
  alignas(64) std::atomic<uint64_t> data_size_;
};

inline bool DBAddrIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBAddrIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

void DBAddrIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false;
}

void DBAddrIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break;
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}

void DBAddrIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;
        }
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBAddrIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(&saved_key_,
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBAddrIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBAddrIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new ConcurrenceDBIter(db, user_key_comparator, internal_iter, sequence,
                               seed);
}

Iterator* NewDBAddrIterator(DBImpl* db, const Comparator* user_key_comparator,
                            Iterator* internal_iter, SequenceNumber sequence,
                            uint32_t seed) {
  return new DBAddrIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
