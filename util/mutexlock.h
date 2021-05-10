// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
#define STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class SCOPED_LOCKABLE MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu) : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;

 private:
  port::Mutex* const mu_;
};

class SCOPED_LOCKABLE WLock {
 public:
  explicit WLock(port::SharedMutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : free(false), mu_(mu) {
    this->mu_->UniqueLock();
  }

  ~WLock() UNLOCK_FUNCTION() {
    if (!free) this->mu_->UniqueUnlock();
  }

  void Unlock() {
    this->mu_->AssertHeld();
    this->mu_->UniqueUnlock();
    free = true;
  }

  WLock(const WLock&) = delete;
  WLock& operator=(const WLock&) = delete;

 private:
  bool free;
  port::SharedMutex* const mu_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_MUTEXLOCK_H_
