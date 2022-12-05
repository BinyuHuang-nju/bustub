//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  std::shared_ptr<Bucket> bucket0 = std::make_shared<Bucket>(bucket_size, 0);
  dir_.push_back(bucket0);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
  // size_t mask = ~((1 << (32 - global_depth_)) - 1);
  // size_t hash_value = std::hash<K>()(key);
  // LOG_DEBUG("In IndexOf, hash value is %zx.", hash_value);
  // return (hash_value & mask) >> (32 - global_depth_);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::PairIndex(size_t bucket_no, int local_depth) -> size_t {
  return bucket_no ^ (1 << (local_depth - 1));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[idx];
  bucket->GetReadLock();
  latch_.unlock();
  if (LOG_ENABLE) {
    LOG_DEBUG("ExtendibleHashTable: Find <k, v> with bucket index %zu.", idx);
  }
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  latch_.lock();
  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = dir_[idx];
  bucket->GetWriteLock();
  latch_.unlock();
  if (LOG_ENABLE) {
    LOG_DEBUG("ExtendibleHashTable: Remove <k, v> with bucket index %zu.", idx);
  }
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  bool complete = false;
  while (!complete) {
    size_t bucket_no = IndexOf(key);
    if (LOG_ENABLE) {
      LOG_DEBUG("ExtendibleHashTable: Insert into bucket with index %zu.", bucket_no);
    }
    std::shared_ptr<Bucket> bucket = dir_[bucket_no];
    bucket->GetWriteLock();
    bool success = bucket->Insert(key, value);
    if (!success) {
      if (LOG_ENABLE) {
        LOG_DEBUG("ExtendibleHashTable: Insert bucket with index %zu and local depth %d is full.", bucket_no,
                  bucket->GetDepth());
      }
      bucket->IncrementDepth();
      int local_depth = bucket->GetDepth();
      if (local_depth > global_depth_) {
        for (int i = 0; i < 1 << global_depth_; i++) {
          dir_.push_back(dir_[i]);
        }
        global_depth_++;
        if (LOG_ENABLE) {
          LOG_DEBUG("ExtendibleHashTable: Global depth increments to %d for local depth increments.", global_depth_);
        }
      }
      size_t pair_index = PairIndex(bucket_no, local_depth);
      dir_[pair_index] = std::make_shared<Bucket>(bucket_size_, local_depth);
      std::list<std::pair<K, V>> itemsToInsert = bucket->GetCopiedItems();
      bucket->ClearItems();
      bucket->ReleaseWriteLock();
      size_t index_diff = 1 << local_depth;
      size_t dir_size = 1 << global_depth_;
      for (int i = pair_index - index_diff; i >= 0; i -= index_diff) {
        if (LOG_ENABLE) {
          LOG_DEBUG("ExtendibleHashTable: directory[%d] points to directory[%zu].", i, pair_index);
        }
        dir_[i] = dir_[pair_index];
      }
      for (size_t i = pair_index + index_diff; i < dir_size; i += index_diff) {
        if (LOG_ENABLE) {
          LOG_DEBUG("ExtendibleHashTable: directory[%zu] points to directory[%zu].", i, pair_index);
        }
        dir_[i] = dir_[pair_index];
      }
      for (auto iter = itemsToInsert.begin(); iter != itemsToInsert.end(); iter++) {
        size_t cur_bucket_no = IndexOf((*iter).first);
        dir_[cur_bucket_no]->GetWriteLock();
        if (LOG_ENABLE) {
          LOG_DEBUG("ExtendibleHashTable: For split, insert into bucket with index %zu and depth %d.", cur_bucket_no,
                    dir_[cur_bucket_no]->GetDepth());
        }
        if (!dir_[cur_bucket_no]->Insert((*iter).first, (*iter).second)) {
          LOG_WARN("ExtendibleHashTable: Split bucket but insert fail.");
        }
        dir_[cur_bucket_no]->ReleaseWriteLock();
      }
    } else {
      bucket->ReleaseWriteLock();
    }
    complete = success;
  }
  latch_.unlock();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {}

/**
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  latch_.lock();
  bool complete = false;
  while (!complete) {
    size_t idx = IndexOf(key);
    LOG_DEBUG("ExtendibleHashTable: Insert (k, v) into bucket with index %zu", idx);
    std::shared_ptr<Bucket> bucket = dir_[idx];
    bucket->GetWriteLock();
    bool success = bucket->Insert(key, value);
    if (!success) {
      LOG_DEBUG("ExtendibleHashTable: <Insert> bucket with index %zu and local depth %d is full.", idx,
                bucket->GetDepth());
      // the bucket is full, time to modify local depth and split bucket
      bool needReLocate = false;
      bucket->IncrementDepth();
      std::shared_ptr<Bucket> old_bucket =
          std::make_shared<Bucket>(bucket_size_, bucket->GetDepth(), bucket->GetPrefix());
      std::shared_ptr<Bucket> new_bucket =
          std::make_shared<Bucket>(bucket_size_, bucket->GetDepth(), bucket->GetPrefix() + 1);

      if (global_depth_ < bucket->GetDepth()) {
        needReLocate = true;
        global_depth_++;
        LOG_DEBUG("ExtendibleHashTable: <Insert> global depth increment to %d.", global_depth_);
      }

      if (!needReLocate) {
        size_t nums_from_new_bucket = 1 << (global_depth_ - bucket->GetDepth());
        size_t current_index = bucket->GetPrefix() << (global_depth_ - bucket->GetDepth());
        for (size_t i = 0; i < nums_from_new_bucket; i++, current_index++) {
          assert(dir_[current_index] == bucket);
          LOG_DEBUG("ExtendibleHashTable: <Insert> index %zu belongs to old bucket.", current_index);
          dir_[current_index] = old_bucket;
        }
        for (size_t i = 0; i < nums_from_new_bucket; i++, current_index++) {
          assert(dir_[current_index] == bucket);
          LOG_DEBUG("ExtendibleHashTable: <Insert> index %zu belongs to new bucket.", current_index);
          dir_[current_index] = new_bucket;
        }
      } else {
        std::vector<std::shared_ptr<Bucket>> new_dir(1 << global_depth_, nullptr);
        for (size_t i = 0; i < (1 << (global_depth_ - 1)); i++) {
          new_dir[i << 1] = dir_[i];
          new_dir[(i << 1) + 1] = dir_[i];
        }
        size_t nums_from_new_bucket = 1 << (global_depth_ - bucket->GetDepth());
        size_t current_index = bucket->GetPrefix() << (global_depth_ - bucket->GetDepth());
        for (size_t i = 0; i < nums_from_new_bucket; i++, current_index++) {
          assert(dir_[current_index >> 1] == bucket);
          LOG_DEBUG("ExtendibleHashTable: <Insert> index %zu belongs to old bucket.", current_index);
          new_dir[current_index] = old_bucket;
        }
        for (size_t i = 0; i < nums_from_new_bucket; i++, current_index++) {
          assert(dir_[current_index >> 1] == bucket);
          LOG_DEBUG("ExtendibleHashTable: <Insert> index %zu belongs to new bucket.", current_index);
          new_dir[current_index] = new_bucket;
        }
        dir_ = new_dir;
      }

      std::list<std::pair<K, V>> list = bucket->GetItems();
      for (auto iter = list.begin(); iter != list.end(); iter++) {
        size_t index = IndexOf((*iter).first) >> (global_depth_ - bucket->GetDepth());
        assert(dir_[index] == old_bucket || dir_[index] == new_bucket);
        if (!dir_[index]->Insert((*iter).first, (*iter).second)) {
          assert(false);
        }
      }
      num_buckets_++;
    }
    bucket->ReleaseWriteLock();
    complete = success;
  }
  latch_.unlock();
}
*/

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth, size_t prefix)
    : size_(array_size), depth_(depth), prefix_(prefix) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // for operation Find, we need to add read lock outside.
  // latch_.RLock();
  assert(list_.size() <= size_);
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      value = iter->second;
      latch_.RUnlock();
      // LOG_DEBUG("Bucket: Find key success.");
      return true;
    }
  }
  latch_.RUnlock();
  // LOG_DEBUG("Bucket: Find key fail.");
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // For operation Remove, we need to add write lock outside.
  // latch_.WLock();
  assert(list_.size() <= size_);
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.erase(iter);
      latch_.WUnlock();
      // LOG_DEBUG("Bucket: Remove key success.");
      return true;
    }
  }
  latch_.WUnlock();
  // LOG_DEBUG("Bucket: Remove key fail.");
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // for operation Insert, we achieve and release lock outside,
  // to ensure in one Insert of hash table, one or more Inserts of Bucket
  // are locked without any other operations exist in the middle.
  if (IsOverflow()) {
    // LOG_WARN("Bucket: Before Insert size overflow.");
    return false;
  }
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      // LOG_DEBUG("Bucket: Insert <k, v> and k already exists in bucket.");
      (*iter).second = value;
      return true;
    }
  }
  if (IsFull()) {
    // LOG_DEBUG("Bucket: Before Insert size full.");
    return false;
  }
  list_.push_back(std::make_pair(key, value));
  // LOG_DEBUG("Bucket: Insert <k, v> success, current num %zu.", list_.size());
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
