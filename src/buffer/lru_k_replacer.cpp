//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  history_dummy_ = new ListNode(-10);
  history_dummy_->prev_ = history_dummy_->next_ = history_dummy_;
  buffer_dummy_ = new ListNode(-100);
  buffer_dummy_->prev_ = buffer_dummy_->next_ = buffer_dummy_;
}

LRUKReplacer::~LRUKReplacer() {
  if (history_dummy_ != nullptr) {
    ListNode *nxt;
    while (history_dummy_->next_ != history_dummy_) {
      nxt = history_dummy_->next_;
      history_dummy_->next_ = nxt->next_;
      delete nxt;
    }
    delete history_dummy_;
    history_dummy_ = nullptr;
  }
  if (buffer_dummy_ != nullptr) {
    ListNode *nxt;
    while (buffer_dummy_->next_ != buffer_dummy_) {
      nxt = buffer_dummy_->next_;
      buffer_dummy_->next_ = nxt->next_;
      delete nxt;
    }
    delete buffer_dummy_;
    buffer_dummy_ = nullptr;
  }
  curr_size_ = history_frame_size_ = buffer_frame_size_ = 0;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    LOG_LRU_DEBUG("LRUKReplacer: [Evict] there is no frame evictable.");
    return false;
  }
  bool in_history = true;
  ListNode *node = GetFirstEvictableNode(true);
  if (node == nullptr) {
    in_history = false;
    node = GetFirstEvictableNode(false);
  }
  assert(node != nullptr);
  *frame_id = node->frame_id_;
  EvictNodeFromList(node);
  if (in_history) {
    history_frames_.erase(*frame_id);
    history_frame_size_--;
    LOG_LRU_DEBUG("LRUKReplacer: Evict frame id %d from history list, after that size %zu.", *frame_id,
              history_frame_size_);
  } else {
    buffer_frames_.erase(*frame_id);
    buffer_frame_size_--;
    LOG_LRU_DEBUG("LRUKReplacer: Evict frame id %d from cache list, after that size %zu.", *frame_id, buffer_frame_size_);
  }
  delete node;
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert((size_t)frame_id <= replacer_size_);
  ListNode *node = GetNode(frame_id);
  if (node == nullptr) {
    if (history_frame_size_ + buffer_frame_size_ == replacer_size_) {
      LOG_WARN("LRUKReplacer: replacer full, for %zu frames in history, %zu frames in cache.", history_frame_size_,
               buffer_frame_size_);
      return;
    }
    node = new ListNode(frame_id);
    InsertNode(history_dummy_, node);
    history_frames_[frame_id] = node;
    history_frame_size_++;
    LOG_LRU_DEBUG("LRUKReplacer: Insert frame id %d into history list.", frame_id);
  }
  node->access_times_++;
  if (node->access_times_ == k_) {
    EvictNodeFromList(node);
    history_frames_.erase(frame_id);
    history_frame_size_--;
    InsertNode(buffer_dummy_, node);
    buffer_frames_[frame_id] = node;
    buffer_frame_size_++;
    LOG_LRU_DEBUG("LRUKReplacer: Move frame id %d from history list to cache list.", frame_id);
  } else {
    if (node->access_times_ < k_) {
      if (node->access_times_ != 1) {
        MoveNodeToHead(history_dummy_, node);
        LOG_LRU_DEBUG("LRUKReplacer: Move frame id %d to head in history list.", frame_id);
      }
    } else {
      MoveNodeToHead(buffer_dummy_, node);
      LOG_LRU_DEBUG("LRUKReplacer: Move frame id %d to head in cache list.", frame_id);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert((size_t)frame_id <= replacer_size_);
  ListNode *node = GetNode(frame_id);
  if (node == nullptr) {
    LOG_LRU_DEBUG("LRUKReplacer: [SetEvictable] node with frame id %d not exists.", frame_id);
    return;
  }
  if (node->evictable_ == set_evictable) {
    LOG_LRU_DEBUG("LRUKReplacer: [SetEvictable] node with frame id %d ecictable is %d.", frame_id, set_evictable);
    return;
  }
  node->evictable_ = set_evictable;
  curr_size_ += set_evictable ? 1 : -1;
  LOG_LRU_DEBUG("LRUKReplacer: [SetEvictable] node with frame id %d has set ecictable to %d.", frame_id, set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  assert((size_t)frame_id <= replacer_size_);
  ListNode *node = GetNode(frame_id);
  if (node == nullptr || !node->evictable_) {
    LOG_LRU_DEBUG("LRUKReplacer: Remove frame id %d fails for not exists or non-evictable.", frame_id);
    return;
  }
  EvictNodeFromList(node);
  bool in_history = history_frames_.count(frame_id) != 0;
  if (in_history) {
    history_frames_.erase(frame_id);
    history_frame_size_--;
    LOG_LRU_DEBUG("LRUKReplacer: Remove frame id %d succeeds in history list, after that size %zu.", frame_id,
              history_frame_size_);
  } else {
    buffer_frames_.erase(frame_id);
    buffer_frame_size_--;
    LOG_LRU_DEBUG("LRUKReplacer: Remove frame id %d succeeds in cache list, after that size %zu.", frame_id,
              buffer_frame_size_);
  }
  delete node;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::GetNode(frame_id_t frame_id) -> LRUKReplacer::ListNode * {
  size_t occurrence = 0;
  ListNode *node = nullptr;
  if (history_frames_.count(frame_id) != 0) {
    occurrence++;
    node = history_frames_[frame_id];
  }
  if (buffer_frames_.count(frame_id) != 0) {
    occurrence++;
    node = buffer_frames_[frame_id];
  }
  assert(occurrence < 2);
  return node;
}

void LRUKReplacer::InsertNode(LRUKReplacer::ListNode *dummy, LRUKReplacer::ListNode *newNode) {
  newNode->prev_ = dummy;
  newNode->next_ = dummy->next_;
  dummy->next_ = newNode;
  newNode->next_->prev_ = newNode;
}

void LRUKReplacer::MoveNodeToHead(LRUKReplacer::ListNode *dummy, LRUKReplacer::ListNode *node) {
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
  node->prev_ = dummy;
  node->next_ = dummy->next_;
  dummy->next_ = node;
  node->next_->prev_ = node;
}

void LRUKReplacer::EvictNodeFromList(LRUKReplacer::ListNode *node) {
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
}

auto LRUKReplacer::GetFirstEvictableNode(bool in_history) -> LRUKReplacer::ListNode * {
  ListNode *head = in_history ? history_dummy_ : buffer_dummy_;
  ListNode *node = head->prev_;
  for (; node != head; node = node->prev_) {
    if (node->evictable_) {
      return node;
    }
  }
  return nullptr;
}

}  // namespace bustub
