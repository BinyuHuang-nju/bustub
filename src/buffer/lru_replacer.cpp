//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages): page_size_(num_pages), cur_size_(0) {
  dummy_node_ = new ListNode(-10000);
  dummy_node_->next_ = dummy_node_->prev_ = dummy_node_;
  if (page_size_ < 2) {
    LOG_WARN("LRUReplacer: page size lower than 2 which is unnecessary.");
  }
}

LRUReplacer::~LRUReplacer() {
  if (dummy_node_ != nullptr) {
    ListNode *nxt;
    while (dummy_node_->next_ != dummy_node_) {
      nxt = dummy_node_->next_;
      dummy_node_->next_ = nxt->next_;
      delete nxt;
    }
    delete dummy_node_;
    dummy_node_ = nullptr;
  }
  cur_size_ = 0;
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (cur_size_ == 0) {
    return false;
  }
  assert(dummy_node_->prev_ != nullptr);
  ListNode* least_used = dummy_node_->prev_;
  *frame_id = least_used->frame_id_;
  dummy_node_->prev_ = least_used->prev_;
  least_used->prev_->next_ = dummy_node_;
  frames_.erase(*frame_id);
  delete least_used;
  cur_size_--;
  LOG_DEBUG("LRUReplacer: Victim frame id %d.", *frame_id);
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if(frames_.count(frame_id) == 0) {
    LOG_DEBUG("LRUReplacer: Pin frame id %d fails for not existing in replacer.", frame_id);
    return;
  }
  ListNode* pinned_node = frames_[frame_id];
  pinned_node->prev_->next_ = pinned_node->next_;
  pinned_node->next_->prev_ = pinned_node->prev_;
  frames_.erase(frame_id);
  delete pinned_node;
  cur_size_--;
  LOG_DEBUG("LRUReplacer: Pin frame id %d succeeds.", frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_.count(frame_id) != 0) {
    LOG_DEBUG("LRUReplacer: Unpin frame id %d fails for already existing in replacer.", frame_id);
    return;
  }
  if (cur_size_ == page_size_) {
    LOG_DEBUG("LRUReplacer: Unpin frame id %d fails for replacer size %zu full.", frame_id, cur_size_);
    return;
  }
  ListNode* unpinned_node = new ListNode(frame_id);
  unpinned_node->prev_ = dummy_node_;
  unpinned_node->next_ = dummy_node_->next_;
  dummy_node_->next_ = unpinned_node;
  unpinned_node->next_->prev_ = unpinned_node;
  frames_[frame_id] = unpinned_node;
  cur_size_++;
  LOG_DEBUG("LRUReplacer: Unpin frame id %d succeeds.", frame_id);
}

auto LRUReplacer::Size() -> size_t { return cur_size_; }

}  // namespace bustub
