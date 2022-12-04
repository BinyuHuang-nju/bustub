//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): implement me!
  struct ListNode {
    frame_id_t frame_id_;
    ListNode *prev_, *next_;

    ListNode() : frame_id_(0), prev_(nullptr), next_(nullptr) {}

    explicit ListNode(frame_id_t id, ListNode *pre = nullptr, ListNode *nxt = nullptr)
        : frame_id_(id), prev_(pre), next_(nxt) {}
  };

  size_t page_size_;
  mutable std::mutex latch_;
  ListNode *dummy_node_;
  size_t cur_size_;
  std::unordered_map<frame_id_t, ListNode*> frames_;
};

}  // namespace bustub
