//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

const bool LOG_LRU_ENABLE = false;
#define LOG_LRU_DEBUG(...)                                                      \
  do {                                                                          \
    if (LOG_LRU_ENABLE) {                                                       \
      OutputLogHeader(__SHORT_FILE__, __LINE__, __FUNCTION__, LOG_LEVEL_DEBUG); \
      ::fprintf(LOG_OUTPUT_STREAM, __VA_ARGS__);                                \
      fprintf(LOG_OUTPUT_STREAM, "\n");                                         \
      ::fflush(stdout);                                                         \
    }                                                                           \
  } while (0)

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer();

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  struct ListNode {
    frame_id_t frame_id_;
    size_t access_times_;
    bool evictable_;
    ListNode *prev_{nullptr}, *next_{nullptr};

    // ListNode() : frame_id_(-1), access_times_(0), evictable_(false), prev_(nullptr), next_(nullptr) {}

    explicit ListNode(frame_id_t id, ListNode *pre = nullptr, ListNode *nxt = nullptr)
        : frame_id_(id), access_times_(0), evictable_(false), prev_(pre), next_(nxt) {}
  };

  auto GetNode(frame_id_t frame_id) -> ListNode *;

  void InsertNode(ListNode *dummy, ListNode *newNode);

  void MoveNodeToHead(ListNode *dummy, ListNode *node);

  void EvictNodeFromList(ListNode *node);

  auto GetFirstEvictableNode(bool in_history) -> ListNode *;

  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};   // size of evictable frames
  size_t replacer_size_;  // size of buffer pool, namely all frames
  size_t k_;
  std::mutex latch_;
  ListNode *history_dummy_;
  std::unordered_map<frame_id_t, ListNode *> history_frames_;
  size_t history_frame_size_{0};
  ListNode *buffer_dummy_;
  std::unordered_map<frame_id_t, ListNode *> buffer_frames_;
  size_t buffer_frame_size_{0};
};

}  // namespace bustub
