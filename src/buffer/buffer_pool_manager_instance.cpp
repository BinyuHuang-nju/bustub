//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

// Ensure that old page's Evict in replacer and Remove in page table appear in pairs,
// as well as new page's Insert in page table and Pin in replacer.
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = GetVictimPage();
  if (frame_id == INVALID_FRAME_ID) {
    LOG_DEBUG("BufferPoolManagerInstance: Create new page fails for no free frame, no evictable page.");
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  *page_id = AllocatePage();
  LOG_DEBUG("BufferPoolManagerInstance: Reset page with frame id %d, page id %d.", frame_id, page->page_id_);
  ResetPage(page);
  InitPage(page, frame_id, *page_id);
  UpdateFrameInfo(frame_id, page->pin_count_);
  LOG_DEBUG("BufferPoolManagerInstance: Create page with frame id %d, page id %d.", frame_id, page->page_id_);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  if (page_table_->Find(page_id, frame_id)) {
    page = &pages_[frame_id];
    page->pin_count_++;
    UpdateFrameInfo(frame_id, page->pin_count_);
    LOG_DEBUG("BufferPoolManagerInstance: Fetch page %d succeeds for already existing in buffer pool.", page_id);
    return page;
  }
  frame_id = GetVictimPage();
  if (frame_id == INVALID_FRAME_ID) {
    LOG_DEBUG("BufferPoolManagerInstance: Fetch page %d fails for no free frame, no evictable page.", page_id);
    return nullptr;
  }
  page = &pages_[frame_id];
  LOG_DEBUG("BufferPoolManagerInstance: Reset page with frame id %d, page id %d.", frame_id, page_id);
  ResetPage(page);
  InitPage(page, frame_id, page_id);
  disk_manager_->ReadPage(page_id, page->data_);  // after InitPage, only fetch page needs to load page data
  UpdateFrameInfo(frame_id, page->pin_count_);
  LOG_DEBUG("BufferPoolManagerInstance: Fetch page with frame id %d, page id %d.", frame_id, page_id);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_DEBUG("BufferPoolManagerInstance: Unpin a page %d not in buffer pool.", page_id);
    return false;
  }
  page = &pages_[frame_id];
  assert(page->page_id_ == page_id);
  if (page->GetPinCount() == 0) {
    LOG_DEBUG("BufferPoolManagerInstance: Unpin a page %d while its pin count is 0.", page_id);
    return false;
  }
  UnpinFrame(page, frame_id);
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  LOG_DEBUG("BufferPoolManagerInstance: Unpin a page %d, and then its pin count is %d.", page_id, page->pin_count_);
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_DEBUG("BufferPoolManagerInstance: Flush a page %d not in buffer pool.", page_id);
    return false;
  }
  page = &pages_[frame_id];
  assert(page->page_id_ == page_id);
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  LOG_DEBUG("BufferPoolManagerInstance: Flush a page %d to disk.", page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page = nullptr;
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    page = &pages_[frame_id];
    if (page->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
  LOG_DEBUG("BufferPoolManagerInstance: Flush all pages to disk.");
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_DEBUG("BufferPoolManagerInstance: Delete page %d succeeds for not existing in buffer pool.", page_id);
    return true;
  }
  page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    LOG_DEBUG("BufferPoolManagerInstance: Delete page %d fails for page being pinned.", page_id);
    return false;
  }
  if (ResetPage(page)) {
    // we need to remove frame in replacer here since in other conditions frame evicted from replacer.
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
    LOG_DEBUG("BufferPoolManagerInstance: Delete page %d succeeds.", page_id);
    return true;
  }
  LOG_WARN("BufferPoolManagerInstance: Delete page %d fails for page not found.", page_id);
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetVictimPage() -> frame_id_t {
  // achieve lock outside
  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    assert(pages_[frame_id].page_id_ == INVALID_PAGE_ID);
    LOG_DEBUG("BufferPoolManagerInstance: Get victim page from free list with frame id %d.", frame_id);
    return frame_id;
  }
  if (replacer_->Evict(&frame_id)) {
    LOG_DEBUG("BufferPoolManagerInstance: Get victim page by replacer with frame id %d.", frame_id);
    return frame_id;
  }
  LOG_DEBUG("BufferPoolManagerInstance: Get victim page fails.");
  return frame_id;
}

auto BufferPoolManagerInstance::ResetPage(Page *page) -> bool {
  if (page->page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  if (page_table_->Remove(page->page_id_)) {
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->ResetMemory();
    return true;
  }
  LOG_WARN("BufferPoolManagerInstance: page id %d valid but not in page table.", page->page_id_);
  return false;
}

auto BufferPoolManagerInstance::InitPage(Page *page, frame_id_t frame_id, page_id_t page_id) -> bool {
  assert(page->page_id_ == INVALID_PAGE_ID && !page->is_dirty_ && page->pin_count_ == 0);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page_table_->Insert(page_id, frame_id);
  return true;
}

auto BufferPoolManagerInstance::UpdateFrameInfo(frame_id_t frame_id, int pin_count) -> bool {
  replacer_->RecordAccess(frame_id);
  if (pin_count == 1) {
    replacer_->SetEvictable(frame_id, false);
  }
  return true;
}

void BufferPoolManagerInstance::UnpinFrame(Page *page, frame_id_t frame_id) {
  assert(page->GetPinCount() > 0);
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
}

}  // namespace bustub
