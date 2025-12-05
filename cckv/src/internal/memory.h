#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>

namespace cckv::internal {

class Allocator {
 public:
  virtual ~Allocator() = default;

  virtual auto Allocate(size_t bytes) -> char* = 0;
};

// WriteBufferManager aggregates memtable allocations globally.
class WriteBufferManager {
 public:
  virtual ~WriteBufferManager() = default;

  virtual void ReserveMem(size_t mem) = 0;

  virtual void ScheduleFreeMem(size_t mem) = 0;

  virtual void FreeMem(size_t mem) = 0;
};

auto NewWriteBufferManager() -> std::unique_ptr<WriteBufferManager>;

// AllocTracker tracks allocator allocations.
class AllocTracker {
 public:
  explicit AllocTracker(WriteBufferManager* write_buffer_manager)
      : wbm_(write_buffer_manager), bytes_allocated_(0) {}

  // Concurrency safe
  auto Allocate(size_t bytes) -> void {
    assert(wbm_ != nullptr);
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    wbm_->ReserveMem(bytes);
  }

  // No more allocate would happen anymore, in read-only mode.
  // NB: Not concurrency safe.
  auto Done() -> void {
    if (!done_) {
      assert(wbm_ != nullptr);
      wbm_->ScheduleFreeMem(bytes_allocated_.load(std::memory_order_relaxed));
      done_ = true;
    }
  }

  // NB: Not concurrency safe.
  auto Free() -> void {
    if (!done_) {
      Done();
    }
    wbm_->FreeMem(bytes_allocated_.load(std::memory_order_relaxed));
    freed_ = true;
  }

 private:
  WriteBufferManager* wbm_;
  // Counting only, no need for synchronization or any ordering semantics,
  // so use relaxed consistency model is enough.
  std::atomic<size_t> bytes_allocated_;
  bool done_{false};
  bool freed_{false};
};

// Factory for allocator.
auto NewAllocator(AllocTracker* tracker) -> std::unique_ptr<Allocator>;

}  // namespace cckv::internal
