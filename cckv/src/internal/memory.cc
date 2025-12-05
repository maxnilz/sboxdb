#include "memory.h"

#include <vector>

namespace cckv::internal {

// Raw allocator that use the raw new/delete for memory allocation.
// NB: Not concurrency safe.
class RawAllocator final : public Allocator {
 public:
  explicit RawAllocator(AllocTracker *tracker) : tracker_(tracker) {}

  RawAllocator(RawAllocator &) = delete;
  auto operator=(RawAllocator &) -> RawAllocator & = delete;

  ~RawAllocator() override {
    if (tracker_ != nullptr) {
      tracker_->Free();
    }
  }

  auto Allocate(const size_t bytes) -> char * override {
    if (bytes == 0) {
      return nullptr;
    }
    if (tracker_ != nullptr) {
      tracker_->Allocate(bytes);
    }
    char *block = new char[bytes];
    blocks_.push_back(std::unique_ptr<char[]>(block));
    blocks_memory_ += bytes;
    return block;
  }

 private:
  // Bytes of memory in blocks allocated so far
  size_t blocks_memory_ = 0;
  // Allocated memory blocks
  std::vector<std::unique_ptr<char[]>> blocks_;
  AllocTracker *tracker_;  // non-owning
};

class SizedWriteBufferManager final : public WriteBufferManager {
 public:
  explicit SizedWriteBufferManager(const size_t buffer_size)
      : buffer_size_(buffer_size), memory_used_(0), memory_active_(0) {};

  ~SizedWriteBufferManager() override = default;

  void ReserveMem(const size_t mem) override {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
  }

  void ScheduleFreeMem(const size_t mem) override {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }

  void FreeMem(const size_t mem) override {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }

 private:
  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
};

auto NewWriteBufferManager() -> std::unique_ptr<WriteBufferManager> {
  return std::make_unique<SizedWriteBufferManager>(0);
}

auto NewAllocator(AllocTracker *tracker) -> std::unique_ptr<Allocator> {
  return std::make_unique<RawAllocator>(tracker);
}
}  // namespace cckv::internal
