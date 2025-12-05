#include "memory.h"

#include <cstddef>
#include <memory>

#include "gtest/gtest.h"

namespace cckv::internal {

class FakeWriteBufferManager : public WriteBufferManager {
 public:
  void ReserveMem(size_t mem) override {
    used_ += mem;
    active_ += mem;
  }

  void ScheduleFreeMem(size_t mem) override { active_ -= mem; }

  void FreeMem(size_t mem) override { used_ -= mem; }

  size_t used_{0};
  size_t active_{0};
};

TEST(Memory, RawAllocatorTracksUsage) {
  FakeWriteBufferManager wbm;
  AllocTracker tracker(&wbm);
  {
    auto alloc = NewAllocator(&tracker);
    char* a = alloc->Allocate(16);
    char* b = alloc->Allocate(32);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_NE(a, b);

    EXPECT_EQ(wbm.used_, static_cast<size_t>(48));
    EXPECT_EQ(wbm.active_, static_cast<size_t>(48));

    // Zero-size allocation should no-op
    EXPECT_EQ(alloc->Allocate(0), nullptr);
    EXPECT_EQ(wbm.used_, static_cast<size_t>(48));

    // Mark allocation done decrease the active
    tracker.Done();
    EXPECT_EQ(wbm.used_, static_cast<size_t>(48));
    EXPECT_EQ(wbm.active_, static_cast<size_t>(0));
  }

  // Destructor should release reservations via tracker.
  EXPECT_EQ(wbm.used_, static_cast<size_t>(0));
  EXPECT_EQ(wbm.active_, static_cast<size_t>(0));
}

}  // namespace cckv::internal
