#include "merging_iterator.h"

#include "codec.h"
#include "gtest/gtest.h"
#include "memory.h"
#include "memtable.h"
namespace cckv::internal {
TEST(MergingIterator, MinHeap) {
  enum Op : unsigned char { kPush, kPop };
  // cases in format of tuple <Op, PushingValue, TopValue>, where
  // PushingValue is ignored in case of Pop.
  std::vector<std::tuple<Op, int, int>> cases = {
      {kPush, 3, 3},  // 3
      {kPush, 2, 2},  // 2, 3
      {kPush, 1, 1},  // 1, 2, 3
      {kPush, 5, 1},  // 1, 2, 3, 5
      {kPop, 0, 2},   // pop 1
      {kPop, 0, 3},   // pop 2
      {kPop, 0, 5},   // pop 3
      {kPush, 4, 4},  // 4, 5
      {kPush, 0, 0},  // 0, 4, 5
      {kPop, 0, 4},   // pop 0
      {kPop, 0, 5},   // pop 4
  };

  MinHeap<int> heap;
  for (auto& [op, pushing_value, top_value] : cases) {
    switch (op) {
      case kPush:
        heap.Push(pushing_value);
        break;
      case kPop:
        heap.Pop();
        break;
    }
    auto top = heap.Top();
    auto s = op == kPush ? "push " + std::to_string(pushing_value) : "pop";
    ASSERT_EQ(top_value, top) << s << ", expect " << top_value << ", got " << top;
  }
}

struct OwnedAllocator {
  OwnedAllocator() {
    wmb_ = NewWriteBufferManager();
    tracker_ = std::make_unique<AllocTracker>(wmb_.get());
    inner_ = NewAllocator(tracker_.get());
  }

  std::unique_ptr<WriteBufferManager> wmb_;
  std::unique_ptr<AllocTracker> tracker_;
  std::unique_ptr<Allocator> inner_;
};

struct OwnedMemtable {
  OwnedMemtable() { inner_ = NewMemTable(alloc_.inner_.get()); }

  OwnedAllocator alloc_{};
  std::unique_ptr<MemTable> inner_;
};

TEST(MergingIterator, Memtables) {
  auto table2 = OwnedMemtable();  // level 2, oldest level
  auto table1 = OwnedMemtable();  // level 1, older level
  auto table0 = OwnedMemtable();  // level 0, newest level

  //
  // --L0-----------------------------------------------------------------------
  // del/range_del:                                                      [k8@37--k9@37)
  // del/range_del:                                [k5@36----------k7@36)
  // del/range_del:           k2@31, [k3@33----------------k6@33)
  // point keys   :    k1@30,              k4@32                   k7@34, k8@35
  // --L1-----------------------------------------------------------------------
  // del/range_del:           [k2@22-------k4@22)
  // point keys   :    k1@20,        k3@21,        k5@23, k6@24
  // --L2------------------------------------------------------------------------
  // point keys   :    k1@10, k2@11, k3@12, k4@13, k5@14
  //

  // table0: newest level with seq 30-35
  // Point keys: k1, k4, k7, k8
  // Point deletion: k2@31
  // Range tombstone: [k3, k6)@33, should delete keys k3, k4, k5 at seq <=33
  //                  [k5, k7)@36, should delete keys k5, k6 at seq <= 36
  //                  [k8, k9)@37, should delete keys k8 at seq <= 38
  table0.inner_->Add(InternalKey(Slice("k1"), 30, ValueType::kTypeValue), Slice("v1@30"));
  table0.inner_->Add(InternalKey(Slice("k2"), 31, ValueType::kTypeDeletion), Slice());
  table0.inner_->Add(InternalKey(Slice("k4"), 32, ValueType::kTypeValue), Slice("v4@32"));
  table0.inner_->Add(InternalKey(Slice("k3"), 33, ValueType::kTypeRangeDeletion), Slice("k6"));
  table0.inner_->Add(InternalKey(Slice("k7"), 34, ValueType::kTypeValue), Slice("v7@34"));
  table0.inner_->Add(InternalKey(Slice("k8"), 35, ValueType::kTypeValue), Slice("v8@35"));
  table0.inner_->Add(InternalKey(Slice("k5"), 36, ValueType::kTypeRangeDeletion), Slice("k7"));
  table0.inner_->Add(InternalKey(Slice("k8"), 37, ValueType::kTypeRangeDeletion), Slice("k9"));

  // table1: level 1 with seq 20-24
  // Point keys: k1, k3, k5, k6
  // Range tombstone: [k2, k4) @ seq 22 - should delete keys k2 and k3 at seq <= 22
  table1.inner_->Add(InternalKey(Slice("k1"), 20, ValueType::kTypeValue), Slice("v1@20"));
  table1.inner_->Add(InternalKey(Slice("k3"), 21, ValueType::kTypeValue), Slice("v3@21"));
  table1.inner_->Add(InternalKey(Slice("k2"), 22, ValueType::kTypeRangeDeletion), Slice("k4"));
  table1.inner_->Add(InternalKey(Slice("k5"), 23, ValueType::kTypeValue), Slice("v5@23"));
  table1.inner_->Add(InternalKey(Slice("k6"), 24, ValueType::kTypeValue), Slice("v6@24"));

  // table2: level 2 with seq 10-14 (oldest)
  // Keys: k1, k2, k3, k4, k5
  table2.inner_->Add(InternalKey(Slice("k1"), 10, ValueType::kTypeValue), Slice("v1@10"));
  table2.inner_->Add(InternalKey(Slice("k2"), 11, ValueType::kTypeValue), Slice("v2@11"));
  table2.inner_->Add(InternalKey(Slice("k3"), 12, ValueType::kTypeValue), Slice("v3@12"));
  table2.inner_->Add(InternalKey(Slice("k4"), 13, ValueType::kTypeValue), Slice("v4@13"));
  table2.inner_->Add(InternalKey(Slice("k5"), 14, ValueType::kTypeValue), Slice("v5@14"));

  std::vector<std::pair<SeqNum, std::vector<std::string>>> cases{
      {0,
       {
           "k1@30",
           "k1@20",
           "k1@10",
           "k2@31_del",
           "k2@11",
           "k3@21",
           "k3@12",
           "k4@32",
           "k4@13",
           "k5@23",
           "k5@14",
           "k6@24",
           "k7@34",
           "k8@35",
       }},
      {22,
       {
           "k1@30",
           "k1@20",
           "k1@10",
           "k2@31_del",
           "k4@32",
           "k4@13",
           "k5@23",
           "k5@14",
           "k6@24",
           "k7@34",
           "k8@35",
       }},
      {33,
       {
           "k1@30",
           "k1@20",
           "k1@10",
           "k2@31_del",
           "k6@24",
           "k7@34",
           "k8@35",
       }},
      {36,
       {
           "k1@30",
           "k1@20",
           "k1@10",
           "k2@31_del",
           "k7@34",
           "k8@35",
       }},
      {37,
       {
           "k1@30",
           "k1@20",
           "k1@10",
           "k2@31_del",
           "k7@34",
       }},
  };

  for (auto& [seq_num, expected] : cases) {
    auto comp = InternalKeyComparator();
    auto alloc = OwnedAllocator();
    auto builder = MergingIteratorBuilder(comp, alloc.inner_.get());

    auto iter0 = table0.inner_->NewIterator();
    auto tombstone_iter0 = table0.inner_->NewRangeTombstoneIterator(seq_num);
    builder.AddIterator(iter0.get(), tombstone_iter0.get());

    auto iter1 = table1.inner_->NewIterator();
    auto tombstone_iter1 = table1.inner_->NewRangeTombstoneIterator(seq_num);
    builder.AddIterator(iter1.get(), tombstone_iter1.get());

    auto iter2 = table2.inner_->NewIterator();
    auto tombstone_iter2 = table2.inner_->NewRangeTombstoneIterator(seq_num);
    builder.AddIterator(iter2.get(), tombstone_iter2.get());

    auto* merge_iter = builder.Build();
    merge_iter->SeekToFirst();

    std::vector<std::string> results;
    while (merge_iter->Valid()) {
      auto key = merge_iter->Key();
      results.emplace_back(key.DebugString());
      merge_iter->Next();
    }
    ASSERT_EQ(expected, results) << "merging iter with tombstone upper bound at seq " << seq_num;
  }
}
}  // namespace cckv::internal