#pragma once
#include <algorithm>
#include <cassert>
#include <functional>

#include "memory.h"
#include "range_tombstone.h"

namespace cckv::internal {

class MergingIterator;

class MergingIteratorBuilder {
 public:
  explicit MergingIteratorBuilder(InternalKeyComparator comp, Allocator* alloc);
  MergingIteratorBuilder(MergingIteratorBuilder&) = delete;
  auto operator=(MergingIteratorBuilder&) -> MergingIteratorBuilder& = delete;
  MergingIteratorBuilder(MergingIteratorBuilder&&) = default;
  auto operator=(MergingIteratorBuilder&&) -> MergingIteratorBuilder& = default;
  ~MergingIteratorBuilder() = default;

  // Add a point key iterator and the paired tombstone iter. Where the point key iterator
  // is NOT bounded by an upper_bound version/snapshot. But the tombstone_iter is capped
  // by an upper_bound version/snapshot.
  //
  // If there is no tombstone_iter paired with the point key iterator, tombstone_iter can
  // be nullptr.
  void AddIterator(InternalIterator* point_iter, FragmentedRangeTombstoneIterator* tombstone_iter);

  auto Build() -> InternalIterator*;

 private:
  MergingIterator* merge_iter_{nullptr};
  Allocator* alloc_;
};

// A minheap that have minimum element at the top.
//
// NB: It is an explicit min heap implementation, And not like
// std::priory_queue that implemented as a max heap by default.
template <typename T, std::strict_weak_order<T, T> Compare = std::less<T>>
class MinHeap {
 public:
  MinHeap() = default;
  explicit MinHeap(Compare cmp) : cmp_(std::move(cmp)) {}

  void Push(const T& value) {
    data_.push_back(value);
    Upward(data_.size() - 1);
  }

  void Push(const T&& value) {
    data_.push_back(std::move(value));
    Upward(data_.size() - 1);
  }

  auto Top() -> T& {
    assert(!Empty());
    return data_.front();
  }

  // Replace the top in-place and adjust downward to maintain
  // the min heap property. Time complexity wise, it only requires
  // one single adjust instead of 2 round in pop+push approach.
  // Comparison wise, it would require [1, 2logN] comparisons instead
  // of around 2logN comparisons in pop+push approach. And when the
  // replacement element is also the new top, it takes just 1 or 2
  // comparisons.
  void ReplaceTop(T&& value) {
    assert(!Empty());
    data_.front() = std::move(value);
    Downward(0);
  }

  void ReplaceTop(T& value) {
    assert(~Empty());
    data_.front() = value;
    Downward(0);
  }

  void Pop() {
    if (data_.size() > 1) {
      data_.front() = data_.back();
    }
    data_.pop_back();
    if (Empty()) {
      return;
    }
    Downward(0);
  }

  auto Empty() const -> bool { return data_.empty(); }

  void Clear() { data_.clear(); }

 private:
  static auto Left(size_t index) -> size_t { return (2 * index) + 1; }
  static auto Right(size_t index) -> size_t { return (2 * index) + 2; }
  static auto Parent(size_t index) -> size_t { return (index - 1) / 2; }

  void Upward(size_t index) {
    T v = std::move(data_[index]);
    while (index > 0) {
      auto parent = Parent(index);
      if (cmp_(data_[parent], v)) {
        break;  // parent less than v, find the target position.
      }
      data_[index] = std::move(data_[parent]);
      index = parent;
    }
    data_[index] = std::move(v);
  }

  void Downward(size_t index) {
    T v = std::move(data_[index]);
    while (true) {
      auto left = Left(index);
      if (left >= data_.size()) {
        break;
      }
      auto right = left + 1;
      assert(right == Right(index));
      auto child = left;
      if (right < data_.size() && !cmp_(data_[left], data_[right])) {
        child = right;  // pick smaller one
      }
      if (cmp_(v, data_[child])) {
        break;  // target smaller than the child, we find the target index.
      }
      data_[index] = std::move(data_[child]);
      index = child;
    }
    data_[index] = std::move(v);
  }

  Compare cmp_;
  std::vector<T> data_;
};

}  // namespace cckv::internal
