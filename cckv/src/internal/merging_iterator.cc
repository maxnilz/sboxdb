#include "merging_iterator.h"

#include <set>
#include <vector>

#include "internal_iterator.h"
#include "range_tombstone.h"

namespace cckv::internal {

// HeapItem represents either a point iterator or a range tombstone boundary
// (start or end key) in the merging iterator's min heap.
struct HeapItem {
  enum class Type : unsigned char { kIterator, kDeleteRangeStart, kDeleteRangeEnd };

  HeapItem() = default;

  HeapItem(size_t level, InternalIterator* iter) : iter_(iter), level_(level) {}
  explicit HeapItem(InternalKey ikey, Type type) : tombstone_key_(ikey), type_(type) {}

  // point iterator
  InternalIterator* iter_{nullptr};
  size_t level_ = 0;

  // range tombstone start or end key
  // depending on the type.
  InternalKey tombstone_key_;

  Type type_ = Type::kIterator;
};

// Comparator for HeapItem that works with both point iterators and
// range tombstone boundaries.
class MinHeapComparator {
 public:
  explicit MinHeapComparator(InternalKeyComparator cmp) : comparator_(cmp) {}

  auto operator()(const HeapItem* a, const HeapItem* b) const -> bool {
    if (a->type_ == HeapItem::Type::kIterator) {
      if (b->type_ == HeapItem::Type::kIterator) {
        return comparator_(a->iter_->Key(), b->iter_->Key());
      }
      return comparator_(a->iter_->Key(), b->tombstone_key_);
    }
    if (b->type_ == HeapItem::Type::kIterator) {
      return comparator_(a->tombstone_key_, b->iter_->Key());
    }
    return comparator_(a->tombstone_key_, b->tombstone_key_);
  }

 private:
  InternalKeyComparator comparator_;
};

// MergingIterator uses a min heap to combine data from point iterators.
// Range tombstones can be added and keys covered by range tombstones will be
// skipped.
//
// For merging iterator to process range tombstones, it treats the start and end
// keys of a range tombstone as two keys and put them into min_heap_
// together with regular point keys. Each range tombstone is active only within
// its internal key range [start_key, end_key). An `active_` set is used to
// track levels that have an active range tombstone. Take forward scanning
// for example. Level j is in active_ if its current range tombstone has its
// start_key popped from min_heap_ and its end_key in min_heap_. If the top of
// min_heap_ is a point key from level L, we can determine if the point key is
// covered by any range tombstone by checking if there is an l <= L in active_.
// The case of l == L also involves checking range tombstone's sequence number.
//
// The following (non-exhaustive) list of invariants are maintained by
// MergingIterator during forward scanning. After each InternalIterator API,
// i.e., Seek*() and Next(), and FindNextVisibleKey(), if min_heap_ is not empty:
// (1) min_heap_.top().type == ITERATOR
// (2) min_heap_.top()->key() is not covered by any range tombstone.
//
// After each call to Seek in addition to the functions mentioned above:
// (3) For all level i and j <= i, range_tombstone_iters_[j].prev.end_key() <
// children_[i].iter.key(). That is, range_tombstone_iters_[j] is at or before
// the first range tombstone from level j with end_key() >
// children_[i].iter.key().
// (4) For all level i and j <= i, if j in active_, then
// range_tombstone_iters_[j]->start_key() < children_[i].iter.key().
// - When range_tombstone_iters_[j] is !Valid(), we consider its `prev` to be
// the last range tombstone from that range tombstone iterator.
// - When referring to range tombstone start/end keys, assume it is the value of
// HeapItem::tombstone_key_. This value has op_type = kMaxValid, which makes
// range tombstone keys have distinct values from point keys.
//
// Applicable class variables have their own (forward scanning) invariants
// listed in the comments above their definition.
//
// **NB**:
// - An overall invariant is: A key in level L has a larger sequence number
//  than all keys in any level greater than L.
// - children_ point iterators are not bound by upper_bound, but
//  range_tombstone_iterators_ are bounded by an upper_bound_.
class MergingIterator : public InternalIterator {
 public:
  explicit MergingIterator(InternalKeyComparator comparator)
      : min_heap_(MinHeapComparator(comparator)) {}
  MergingIterator(MergingIterator&) = delete;
  auto operator=(MergingIterator&) -> MergingIterator& = delete;
  MergingIterator(MergingIterator&&) = default;
  auto operator=(MergingIterator&&) -> MergingIterator& = default;

  void AddIterator(InternalIterator* iter, FragmentedRangeTombstoneIterator* tombstone_iter) {
    assert(children_.size() == range_tombstone_iterators_.size());
    children_.emplace_back(children_.size(), iter);
    // There must be the same number of range tombstone iterators as point
    // iterators and the i-th added range tombstone iterator and the i-th
    // point iterator must point to the same LSM level.
    range_tombstone_iterators_.emplace_back(tombstone_iter);
  }

  // Called by MergingIteratorBuilder when all point iterators and range
  // tombstone iterators are added. Initializes HeapItems for range tombstone
  // iterators.
  void Finish();

  ~MergingIterator() override = default;

  auto Valid() const -> bool override {
    if (current_ == nullptr) {
      return false;
    }
    return current_->Valid();
  }
  void SeekToFirst() override;
  void Seek(const Slice& user_key) override;
  auto Next() -> void override;
  auto Key() const -> InternalKey override;
  auto UserKey() const -> Slice override;
  auto Value() const -> Slice override;
  auto Status() const -> cckv::Status override;

 private:
  // Advance the overall iterator to a position the is visible to the caller.
  void FindNextVisibleKey();

  // Pop the top until it is not a tombstone start key. In case of a tombstone start
  // at level i is popped out, it means the tombstone is activated for levels equal to
  // or greater than level i.
  void PopTombstoneStart();

  // Advance the overall iterator position by one, if the
  // current key(represented by the min heap top) is covered
  // by a tombstone, otherwise, the overall iterator position
  // is stay the same, i.e., not being advanced.
  //
  // Return true if the current key is deleted, otherwise
  // false.
  auto SkipNextDeleted() -> bool;

  // Advance this merging iterators to the first key >= `target` for all
  // components from levels >= starting_level. All iterators before
  // starting_level are untouched.
  void Seek(InternalKey target, size_t starting_level);

  void InsertTombstoneStartToHeap(size_t level);

  auto Current() -> InternalIterator* {
    assert(min_heap_.Empty() || min_heap_.Top()->type_ == HeapItem::Type::kIterator);
    return min_heap_.Empty() ? nullptr : min_heap_.Top()->iter_;
  }

  // Wrap the child iterator Valid to record status_ if child iterator is not valid.
  auto IsChildIterValid(const InternalIterator* iter) -> bool {
    if (iter->Valid()) {
      return true;
    }
    if (!iter->Status().ok() && status_.ok()) {
      // record the first child non-ok status as the status of merging iterator.
      status_ = iter->Status();
    }
    return false;
  }

  // HeapItem for all child point iterators
  // Invariant(children_): children_[i] is in min_heap_ iff
  // children_[i].iter.Valid(), and at most one children_[i] is in min_heap_.
  std::vector<HeapItem> children_;
  // HeapItem for range tombstone start and end keys.
  // pinned_heap_item_[i] corresponds to range_tombstone_iters_[i].
  // Invariant(phi): If range_tombstone_iters_[i]->Valid(),
  // pinned_heap_item_[i].tombstone_pik is equal to
  // range_tombstone_iters_[i]->start_key() when
  // pinned_heap_item_[i].type is DELETE_RANGE_START and
  // range_tombstone_iters_[i]->end_key() when
  // pinned_heap_item_[i].type is DELETE_RANGE_END (ignoring op_type which is
  // kMaxValid for all pinned_heap_item_.tombstone_key_).
  // pinned_heap_item_[i].type is either DELETE_RANGE_START or DELETE_RANGE_END.
  std::vector<HeapItem> pinned_heap_item_;
  // range_tombstone_iters_[i] contains range tombstones in the sorted run that
  // corresponds to children_[i]. range_tombstone_iters_.empty() means not
  // handling range tombstones in merging iterator. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones.
  // Invariant(rti): pinned_heap_item_[i] is in min_heap_ iff
  // range_tombstone_iters_[i]->Valid() and at most one pinned_heap_item_[i] is
  // in min_heap_.
  std::vector<FragmentedRangeTombstoneIterator*> range_tombstone_iterators_;
  // Levels (indices into range_tombstone_iters_/children_ ) that currently have
  // "active" range tombstones. Ordered ascendantly.
  // Invariant(active_): i is in active_ iff range_tombstone_iters_[i]->Valid()
  // and pinned_heap_item_[i].type == DELETE_RANGE_END.
  std::set<size_t> active_;

  // Invariant: at the end of each InternalIterator API, current_ point to
  // min_heap_.Top().iter or nullptr if no child iterator is valid.
  InternalIterator* current_{nullptr};

  MinHeap<HeapItem*, MinHeapComparator> min_heap_;

  // If any of the children have non-ok status, this is one of them.
  cckv::Status status_;
};

void MergingIterator::Finish() {
  assert(range_tombstone_iterators_.size() == children_.size());
  pinned_heap_item_.resize(range_tombstone_iterators_.size());
  for (size_t i = 0; i < pinned_heap_item_.size(); ++i) {
    pinned_heap_item_[i].level_ = i;
    // Range tombstone end key is exclusive. If a point internal key has the
    // same user key and sequence number as the start or end key of a range
    // tombstone, i.e., for start, it will be start key < point internal key
    // and for end, it will be end key < point internal key. This is helpful
    // to ensure keys popped from heap are in expected order since range tombstone
    // start/end keys will be distinct from point internal keys.
    pinned_heap_item_[i].tombstone_key_.type_ = kTypeMaxValue;
  }
  current_ = nullptr;
}

void MergingIterator::SeekToFirst() {
  for (auto& child : children_) {
    child.iter_->SeekToFirst();
    if (IsChildIterValid(child.iter_)) {
      min_heap_.Push(&child);
    }
  }
  for (size_t level = 0; level < range_tombstone_iterators_.size(); ++level) {
    if (range_tombstone_iterators_[level]) {
      range_tombstone_iterators_[level]->SeekToFirst();
      if (range_tombstone_iterators_[level]->Valid()) {
        InsertTombstoneStartToHeap(level);
      }
    }
  }
  FindNextVisibleKey();
  current_ = Current();
}

// Position this merging iterator at the first key >= target (internal key).
// If range tombstones are present, keys covered by range tombstones are
// skipped, and this merging iter points to the first non-range-deleted key >=
// target after Seek(). If !Valid() and status().ok() then this iterator
// reaches the end.
void MergingIterator::Seek(const Slice& user_key) {
  auto ikey = InternalKey::LowerBound(user_key);
  Seek(ikey, 0);
  FindNextVisibleKey();
  current_ = Current();
}

auto MergingIterator::Next() -> void {
  assert(Valid());

  // current_ must be the heap top
  assert(!min_heap_.Empty() && current_ == min_heap_.Top()->iter_);

  current_->Next();
  if (current_->Valid()) {
    min_heap_.ReplaceTop(min_heap_.Top());
  } else {
    min_heap_.Pop();
  }

  FindNextVisibleKey();
  current_ = Current();
}

auto MergingIterator::Key() const -> InternalKey {
  assert(Valid());
  return current_->Key();
}

auto MergingIterator::UserKey() const -> Slice {
  assert(Valid());
  return current_->UserKey();
}

auto MergingIterator::Value() const -> Slice {
  assert(Valid());
  return current_->Value();
}

auto MergingIterator::Status() const -> cckv::Status { return status_; }

void MergingIterator::FindNextVisibleKey() {
  PopTombstoneStart();
  // PopTombstoneStart() implies heap top is not kDeleteRangeStart
  // active_ being empty implies no kDeleteRangeEnd in heap.
  // So minHeap_->top() must be of type kIterator.
  while (!min_heap_.Empty() && !active_.empty() && SkipNextDeleted()) {
    PopTombstoneStart();
  }
  assert(min_heap_.Empty() || min_heap_.Top()->type_ == HeapItem::Type::kIterator);
}

void MergingIterator::PopTombstoneStart() {
  while (!min_heap_.Empty() && min_heap_.Top()->type_ == HeapItem::Type::kDeleteRangeStart) {
    auto level = min_heap_.Top()->level_;
    // Activate the tombstone at the top by
    // 1. Update the pined_heap_item_ at level to DeleteRangeEnd
    // 2. Put the tombstone end key into the heap
    // 3. Add the level into the active_

    // Maintains Invariant(active_) and Invariant(phi)
    pinned_heap_item_[level].type_ = HeapItem::Type::kDeleteRangeEnd;
    pinned_heap_item_[level].tombstone_key_ = range_tombstone_iterators_[level]->EndKey();
    // Pop the top and push the end key into heap by replace.
    min_heap_.ReplaceTop(&pinned_heap_item_[level]);
    active_.insert(level);
  }
}

auto MergingIterator::SkipNextDeleted() -> bool {
  // We have 2 types of key:
  //  - point key
  //  - tombstone end key
  auto* current = min_heap_.Top();
  if (current->type_ == HeapItem::Type::kDeleteRangeEnd) {
    auto level = current->level_;
    // We are going to pass the tombstone, try to push the
    // next tombstone if we have more.
    //
    // Pop the top and deactivate the current first.
    min_heap_.Pop();
    active_.erase(level);
    // Move to next tombstone if we have
    assert(range_tombstone_iterators_[level] && range_tombstone_iterators_[level]->Valid());
    range_tombstone_iterators_[level]->Next();
    if (range_tombstone_iterators_[level]->Valid()) {
      InsertTombstoneStartToHeap(level);
    }
    return true;
  }
  // We are point key onwards, check the coverness and do
  // the fast-forward seek optimization.
  assert(current->type_ == HeapItem::Type::kIterator);
  if (active_.empty()) {
    return false;
  }
  auto key = current->iter_->Key();
  // Get the newest level in active_
  auto i = *active_.begin();
  if (i < current->level_) {
    // we have an active range tombstone that comes from a newer level definitely covers.
    // More specifically, since it is guaranteed that A key in level L has a larger
    // sequence number than all keys in any level >L, if we have an active tombstone
    // in a lower level it means the active tombstone must have a higher sequence number
    // than the point key we are currently looking at, so the current point key must be
    // covered by the active tombstone.
    assert(InternalKeyComparator::Compare(range_tombstone_iterators_[i]->StartKey(), key) <= 0);
    assert(InternalKeyComparator::Compare(key, range_tombstone_iterators_[i]->EndKey()) < 0);
    // perform the fast-forward seek optimization to skip all the point keys that
    // covered by the active tombstone.
    Seek(range_tombstone_iterators_[i]->EndKey(), current->level_);
    return true;
  }
  if (i == current->level_) {
    // The active tombstone have the same level as the current point key, we need to
    // check the sequence number to decide the converness.
    //
    // Being active we know we have the current point key must between the start key
    // and end key.
    assert(InternalKeyComparator::Compare(range_tombstone_iterators_[i]->StartKey(), key) <= 0);
    assert(InternalKeyComparator::Compare(key, range_tombstone_iterators_[i]->EndKey()) < 0);
    if (key.seq_ < range_tombstone_iterators_[i]->MaxCoveringSeq(key.user_key_)) {
      // Covered by range tombstone
      //
      // Advance the iter to next.
      current->iter_->Next();
      // Invariant(children_): children_[i] is in min_heap_ iff
      // children_[i].iter.Valid(), and at most one children_[i] is in min_heap_.
      if (IsChildIterValid(current->iter_)) {
        min_heap_.ReplaceTop(current);
      } else {
        min_heap_.Pop();
      }
      return true;
    }
    // Current point key is not covered by the active tombstone.
    return false;
  }
  assert(i > current->level_);
  // Active tombstone from older level cannot cover current point key(newer level point key).
  return false;
}

void MergingIterator::Seek(InternalKey target, size_t starting_level) {
  // Re-populate the heap by clearn fist to simplify the process,
  min_heap_.Clear();
  // Add children for level < starting_level
  for (size_t i = 0; i < starting_level; ++i) {
    if (IsChildIterValid(children_[i].iter_)) {
      min_heap_.Push(&children_[i]);
    }
  }
  // Range tombstone before the starting level.
  if (!range_tombstone_iterators_.empty()) {
    // restores Invariants(rti), (phi) and (active_) for level < starting_level
    // with pinned_heap_item_.
    for (size_t i = 0; i < starting_level; ++i) {
      if (!range_tombstone_iterators_[i] || !range_tombstone_iterators_[i]->Valid()) {
        continue;
      }
      if (active_.contains(i)) {
        assert(pinned_heap_item_[i].type_ == HeapItem::Type::kDeleteRangeEnd);
        min_heap_.Push(&pinned_heap_item_[i]);
        continue;
      }
      assert(pinned_heap_item_[i].type_ == HeapItem::Type::kDeleteRangeStart);
      min_heap_.Push(&pinned_heap_item_[i]);
    }
    // level >= starting_level will be reseeked below, so remove them from
    // the active.
    active_.erase(active_.lower_bound(starting_level), active_.end());
  }
  // Seek and add children for level >= starting_level.
  //
  // the target may change as we go downer to older level in case of range tombstone
  // end key covered the target in newer level.
  auto search_key = target;
  for (size_t i = starting_level; i < children_.size(); ++i) {
    children_[i].iter_->Seek(search_key.user_key_);
    if (!IsChildIterValid(children_[i].iter_)) {
      continue;
    }
    min_heap_.Push(&children_[i]);

    // Invariants (rti) and (phi)
    if (!range_tombstone_iterators_[i] || !range_tombstone_iterators_[i]->Valid()) {
      continue;  // no valid range tombstone at level i.
    }
    range_tombstone_iterators_[i]->Seek(search_key.user_key_);
    if (!range_tombstone_iterators_[i]->Valid()) {
      continue;  // no valid range tombstone at level i after seek.
    }
    // start_key > search_key: means the range tombstone is in the not active yet, add
    // start_key to pinned_heap_item_ with START.
    if (InternalKeyComparator::Compare(range_tombstone_iterators_[i]->StartKey(), search_key) > 0) {
      // Check Invariants(active)
      assert(!active_.contains(i));
      pinned_heap_item_[i].type_ = HeapItem::Type::kDeleteRangeStart;
      pinned_heap_item_[i].tombstone_key_ = range_tombstone_iterators_[i]->StartKey();
      min_heap_.Push(&pinned_heap_item_[i]);
      continue;
    }

    // start_key <= search_key < end_key: means the range tombstone should be considered
    // as active, and end_key to pinned_heap_item with END.
    pinned_heap_item_[i].type_ = HeapItem::Type::kDeleteRangeEnd;
    pinned_heap_item_[i].tombstone_key_ = range_tombstone_iterators_[i]->EndKey();
    min_heap_.Push(&pinned_heap_item_[i]);
    active_.insert(i);

    // extend the search_key to the end_key for the next older level.
    search_key = range_tombstone_iterators_[i]->EndKey();
  }
}

void MergingIterator::InsertTombstoneStartToHeap(size_t level) {
  pinned_heap_item_[level].type_ = HeapItem::Type::kDeleteRangeStart;
  pinned_heap_item_[level].tombstone_key_ = range_tombstone_iterators_[level]->StartKey();
  // Checks Invariant(active_)
  assert(!active_.contains(level));
  // Add the tombstone at level i into the min_heap_
  min_heap_.Push(&pinned_heap_item_[level]);
}

MergingIteratorBuilder::MergingIteratorBuilder(InternalKeyComparator comp, Allocator* alloc)
    : alloc_(alloc) {
  char* mem = alloc->Allocate(sizeof(MergingIterator));
  merge_iter_ = new (mem) MergingIterator(comp);
}

void MergingIteratorBuilder::AddIterator(InternalIterator* point_iter,
                                         FragmentedRangeTombstoneIterator* tombstone_iter) {
  merge_iter_->AddIterator(point_iter, tombstone_iter);
}

auto MergingIteratorBuilder::Build() -> InternalIterator* {
  merge_iter_->Finish();
  InternalIterator* ret = nullptr;
  ret = merge_iter_;
  merge_iter_ = nullptr;
  return ret;
}

}  // namespace cckv::internal