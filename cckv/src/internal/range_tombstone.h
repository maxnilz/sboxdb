#pragma once
#include <format>
#include <vector>

#include "cckv/cckv.h"
#include "cckv/slice.h"
#include "internal_iterator.h"

namespace cckv::internal {

struct FragmentedRangeTombstones {
  struct RangeTombstone {
    RangeTombstone(const InternalKey& start_key, const InternalKey& end_key, size_t seq_start_idx,
                   size_t seq_end_idx)
        : start_key_(start_key),
          end_key_(end_key),
          seq_start_idx_(seq_start_idx),
          seq_end_idx_(seq_end_idx) {}
    InternalKey start_key_;  // inclusive
    InternalKey end_key_;    // exclusive
    size_t seq_start_idx_;   // inclusive
    size_t seq_end_idx_;     // exclusive
  };

  // Given an ordered range tombstone iterator unfragmented_tombstones,
  // "fragment" the tombstones into non-overlapping pieces. Each
  // "non-overlapping piece" is a RangeTombstone in tombstones_, which
  // contains start_key, end_key, and indices that points to sequence numbers
  // (in tombstone_seqs_).
  explicit FragmentedRangeTombstones(
      const std::unique_ptr<InternalIterator>& unfragmented_tombstones);

  auto MaxCoveringSeq(const Slice& user_key, Version upper_bound) -> Version;

  auto ToString() const -> std::string;

  // NOLINTBEGIN
  auto begin() -> std::vector<RangeTombstone>::const_iterator { return tombstones_.begin(); }
  auto end() -> std::vector<RangeTombstone>::const_iterator { return tombstones_.end(); }
  auto seq_iter(size_t idx) -> std::vector<Version>::const_iterator {
    return std::next(tombstone_seqs_.begin(), idx);
  }
  auto seq_begin() -> std::vector<Version>::const_iterator { return tombstone_seqs_.begin(); }
  auto seq_end() -> std::vector<Version>::const_iterator { return tombstone_seqs_.end(); }
  // NOLINTEND

  std::vector<RangeTombstone> tombstones_;
  // link with each tombstone by seq_start_idx and seq_end_idx,
  // order by seq num desc.
  std::vector<Version> tombstone_seqs_;
};

// The iterator for traverse the fragmented/non-overlapped range tombstone with the
// given inclusive upper_bound, it returns tombstones that closest or equal to the
// given upper_bound version.
class FragmentedRangeTombstoneIterator {
 public:
  // Construct a fragmented range tombstone that positioned at the virtual end. Seek* methods
  // are expected to be called explicitly.
  //
  // NB: the input tombstones is not owned by the iterator.
  FragmentedRangeTombstoneIterator(FragmentedRangeTombstones* tombstones, Version upper_bound)
      : tombstones_(tombstones), upper_bound_(upper_bound) {
    assert(tombstones_ != nullptr);
    Invalidate();  // Position the iterator to the virtual end.
  }
  ~FragmentedRangeTombstoneIterator() = default;

  auto Valid() const -> bool;
  void SeekToFirst();
  void SeekToLast();
  // Seek to covering tombstone with the given user key or to the end
  // if there is no tombstone can cover the given user key under the
  // given upper_bound_.
  void Seek(const Slice& user_key);
  // Advance the cursor to the next visible tombstone at the maximum
  // visible seq under the given upper_bound_, if there is no tombstone
  // found under the given upper_bound_, the cursor is positioned at the
  // end.
  auto Next() -> void;
  auto StartKey() const -> InternalKey;
  auto EndKey() const -> InternalKey;
  auto Seq() const -> Version;

  // Get the max covering seq that honor the given upper_bound_ for the
  // given user_key .
  auto MaxCoveringSeq(const Slice& user_key) -> Version;

 private:
  void SetMaximumVisibleSeq();
  void SeekToCoveringTombstone(const Slice& user_key);
  void ScanForwardToVisibleTombstone();
  void ScanBackwardToVisibleTombstone();
  void Invalidate() {
    pos_ = tombstones_->end();
    seq_pos_ = tombstones_->seq_end();
  }

  using RangeTombstone = FragmentedRangeTombstones::RangeTombstone;

  FragmentedRangeTombstones* tombstones_;
  Version upper_bound_;  // inclusive upper bound

  std::vector<RangeTombstone>::const_iterator pos_;
  std::vector<Version>::const_iterator seq_pos_;
};

}  // namespace cckv::internal
