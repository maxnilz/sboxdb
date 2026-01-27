#include "range_tombstone.h"

#include <algorithm>
#include <set>

namespace cckv::internal {
FragmentedRangeTombstones::FragmentedRangeTombstones(
    const std::unique_ptr<InternalIterator>& unfragmented_tombstones) {
  InternalKey cur_start_key;
  std::set<InternalKey, InternalKeyComparator> cur_end_keys;
  auto flush_current_tombstones = [&](const InternalKey& next_start_key) -> void {
    auto it = cur_end_keys.begin();
    bool reached_next_start_key = false;
    for (; it != cur_end_keys.end() && !reached_next_start_key; ++it) {
      auto cur_end_key = *it;
      if (cur_start_key.user_key_ == cur_end_key.user_key_) {
        // Empty tombstone.
        continue;
      }
      if (InternalKeyComparator::Compare(next_start_key, cur_end_key) <= 0) {
        reached_next_start_key = true;
        // All the end keys in [it, cur_end_keys.end()) are after
        // next_start_key, so the tombstones they represent can be used in
        // fragments that start with keys greater than or equal to
        // next_start_key. However, the end keys we already passed will not be
        // used in any more tombstone fragments.
        //
        // Remove the fully fragmented tombstones and stop iteration after a
        // final round of flushing to preserve the tombstones we can create more
        // fragments from.
        cur_end_keys.erase(cur_end_keys.begin(), it);
        cur_end_key = next_start_key;
      }
      std::vector<SeqNum> seqs_to_flush;
      for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
        seqs_to_flush.push_back(flush_it->seq_);
      }
      std::ranges::sort(seqs_to_flush.begin(), seqs_to_flush.end(), std::greater<>{});
      size_t seq_start_idx = tombstone_seqs_.size();
      size_t seq_end_idx = seq_start_idx + seqs_to_flush.size();

      tombstone_seqs_.insert(tombstone_seqs_.end(), seqs_to_flush.begin(), seqs_to_flush.end());

      tombstones_.emplace_back(cur_start_key, cur_end_key, seq_start_idx, seq_end_idx);
      cur_start_key = cur_end_key;
    }

    if (!reached_next_start_key) {
      // There is a gap between the last flushed tombstone fragment and
      // the next tombstone's start key. Remove all the end keys in
      // the working set, since we have fully fragmented their corresponding
      // tombstones.
      cur_end_keys.clear();
    }
    cur_start_key = next_start_key;
  };
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    auto start_key = unfragmented_tombstones->Key();
    if (!cur_end_keys.empty() && cur_start_key.user_key_ != start_key.user_key_) {
      // The start key has changed. Flush all tombstones that start before
      // this new start key.
      flush_current_tombstones(start_key);
    }
    cur_start_key = start_key;

    auto buf = unfragmented_tombstones->Value();
    auto end_key = InternalKey::Decode(buf.Data());
    cur_end_keys.emplace(end_key);
  }
  // flush remaining end keys
  if (!cur_end_keys.empty()) {
    const auto last_end_key = *std::prev(cur_end_keys.end());
    flush_current_tombstones(last_end_key);
  }
}

auto FragmentedRangeTombstones::MaxCoveringSeq(const Slice& user_key,
                                               SeqNum upper_bound) -> SeqNum {
  auto proj = [](const RangeTombstone& t) -> Slice { return t.end_key_.user_key_; };
  // Since the end_key_ is exclusive, the first tombstone that
  // end_key_.user_key_ > user_key is our target, e.g., Given
  // tombstones like [3, 5), [5, 7), if target user_key is 5,
  // we land on [5, 7), if target user_key is 4, we land on [3, 5).
  const auto pos = std::ranges::upper_bound(tombstones_, user_key, std::ranges::less{}, proj);
  if (pos == tombstones_.end() || pos->start_key_.user_key_ > user_key) {
    // No covering tombstone
    return 0;
  }
  const auto seq_it_begin = seq_iter(pos->seq_start_idx_);
  const auto seq_it_end = seq_iter(pos->seq_end_idx_);
  // seqs is in desc order, find first seq less than or equal to the given seq.
  const auto seq_pos =
      std::ranges::lower_bound(seq_it_begin, seq_it_end, upper_bound, std::greater<>{});
  if (seq_pos == seq_it_end) {
    return 0;
  }
  return *seq_pos;
}

auto FragmentedRangeTombstones::ToString() const -> std::string {
  std::string out;
  out.reserve(tombstones_.size() * 64);  // small heuristic to reduce reallocations
  bool first = true;

  for (const auto& tombstone : tombstones_) {
    for (size_t i = tombstone.seq_start_idx_; i < tombstone.seq_end_idx_; ++i) {
      if (!first) {
        out.append(", ");
      }

      out.push_back('[');
      auto user_key = tombstone.start_key_.user_key_;
      out.append(std::string(user_key.Data(), user_key.Size()));
      out.append(", ");
      user_key = tombstone.end_key_.user_key_;
      out.append(std::string(user_key.Data(), user_key.Size()));
      out.push_back(')');
      out.push_back('@');
      out.append(std::to_string(tombstone_seqs_[i]));

      first = false;
    }
  }
  return out;
}

auto FragmentedRangeTombstoneIterator::Valid() const -> bool {
  if (pos_ == tombstones_.end()) {
    return false;
  }
  if (seq_pos_ == tombstones_.seq_iter(pos_->seq_end_idx_)) {
    return false;
  }
  return true;
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_.begin();
  // update seq_pos_ and pos_(maybe) to honor
  // the given upper bound.
  SetMaximumVisibleSeq();
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekToLast() {
  pos_ = std::prev(tombstones_.end());
  // update seq_pos_ and pos_(maybe) to honor
  // the given upper bound.
  SetMaximumVisibleSeq();
  ScanBackwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::Seek(const Slice& user_key) {
  SeekToCoveringTombstone(user_key);
  ScanForwardToVisibleTombstone();
}

auto FragmentedRangeTombstoneIterator::Next() -> void {
  ++pos_;
  if (pos_ == tombstones_.end()) {
    return;
  }
  SetMaximumVisibleSeq();
  ScanForwardToVisibleTombstone();
}

auto FragmentedRangeTombstoneIterator::StartKey() const -> InternalKey { return pos_->start_key_; }
auto FragmentedRangeTombstoneIterator::EndKey() const -> InternalKey { return pos_->end_key_; }
auto FragmentedRangeTombstoneIterator::Seq() const -> SeqNum { return *seq_pos_; }

auto FragmentedRangeTombstoneIterator::MaxCoveringSeq(const Slice& user_key) -> SeqNum {
  SeekToCoveringTombstone(user_key);
  if (pos_ == tombstones_.end() || pos_->start_key_.user_key_ > user_key) {
    return 0;  // No covering tombstone
  }
  if (seq_pos_ == tombstones_.seq_iter(pos_->seq_end_idx_)) {
    return 0;  // No seq less than the upper_bound_
  }
  return *seq_pos_;
}

void FragmentedRangeTombstoneIterator::SetMaximumVisibleSeq() {
  const auto seq_it_begin = tombstones_.seq_iter(pos_->seq_start_idx_);
  const auto seq_it_end = tombstones_.seq_iter(pos_->seq_end_idx_);
  // seqs is in desc order, find first seq less than or equal to the given upper_bound_.
  // if there is no seq found for the given upper_bound_, it positioned at seq_it_end.
  seq_pos_ = std::ranges::lower_bound(seq_it_begin, seq_it_end, upper_bound_, std::greater<>{});
}

void FragmentedRangeTombstoneIterator::SeekToCoveringTombstone(const Slice& user_key) {
  auto proj = [](const RangeTombstone& t) -> Slice { return t.end_key_.user_key_; };
  // Since the end_key_ is exclusive, the first tombstone that
  // end_key_.user_key_ > user_key is our target, e.g., Given
  // tombstones like [3, 5), [5, 7), if target user_key is 5,
  // we land on [5, 7), if target user_key is 4, we land on [3, 5).
  const auto pos = std::ranges::upper_bound(tombstones_.begin(), tombstones_.end(), user_key,
                                            std::ranges::less{}, proj);
  if (pos == tombstones_.end()) {
    Invalidate();
    return;
  }
  pos_ = pos;
  SetMaximumVisibleSeq();
}

void FragmentedRangeTombstoneIterator::ScanForwardToVisibleTombstone() {
  while (pos_ != tombstones_.end() && seq_pos_ == tombstones_.seq_iter(pos_->seq_end_idx_)) {
    ++pos_;
    if (pos_ == tombstones_.end()) {
      Invalidate();
      return;  // no visible tombstone
    }
    SetMaximumVisibleSeq();
  }
}

void FragmentedRangeTombstoneIterator::ScanBackwardToVisibleTombstone() {
  while (pos_ != tombstones_.end() && seq_pos_ == tombstones_.seq_iter(pos_->seq_end_idx_)) {
    if (pos_ == tombstones_.begin()) {
      Invalidate();
      return;  // no visible tombstone
    }
    --pos_;
    SetMaximumVisibleSeq();
  }
}

}  // namespace cckv::internal
