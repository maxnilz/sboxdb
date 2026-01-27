#pragma once

#include <memory>

#include "cckv/cckv.h"
#include "cckv/slice.h"
#include "cckv/status.h"
#include "codec.h"
#include "internal_iterator.h"
#include "range_tombstone.h"

namespace cckv::internal {

class Allocator;

class ReadOnlyMemTable {
 public:
  virtual ~ReadOnlyMemTable() = default;

  // Return latest visible value for user_key with seq <= as_of; NotFound
  // if none.
  virtual auto Get(const Slice& user_key, SeqNum as_of, std::string* value) -> Status = 0;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying memtable remains live
  // while the returned interator is live.
  virtual auto NewIterator() const -> std::unique_ptr<InternalIterator> = 0;

  // Return an iterator that yields non-overlapped range tombstones of
  // the memtable.
  //
  // The caller must ensure that the underlying memtable remains live
  // while the returned interator is live.
  virtual auto NewRangeTombstoneIterator(SeqNum upper_bound)
      -> std::unique_ptr<FragmentedRangeTombstoneIterator> = 0;
};

class MemTable : public ReadOnlyMemTable {
 public:
  ~MemTable() override = default;

  // Insert a value/tombstone; caller guarantees slices live through the call.
  //
  // ikey and value are copied/encoded into the underlying storage.
  virtual auto Add(const InternalKey& ikey, const Slice& value) -> Status = 0;
};

// Factory for memtable.
auto NewMemTable(Allocator* allocator) -> std::unique_ptr<MemTable>;

}  // namespace cckv::internal
