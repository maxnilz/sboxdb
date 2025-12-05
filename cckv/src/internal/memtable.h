#pragma once

#include <memory>

#include "cckv/cckv.h"
#include "cckv/slice.h"
#include "cckv/status.h"
#include "codec.h"

namespace cckv::internal {

class Allocator;

struct LookupOptions {
  Version version_{0};
  std::optional<Slice> lower_;  // inclusive user_key
  std::optional<Slice> upper_;  // exclusive user_key
};

class ReadOnlyMemTable {
 public:
  virtual ~ReadOnlyMemTable() = default;

  // Return latest visible value for user_key with version <= as_of; NotFound if
  // none.
  virtual auto Get(const Slice& user_key, Version as_of, std::string* value) -> Status = 0;

  // Iterator yields entries visible at as_of, honoring bounds hints.
  virtual auto NewIterator(const LookupOptions& opts) const -> std::unique_ptr<Iterator> = 0;
};

class MemTable : public ReadOnlyMemTable {
 public:
  ~MemTable() override = default;

  // Insert a value/tombstone; caller guarantees slices live through the call.
  virtual auto Add(const InternalKey& ikey, const Slice& value) -> Status = 0;
};

// Factory for memtable.
auto NewMemTable(Allocator* allocator) -> std::unique_ptr<MemTable>;

}  // namespace cckv::internal
