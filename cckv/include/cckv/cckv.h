#pragma once

#include "slice.h"
#include "status.h"

namespace cckv {

using Version = uint64_t;

class Iterator {
 public:
  Iterator() = default;

  // No copying allowed
  Iterator(const Iterator&) = delete;
  auto operator=(const Iterator&) -> Iterator& = delete;

  // Allow move
  Iterator(Iterator&&) = default;
  auto operator=(Iterator&&) -> Iterator& = default;

  virtual ~Iterator() = default;

  // An iterator is either positioned at a key/value pair, or not valid.
  virtual auto Valid() const -> bool = 0;

  // Position at the first key in the source that at or past user_key.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& user_key) = 0;

  // Advance to next entry
  virtual auto Next() -> void = 0;

  virtual auto Key() const -> Slice = 0;
  virtual auto Value() const -> Slice = 0;

  virtual auto Status() const -> Status = 0;
};

struct RangeOptions {
  std::optional<Slice> lower_;  // inclusive user_key
  std::optional<Slice> upper_;  // exclusive user_key
};

class Snapshot {
 public:
  virtual ~Snapshot() = default;

  virtual auto ReadTs() const -> Version = 0;
  virtual auto Get(const Slice& key, std::string* value) -> Status = 0;
  virtual auto NewIterator(const RangeOptions& options) -> StatusOr<std::unique_ptr<Iterator>> = 0;
};

class WriteBatch {
 public:
  virtual ~WriteBatch() = default;

  virtual auto Put(const Slice& key, const Slice& value) -> Status = 0;
  virtual auto Delete(const Slice& key) -> Status = 0;

  // Removes the database entries in the range ["begin_key", "end_key"), i.e.,
  // including "begin_key" and excluding "end_key". Returns OK on success, and
  // a non-OK status on error. It is not an error if the database does not
  // contain any existing data in the range ["begin_key", "end_key").
  virtual auto DeleteRange(const Slice& begin_key, const Slice& end_key) -> Status = 0;

  // Commit atomically with caller-supplied commit version.
  virtual auto Commit(const Version& version) -> Status = 0;
};

class Storage {
 public:
  Storage() = default;

  Storage(Storage&) = delete;
  auto operator=(Storage&) -> Storage& = delete;

  virtual ~Storage() = default;

  virtual auto Snapshot(Version version) -> StatusOr<std::unique_ptr<Snapshot>> = 0;
  virtual auto WriteBatch() -> StatusOr<std::unique_ptr<WriteBatch>> = 0;

  virtual auto NextVersion() -> Version = 0;

  virtual auto LastCommittedVersion() const -> Version = 0;
};

}  // namespace cckv