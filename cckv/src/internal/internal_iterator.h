#pragma once
#include "cckv/slice.h"
#include "codec.h"

namespace cckv::internal {
class InternalIterator {
 public:
  InternalIterator() = default;

  InternalIterator(InternalIterator&) = delete;
  auto operator=(InternalIterator&) -> InternalIterator& = delete;

  InternalIterator(InternalIterator&&) = default;
  auto operator=(InternalIterator&&) -> InternalIterator& = default;

  virtual ~InternalIterator() = default;

  // An iterator is either positioned at a key/value pair, or not valid.
  virtual auto Valid() const -> bool = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that at or past user_key.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& user_key) = 0;

  // Advance to next entry
  virtual auto Next() -> void = 0;

  virtual auto Key() const -> InternalKey = 0;
  virtual auto UserKey() const -> Slice = 0;
  virtual auto Value() const -> Slice = 0;

  virtual auto Status() const -> Status = 0;
};

class IteratorAdapter : public cckv::Iterator {
 public:
  explicit IteratorAdapter(std::unique_ptr<InternalIterator> internal_iterator)
      : internal_iterator_(std::move(internal_iterator)) {}

  ~IteratorAdapter() override = default;

  auto Valid() const -> bool override { return internal_iterator_->Valid(); }
  void Seek(const Slice& user_key) override { internal_iterator_->Seek(user_key); }
  auto Next() -> void override { internal_iterator_->Next(); }
  auto Key() const -> Slice override { return internal_iterator_->Key().user_key_; }
  auto Value() const -> Slice override { return internal_iterator_->Value(); }
  auto Status() const -> cckv::Status override { return internal_iterator_->Status(); }

 private:
  std::unique_ptr<InternalIterator> internal_iterator_;
};

inline auto MakeIterator(std::unique_ptr<InternalIterator> it) -> std::unique_ptr<cckv::Iterator> {
  return std::make_unique<IteratorAdapter>(std::move(it));
}

}  // namespace cckv::internal
