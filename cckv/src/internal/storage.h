#pragma once

#include <list>
#include <memory>
#include <string>

#include "cckv/cckv.h"
#include "memtable.h"

namespace cckv::internal {

class Batch;

class Storage : public cckv::Storage {
 public:
  Storage(Storage&) = delete;
  auto operator=(Storage&) -> Storage& = delete;

  ~Storage() override = default;

  auto Snapshot() -> StatusOr<std::unique_ptr<cckv::Snapshot>> override;

  auto WriteBatch(size_t reserved_bytes) -> StatusOr<std::unique_ptr<cckv::WriteBatch>> override;

  // Apply a batch of operations to the storage
  auto ApplyBatch(const Batch* batch) -> Status;

 private:
  std::unique_ptr<MemTable> table_;
  std::list<std::unique_ptr<ReadOnlyMemTable>> imm_list_;

  SeqNum seq_num_;
};

class Batch : public WriteBatch {
 public:
  explicit Batch(Storage* storage, size_t reserved_bytes) : storage_(storage) {
    buffer_.reserve(reserved_bytes);
  }

  ~Batch() override = default;

  auto Put(const Slice& key, const Slice& value) -> Status override;
  auto Delete(const Slice& key) -> Status override;
  auto DeleteRange(const Slice& begin_key, const Slice& end_key) -> Status override;

  auto Apply() -> Status override {
    storage_->ApplyBatch(this);
    return Status::Ok();
  }

  class Iterator {
   public:
    explicit Iterator(const std::string& buffer) : buffer_(buffer), it_(buffer.begin()) {}

    auto Next() -> void;

    auto Type() const -> ValueType { return type_; }
    auto Key() const -> Slice { return key_; }
    auto Value() const -> Slice { return value_; }

    auto Valid() const -> bool { return it_ != buffer_.end(); }

   private:
    const std::string& buffer_;

    std::string::const_iterator it_;

    ValueType type_{};
    Slice key_;
    Slice value_;
  };

  auto Iter() const -> Iterator { return Iterator(buffer_); }

 private:
  auto AppendLengthPrefixedSlice(const Slice& s) -> void;

  Storage* storage_;  // Non-owning pointer to storage

  // buffer_ is a contiguous memory in the format of an array of
  // records to represents the ops of the batch. For each
  // record :=
  //   kTypeValue varstring varstring
  //   kTypeDeletion varstring
  //   kTypeRangeDeletion varstring varstring
  // varstring :=
  //  len: varint32
  //  data: uint8[len]
  std::string buffer_;
};

}  // namespace cckv::internal