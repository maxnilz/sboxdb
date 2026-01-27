#pragma once

#include <list>
#include <memory>
#include <string>

#include "cckv/cckv.h"
#include "memtable.h"

namespace cckv::internal {

class BatchRep {
 public:
  class Iterator {
   public:
    explicit Iterator(const Slice& buf) : buffer_(buf.Data()), size_(buf.Size()) {}

    auto Valid() const -> bool { return offset_ < size_; }
    auto Next() -> void;

    auto Type() const -> ValueType { return type_; }
    auto Key() const -> Slice { return key_; }
    auto Value() const -> Slice { return value_; }

    auto Status() -> Status { return status_; }

   private:
    const char* buffer_;
    size_t size_;
    size_t offset_{0};

    // current entry
    ValueType type_{0};
    Slice key_;
    Slice value_;

    cckv::Status status_;
  };

  explicit BatchRep(size_t reserve_bytes) { buffer_.reserve(reserve_bytes); }

  auto NewIterator() const -> Iterator { return Iterator(buffer_); }

  auto AppendPutEntry(const Slice& key, const Slice& value) -> void {
    buffer_.push_back(kTypeValue);
    AppendLengthPrefixedSlice(key);
    AppendLengthPrefixedSlice(value);
  }

  auto AppendDelEntry(const Slice& key) -> void {
    buffer_.push_back(kTypeDeletion);
    AppendLengthPrefixedSlice(key);
  }

  auto AppendDelRangeEntry(const Slice& begin_key, const Slice& end_key) -> void {
    buffer_.push_back(kTypeRangeDeletion);
    AppendLengthPrefixedSlice(begin_key);
    AppendLengthPrefixedSlice(end_key);
  }

 private:
  auto AppendLengthPrefixedSlice(const Slice& s) -> void {
    char buf[5];
    const char* ptr = EncodeVarint32(buf, s.Size());
    buffer_.append(buf, ptr - buf);
    buffer_.append(s.Data(), s.Size());
  }

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

class Batch : public WriteBatch {
 public:
  class Handler {
   public:
    virtual ~Handler() = default;
    virtual auto Apply(const BatchRep& rep) -> Status = 0;
  };

  explicit Batch(size_t reserved_bytes, Handler* handler)
      : handler_(handler), rep_(reserved_bytes) {}

  ~Batch() override = default;

  auto Put(const Slice& key, const Slice& value) -> Status override {
    rep_.AppendPutEntry(key, value);
    return Status::Ok();
  }

  auto Delete(const Slice& key) -> Status override {
    rep_.AppendDelEntry(key);
    return Status::Ok();
  }

  auto DeleteRange(const Slice& begin_key, const Slice& end_key) -> Status override {
    if (end_key <= begin_key) {
      return Status::InvalidArgument("end_key is required to be greater than begin_key");
    }
    rep_.AppendDelRangeEntry(begin_key, end_key);
    return Status::Ok();
  }

  auto Apply() -> Status override { return handler_->Apply(rep_); }

 private:
  Handler* handler_;
  BatchRep rep_;
};

class Snapshot : public cckv::Snapshot {
 public:
  class Handler {
   public:
    virtual ~Handler() = default;

    virtual auto Get(const Slice& key, const SeqNum& seq, std::string* value) -> Status = 0;
    virtual auto NewIterator(const RangeOptions& options,
                             const SeqNum& seq) -> StatusOr<std::unique_ptr<Iterator>> = 0;
  };

  explicit Snapshot(Handler* handler, SeqNum seq) : handler_(handler), seq_(seq) {}

  ~Snapshot() override = default;

  auto Get(const Slice& key, std::string* value) -> Status override {
    return handler_->Get(key, seq_, value);
  }
  auto NewIterator(const RangeOptions& options) -> StatusOr<std::unique_ptr<Iterator>> override {
    return handler_->NewIterator(options, seq_);
  }

 private:
  Handler* handler_;
  SeqNum seq_;
};

class Metadata {
 public:
  explicit Metadata(SeqNum next_seq) : next_seq_(next_seq) {}

  ~Metadata() = default;

 private:
  SeqNum next_seq_;
};

class Storage : public cckv::Storage {
 public:
  Storage(Storage&) = delete;
  auto operator=(Storage&) -> Storage& = delete;

  ~Storage() override = default;

  auto Snapshot() -> StatusOr<std::unique_ptr<cckv::Snapshot>> override;

  auto WriteBatch() -> StatusOr<std::unique_ptr<cckv::WriteBatch>> override;

 private:
  // Private nested types
  class HandlerImpl : public Batch::Handler, public Snapshot::Handler {
   public:
    explicit HandlerImpl(Storage* storage) : storage_(storage) {};
    ~HandlerImpl() override = default;

    auto Apply(const BatchRep& rep) -> Status override {
      // TODO:
      //  apply to memtable with correct seq
      return Status::Ok();
    }

    auto Get(const Slice& key, const SeqNum& seq, std::string* value) -> Status override {
      // TODO:
      //  check table_, imm_lists_ sequentially
      return Status::Ok();
    }

    auto NewIterator(const RangeOptions& options,
                     const SeqNum& seq) -> StatusOr<std::unique_ptr<Iterator>> override {
      // TODO:
      //  Need to implement rocksdb-like n-way merging iterator
      return Status::InvalidArgument("");
    }

   private:
    Storage* storage_;
  };

  // Private data member
  Metadata metadata_;

  std::unique_ptr<MemTable> table_;
  std::list<ReadOnlyMemTable> imm_list_;

  HandlerImpl* handler_;
};
}  // namespace cckv::internal