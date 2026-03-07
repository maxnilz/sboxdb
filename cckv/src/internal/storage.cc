#include "storage.h"

namespace cckv::internal {
auto Storage::Snapshot() -> StatusOr<std::unique_ptr<cckv::Snapshot>> {
  return StatusOr<std::unique_ptr<cckv::Snapshot>>(Status::Ok());
}

auto Storage::WriteBatch(size_t reserved_bytes) -> StatusOr<std::unique_ptr<cckv::WriteBatch>> {
  std::unique_ptr<cckv::WriteBatch> a = std::make_unique<Batch>(Batch(this, reserved_bytes));
  return StatusOr(std::move(a));
}

auto Storage::ApplyBatch(const Batch* batch) -> Status {
  auto iter = batch->Iter();
  while (iter.Valid()) {
    auto typ = iter.Type();
    auto ikey = InternalKey(iter.Key(), seq_num_, typ);
    table_->Add(ikey, iter.Value());
    seq_num_++;
    iter.Next();
  }
  return Status::Ok();
}

auto Batch::Put(const Slice& key, const Slice& value) -> Status {
  buffer_.push_back(kTypeValue);
  AppendLengthPrefixedSlice(key);
  AppendLengthPrefixedSlice(value);
  return Status::Ok();
}

auto Batch::Delete(const Slice& key) -> Status {
  buffer_.push_back(kTypeDeletion);
  AppendLengthPrefixedSlice(key);
  return Status::Ok();
}

auto Batch::DeleteRange(const Slice& begin_key, const Slice& end_key) -> Status {
  if (end_key <= begin_key) {
    return Status::InvalidArgument("end_key is required to be greater than begin_key");
  }
  buffer_.push_back(kTypeRangeDeletion);
  AppendLengthPrefixedSlice(begin_key);
  AppendLengthPrefixedSlice(end_key);
  return Status::Ok();
}

auto Batch::Iterator::Next() -> void {
  if (!Valid()) {
    return;
  }

  const char* buf_start = buffer_.data();
  const char* buf_end = buf_start + buffer_.size();
  const char* p = &*it_;

  if (p >= buf_end) {
    it_ = buffer_.end();
    return;
  }

  // Read type
  type_ = static_cast<ValueType>(*p++);

  // Read key
  uint32_t key_len;
  p = DecodeVarint32(p, buf_end, &key_len);
  if (p == nullptr || p + key_len > buf_end) {
    it_ = buffer_.end();  // Invalid format - set to end
    return;
  }
  key_ = Slice(p, key_len);
  p += key_len;

  // Read value based on type
  switch (type_) {
    case ValueType::kTypeValue:
    case ValueType::kTypeRangeDeletion: {
      uint32_t value_len;
      p = DecodeVarint32(p, buf_end, &value_len);
      if (p == nullptr || p + value_len > buf_end) {
        it_ = buffer_.end();  // Invalid format - set to end
        return;
      }
      value_ = Slice(p, value_len);
      p += value_len;
      break;
    }
    case ValueType::kTypeDeletion:
      value_ = Slice();  // Empty slice for deletion
      break;
    default:
      // Invalid type - set to end
      it_ = buffer_.end();
      return;
  }

  // Advance iterator to next record
  it_ = buffer_.begin() + (p - buf_start);
}

auto Batch::AppendLengthPrefixedSlice(const Slice& s) -> void {
  char buf[5];
  const char* ptr = EncodeVarint32(buf, s.Size());
  buffer_.append(buf, ptr - buf);
  buffer_.append(s.Data(), s.Size());
}

}  // namespace cckv::internal
