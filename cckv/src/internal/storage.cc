#include "storage.h"

namespace cckv::internal {
auto BatchRep::Iterator::Next() -> void {
  const char* start = buffer_ + offset_;
  const char* ptr = start;
  type_ = static_cast<ValueType>(*ptr++);
  uint32_t len;
  ptr = DecodeVarint32(ptr, ptr + 5, &len);
  key_ = Slice(ptr, len);
  switch (type_) {
    case kTypeValue:
      DecodeVarint32(ptr, ptr + 5, &len);
      value_ = Slice();
      break;
    case kTypeDeletion:
      break;
    case kTypeRangeDeletion:
      DecodeVarint32(ptr, ptr + 5, &len);
      value_ = Slice();
      break;
  }
  offset_ += (ptr - start);
}

auto Storage::HandlerImpl::Commit(const BatchRep& rep, Version version) -> Status {
  auto iter = rep.NewIterator();
  while (iter.Valid()) {
    iter.Next();
    InternalKey ikey(iter.Key(), version, iter.Type());
    auto s = storage_->table_->Add(ikey, iter.Value());
    if (!s.ok()) {
      return s;
    }
  }
  return Status::Ok();
}

}  // namespace cckv::internal
