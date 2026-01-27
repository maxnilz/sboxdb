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
    default:
      status_ = Status::Corruption("unknown BatchRep value type", std::to_string(type_));
  }
  offset_ += (ptr - start);
}
}  // namespace cckv::internal
