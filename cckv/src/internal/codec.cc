#include "codec.h"

#include <algorithm>
#include <cassert>
#include <cstring>

namespace cckv::internal {

static constexpr int kContinueBit = 0x80U;

// encode with the least significant byte first fashion.
auto EncodeVarint32(char* dst, uint32_t value) -> char* {
  auto* ptr = reinterpret_cast<unsigned char*>(dst);
  while (value >= kContinueBit) {
    *(ptr++) = value | kContinueBit;
    value >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(value);
  return reinterpret_cast<char*>(ptr);
}

auto EncodeVarint64(char* dst, uint64_t value) -> char* {
  auto* ptr = reinterpret_cast<unsigned char*>(dst);
  while (value >= kContinueBit) {
    *(ptr++) = value | kContinueBit;
    value >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(value);
  return reinterpret_cast<char*>(ptr);
}

auto VarintLength(uint64_t v) -> uint16_t {
  uint16_t len = 1;
  while (v >= kContinueBit) {
    v >>= 7;
    len++;
  }
  return len;
}

auto DecodeVarint32(const char* p, const char* limit, uint32_t* value) -> const char* {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if ((byte & kContinueBit) != 0U) {
      // we have more bytes
      result |= (byte & 0x7F) << shift;
      continue;
    }
    // no more bytes continued
    result |= (byte & 0x7F) << shift;
    *value = result;
    return p;
  }
  return nullptr;
}

auto DecodeVarint64(const char* p, const char* limit, uint64_t* value) -> const char* {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if ((byte & kContinueBit) != 0U) {
      // we have more bytes
      result |= (byte & 0x7F) << shift;
      continue;
    }
    // no more bytes continued
    result |= (byte & 0x7F) << shift;
    *value = result;
    return p;
  }
  return nullptr;
}

auto EncodeFixed64(char* dst, uint64_t value) -> void {
  auto* ptr = reinterpret_cast<unsigned char*>(dst);
  for (uint32_t shift = 0; shift <= 56; shift += 8) {
    *(ptr++) = value >> shift;
  }
}

auto DecodeFixed64(const char* p) -> uint64_t {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 56; shift += 8) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    result |= byte << shift;
  }
  return result;
}

auto InternalKey::Encode(char* buf) const -> char* {
  uint32_t key_size = user_key_.Size() + 8;
  buf = EncodeVarint32(buf, key_size);

  memcpy(buf, user_key_.Data(), user_key_.Size());
  buf += user_key_.Size();

  auto packed = PackVersionAndType(version_, type_);
  EncodeFixed64(buf, packed);
  buf += 8;

  return buf;
}

auto InternalKey::Decode(const char* p) -> InternalKey {
  uint32_t key_size = 0;
  const char* user_key = DecodeVarint32(p, p + 5, &key_size);
  assert(user_key != nullptr);
  assert(key_size >= 8);

  const uint32_t user_key_size = key_size - 8;
  const char* packed_ptr = user_key + user_key_size;
  uint64_t packed = DecodeFixed64(packed_ptr);

  Version version;
  ValueType type;
  UnpackVersionAndType(packed, &version, &type);

  return InternalKey{Slice(user_key, user_key_size), version, type};
}

auto InternalKey::LowerBound(const Slice& user_key) -> InternalKey {
  return InternalKey(user_key, kMaxVersion, ValueType::kTypeValue);
}

auto InternalKey::LowerBound(const Slice& user_key, Version as_of) -> InternalKey {
  return InternalKey(user_key, as_of, ValueType::kTypeValue);
}

auto InternalKey::UpperBound(const Slice& user_key) -> InternalKey {
  return InternalKey(user_key, kMinVersion, ValueType::kTypeValue);
}

auto InternalKeyComparator::CompareUserKey(const Slice& a, const Slice& b) -> int {
  const size_t min_len = std::min(a.Size(), b.Size());
  int r = memcmp(a.Data(), b.Data(), min_len);
  if (r != 0) {
    return r;
  }
  return static_cast<int>(a.Size() - b.Size());
}

auto InternalKeyComparator::Compare(const InternalKey& a, const InternalKey& b) -> int {
  // order by user key asc
  const size_t min_len = std::min(a.user_key_.Size(), b.user_key_.Size());
  int r = memcmp(a.user_key_.Data(), b.user_key_.Data(), min_len);
  if (r != 0) {
    return r;
  }
  r = static_cast<int>(a.user_key_.Size() - b.user_key_.Size());
  if (r != 0) {
    return r;
  }
  // order by version desc;
  r = static_cast<int>(b.version_ - a.version_);
  if (r != 0) {
    return r;
  }
  // order by type desc
  return b.type_ - a.type_;
}

auto InternalKeyComparator::Compare(const char* a, const char* b) -> int {
  auto akey = InternalKey::Decode(a);
  auto bkey = InternalKey::Decode(b);

  return Compare(akey, bkey);
}

auto InternalKeyComparator::Compare(const Slice& a, const Slice& b) -> int {
  auto akey = InternalKey::Decode(a.Data());
  auto bkey = InternalKey::Decode(b.Data());

  return Compare(akey, bkey);
}

auto InternalKeyComparator::operator()(const InternalKey& a, const InternalKey& b) const -> bool {
  return Compare(a, b) < 0;
}

auto InternalKeyComparator::operator()(const char* a, const char* b) const -> bool {
  return Compare(a, b) < 0;
}
auto InternalKeyComparator::operator()(const InternalKey& a, const char* b) const -> bool {
  auto bkey = InternalKey::Decode(b);
  return Compare(a, bkey) < 0;
}

auto InternalKeyComparator::operator()(const Slice& a, const Slice& b) const -> bool {
  return Compare(a, b) < 0;
}

}  // namespace cckv::internal
