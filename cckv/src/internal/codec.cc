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

auto InternalKey::LowerBound(const Slice& user_key) -> std::string {
  InternalKey ikey(user_key, kMaxVersion, ValueType::kTypeValue);
  std::string out;
  out.resize(ikey.EncodedLen());
  ikey.Encode(out.data());
  return out;
}

auto InternalKey::LowerBound(const Slice& user_key, Version as_of) -> std::string {
  InternalKey ikey(user_key, as_of, ValueType::kTypeValue);
  std::string out;
  out.resize(ikey.EncodedLen());
  ikey.Encode(out.data());
  return out;
}

auto InternalKey::UpperBound(const Slice& user_key) -> std::string {
  InternalKey ikey(user_key, 0, ValueType::kTypeValue);
  std::string out;
  out.resize(ikey.EncodedLen());
  ikey.Encode(out.data());
  return out;
}

auto InternalKeyComparator::operator()(const char* a, const char* b) const -> bool {
  auto akey = InternalKey::Decode(a);
  auto bkey = InternalKey::Decode(b);

  const size_t min_len = std::min(akey.user_key_.Size(), bkey.user_key_.Size());
  int cmp = memcmp(akey.user_key_.Data(), bkey.user_key_.Data(), min_len);
  if (cmp != 0) {
    return cmp < 0;
  }
  if (akey.user_key_.Size() != bkey.user_key_.Size()) {
    return akey.user_key_.Size() < bkey.user_key_.Size();
  }

  return akey.version_ > bkey.version_;
}

}  // namespace cckv::internal
