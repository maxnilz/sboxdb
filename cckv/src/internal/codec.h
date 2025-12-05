#pragma once

#include "cckv/cckv.h"
#include "cckv/slice.h"

namespace cckv::internal {

enum ValueType : unsigned char {
  kTypeValue = 0x00,
  kTypeDeletion = 0x01,
  kTypeRangeDeletion = 0x02,
};

// We leave eight bits empty at the bottom so a type and version#
// can be packed together into 64-bits.
static constexpr Version kMaxVersion = ((1ULL << 56) - 1);

// Pack a sequence number and a ValueType into an uint64_t
inline auto PackVersionAndType(Version version, ValueType typ) -> uint64_t {
  assert(version <= kMaxVersion);
  return version << 8 | typ;
}

// Unpack the packed version and value type
inline auto UnpackVersionAndType(uint64_t packed, Version* version, ValueType* typ) -> void {
  *typ = static_cast<ValueType>(packed & 0xFF);
  *version = static_cast<Version>(packed >> 8);
}

// write varint into a character buffer directly, and return a pointer
// just past the last byte written.
// REQUIRES: dst has enough space for the value being written
auto EncodeVarint32(char* dst, uint32_t value) -> char*;

auto EncodeVarint64(char* dst, uint64_t value) -> char*;

// Returns the length of the varint32 or varint64 encoding of "v"
auto VarintLength(uint64_t v) -> uint16_t;

// decode a varint into *value, return a pointer just past the parsed value,
// or return nullptr on error.  These routines only look at bytes in the range
// [p, limit)
auto DecodeVarint32(const char* p, const char* limit, uint32_t* value) -> const char*;

auto DecodeVarint64(const char* p, const char* limit, uint64_t* value) -> const char*;

// Encode a given 64bit value into the least significant byte first fashion.
auto EncodeFixed64(char* dst, uint64_t value) -> void;
auto DecodeFixed64(const char* p) -> uint64_t;

// Packed ordering key: user key + commit version + value type.
struct InternalKey {
  InternalKey(const Slice& user_key, Version version, ValueType type)
      : user_key_(user_key), version_(version), type_(type) {}

  Slice user_key_;
  Version version_;
  ValueType type_;

  auto EncodedLen() const -> size_t {
    uint32_t key_size = user_key_.Size() + 8;
    return VarintLength(key_size) + key_size;
  }

  // Encode the internal key into the give buf, and return a pointer
  // just past the last byte written.
  // REQUIRES: buf has enough space for the value being written
  //
  // Format of an encoded key entry is concatenation of:
  //  user_key        : varint32 of user_key.size()
  //  user_key bytes  : char[user_key.size()]
  //  packed version and type: 8
  auto Encode(char* buf) const -> char*;

  // Decode an encoded internal key into a lightweight view that points into
  // the provided buffer; caller must ensure the lifetime of `p` exceeds the
  // returned object.
  static auto Decode(const char* p) -> InternalKey;

  // Returns an encoded InternalKey that is guaranteed to be < any InternalKey
  // for this user_key and any version.
  static auto LowerBound(const Slice& user_key) -> std::string;
  // Returns an encoded InternalKey that is suitable for finding the first entry
  // with a version less than or equal to `as_of` for a given user_key.
  static auto LowerBound(const Slice& user_key, Version as_of) -> std::string;
  static auto UpperBound(const Slice& user_key) -> std::string;
};

struct InternalKeyComparator {
  // Orders by user key ascending, then by version descending so that
  // newer entries come before older ones for the same user key.
  //
  // NB: Two key are considered as equal if the user key and version are
  // equal respectively, the value type is ignored.
  // The returning result following the less_than semantic.
  //
  // Example semantic of sorting (UserKey ASC, Version DESC):
  // smaller
  //    |   key@3
  //    |   key@2
  //    |   key@1
  //    ↓   key@0
  // greater
  auto operator()(const char* a, const char* b) const -> bool;
};

}  // namespace cckv::internal
