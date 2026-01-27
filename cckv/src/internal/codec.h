#pragma once

#include <format>

#include "cckv/cckv.h"
#include "cckv/slice.h"

namespace cckv::internal {

using SeqNum = uint64_t;

enum ValueType : unsigned char {
  kTypeValue = 0x00,
  kTypeDeletion = 0x01,
  kTypeRangeDeletion = 0x02,
  kTypeMaxValue = 0x7F,
};

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static constexpr SeqNum kMaxSeqNum = ((1ULL << 56) - 1);
static constexpr SeqNum kMinSeqNum = 0;

// Pack a sequence number and a ValueType into an uint64_t
inline auto PackSequenceAndType(SeqNum seq, ValueType typ) -> uint64_t {
  assert(seq <= kMaxSeqNum);
  return seq << 8 | typ;
}

// Unpack the packed sequence number and value type
inline auto UnpackSequenceAndType(uint64_t packed, SeqNum* seq, ValueType* typ) -> void {
  *typ = static_cast<ValueType>(packed & 0xFF);
  *seq = static_cast<SeqNum>(packed >> 8);
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

// Packed ordering key: user key + sequence number + value type.
struct InternalKey {
  InternalKey() : seq_(0), type_(kTypeValue) {};

  InternalKey(const Slice& user_key, SeqNum seq, ValueType type)
      : user_key_(user_key), seq_(seq), type_(type) {}

  auto Valid() const -> bool { return !user_key_.Empty() && seq_ > 0; }

  auto EncodedLen() const -> size_t {
    uint32_t key_size = user_key_.Size() + 8;
    return VarintLength(key_size) + key_size;
  }

  // Encode the internal key into the give buf, and return a pointer
  // just past the last byte written.
  // REQUIRES: buf has enough space for the value being written
  //
  // Format of an encoded key entry is concatenation of:
  //  user_key           : varint32 of user_key.size()
  //  user_key bytes     : char[user_key.size()]
  //  packed seq and type: 8
  auto Encode(char* buf) const -> char*;

  auto ToString() const -> std::string {
    std::string out;
    out.resize(EncodedLen());
    Encode(out.data());
    return out;
  }

  auto DebugString() const -> std::string {
    auto seq_str = (seq_ == kMaxSeqNum) ? "@max" : std::format("@{}", seq_);
    std::string type_str;
    switch (type_) {
      case kTypeValue:
        type_str = "";
        break;
      case kTypeDeletion:
        type_str = "_del";
        break;
      case kTypeMaxValue:
        type_str = "_max";
        break;
      default:
        type_str = std::format("{}", static_cast<char>(type_ + 48));
    }
    return std::format("{}{}{}", user_key_.ToString(), seq_str, type_str);
  }

  // Decode an encoded internal key into a lightweight view that points into
  // the provided buffer; caller must ensure the lifetime of `p` exceeds the
  // returned object.
  static auto Decode(const char* p) -> InternalKey;

  // Returns an encoded InternalKey that is guaranteed to be < any InternalKey
  // for this user_key and any sequence number.
  static auto LowerBound(const Slice& user_key) -> InternalKey;
  static auto LowerBound(const Slice& user_key, SeqNum seq) -> InternalKey;

  static auto UpperBound(const Slice& user_key) -> InternalKey;

  Slice user_key_;
  SeqNum seq_;
  ValueType type_;
};

// Orders by user key ascending, then by seq number descending so that
// newer entries come before older ones for the same user key.
//
// NB: Two key are considered as equal if the user key and seq number are
// equal respectively, the value type is ignored.
// The returning result following the less_than semantic.
//
// Example semantic of sorting (UserKey ASC, sequence DESC):
// smaller
//    |   key@3
//    |   key@2
//    |   key@1
//    ↓   key@0
// greater
struct InternalKeyComparator {
  static auto CompareUserKey(const Slice& a, const Slice& b) -> int;
  static auto Compare(const InternalKey& a, const InternalKey& b) -> int;
  static auto Compare(const char* a, const char* b) -> int;
  static auto Compare(const Slice& a, const Slice& b) -> int;

  auto operator()(const InternalKey& a, const InternalKey& b) const -> bool;
  auto operator()(const char* a, const char* b) const -> bool;
  auto operator()(const InternalKey& a, const char* b) const -> bool;
  auto operator()(const Slice& a, const Slice& b) const -> bool;
};

}  // namespace cckv::internal
