#include "codec.h"

#include <array>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace cckv::internal {
TEST(Codec, PackVersionAndTyp) {
  const std::vector<std::pair<Version, ValueType>> cases = {
      {1, ValueType::kTypeDeletion},
      {kMaxVersion, ValueType::kTypeValue},
  };
  for (const auto& [fst, snd] : cases) {
    auto packed = PackVersionAndType(fst, snd);
    Version version;
    ValueType typ;
    UnpackVersionAndType(packed, &version, &typ);
    ASSERT_EQ(fst, version);
    ASSERT_EQ(snd, typ);
  }
}

TEST(Codec, Varint32RoundTrip) {
  const std::vector<uint32_t> cases = {0U,   1U,     127U,    128U,       255U,
                                       300U, 16384U, 999999U, 0xFFFFFFFFU};
  for (const uint32_t v : cases) {
    std::array<char, 10> buf{};
    char* end = EncodeVarint32(buf.data(), v);
    auto len = static_cast<size_t>(end - buf.data());
    uint32_t decoded = 0;
    const char* p = DecodeVarint32(buf.data(), buf.data() + len, &decoded);
    ASSERT_NE(p, nullptr) << "value=" << v;
    EXPECT_EQ(decoded, v);
    EXPECT_EQ(p, buf.data() + len);
  }
}

TEST(Codec, Varint32TruncatedFails) {
  std::array<char, 10> buf{};
  char* end = EncodeVarint32(buf.data(), 300U);  // encodes to two bytes
  auto len = static_cast<size_t>(end - buf.data());
  uint32_t decoded = 0;
  const char* p = DecodeVarint32(buf.data(), buf.data() + (len - 1), &decoded);
  EXPECT_EQ(p, nullptr);
}

TEST(Codec, Varint64RoundTrip) {
  const std::vector<uint64_t> cases = {0ULL,
                                       1ULL,
                                       127ULL,
                                       128ULL,
                                       255ULL,
                                       300ULL,
                                       16384ULL,
                                       999999ULL,
                                       (1ULL << 32) - 1,
                                       static_cast<uint64_t>(1ULL << 63),
                                       0xFFFFFFFFFFFFFFFFULL};

  for (const uint64_t v : cases) {
    std::array<char, 20> buf{};
    char* end = EncodeVarint64(buf.data(), v);
    auto len = static_cast<size_t>(end - buf.data());
    uint64_t decoded = 0;
    const char* p = DecodeVarint64(buf.data(), buf.data() + len, &decoded);
    ASSERT_NE(p, nullptr) << "value=" << v;
    EXPECT_EQ(decoded, v);
    EXPECT_EQ(p, buf.data() + len);
  }
}

TEST(Codec, Varint64TruncatedFails) {
  std::array<char, 20> buf{};
  char* end = EncodeVarint64(buf.data(), (1ULL << 40));  // multi-byte encoding
  auto len = static_cast<size_t>(end - buf.data());
  uint64_t decoded = 0;
  const char* p = DecodeVarint64(buf.data(), buf.data() + (len - 1), &decoded);
  EXPECT_EQ(p, nullptr);
}

TEST(Codec, VarintLengthMatchesEncoding) {
  const std::vector<std::pair<uint64_t, uint16_t>> cases = {
      {0ULL, 1},       {1ULL, 1},       {127ULL, 1},
      {128ULL, 2},     {255ULL, 2},     {300ULL, 2},
      {16383ULL, 2},   {16384ULL, 3},   {1ULL << 21, 4},
      {1ULL << 28, 5}, {1ULL << 35, 6}, {1ULL << 42, 7},
      {1ULL << 49, 8}, {1ULL << 56, 9}, {0xFFFFFFFFFFFFFFFFULL, 10}};

  for (const auto& [v, expected] : cases) {
    EXPECT_EQ(VarintLength(v), expected) << "value=" << v;

    std::array<char, 20> buf{};
    char* end = EncodeVarint64(buf.data(), v);
    auto len = static_cast<size_t>(end - buf.data());
    EXPECT_EQ(len, expected) << "value=" << v;
  }
}

TEST(Codec, Fixed64) {
  const std::vector<uint64_t> cases = {0ULL,
                                       1ULL,
                                       127ULL,
                                       128ULL,
                                       255ULL,
                                       300ULL,
                                       16384ULL,
                                       999999ULL,
                                       (1ULL << 32) - 1,
                                       static_cast<uint64_t>(1ULL << 63),
                                       0xFFFFFFFFFFFFFFFFULL};

  for (auto value : cases) {
    std::array<char, 8> buf{};
    EncodeFixed64(buf.data(), value);
    auto decoded = DecodeFixed64(buf.data());
    EXPECT_EQ(value, decoded);
  }
}

TEST(Codec, InternalKey) {
  struct Case {
    std::string user_key_;
    Version version_;
    ValueType type_;
  };

  const std::vector<Case> cases = {
      {"foo", 1, ValueType::kTypeValue},
      {"foo", 2, ValueType::kTypeDeletion},
      {"bar", 10, ValueType::kTypeValue},
      {"", kMaxVersion, ValueType::kTypeDeletion},
  };

  for (const auto& c : cases) {
    InternalKey ikey{Slice(c.user_key_), c.version_, c.type_};
    std::vector<char> buf(ikey.EncodedLen());
    char* end = ikey.Encode(buf.data());
    EXPECT_EQ(end, buf.data() + buf.size());

    auto decoded = InternalKey::Decode(buf.data());
    EXPECT_EQ(decoded.user_key_.Size(), c.user_key_.size());
    EXPECT_EQ(std::string(decoded.user_key_.Data(), decoded.user_key_.Size()), c.user_key_);
    EXPECT_EQ(decoded.version_, c.version_);
    EXPECT_EQ(decoded.type_, c.type_);
  }

  // Comparator orders by user key asc, then version desc.
  InternalKeyComparator cmp;

  // same key
  InternalKey newer{Slice("same"), 5, ValueType::kTypeValue};
  InternalKey older{Slice("same"), 3, ValueType::kTypeDeletion};

  std::vector<char> newer_buf(newer.EncodedLen());
  std::vector<char> older_buf(older.EncodedLen());

  newer.Encode(newer_buf.data());
  older.Encode(older_buf.data());

  EXPECT_TRUE(cmp(newer_buf.data(), older_buf.data()));
  EXPECT_FALSE(cmp(older_buf.data(), newer_buf.data()));
  EXPECT_FALSE(cmp(newer_buf.data(), newer_buf.data()));

  // different key
  InternalKey akey{Slice("a"), 1, ValueType::kTypeValue};
  InternalKey bkey{Slice("b"), 1, ValueType::kTypeValue};

  std::vector<char> abuf(akey.EncodedLen());
  std::vector<char> bbuf(bkey.EncodedLen());

  akey.Encode(abuf.data());
  bkey.Encode(bbuf.data());

  EXPECT_TRUE(cmp(abuf.data(), bbuf.data()));
  EXPECT_FALSE(cmp(bbuf.data(), abuf.data()));
}
TEST(Codec, InternalKeyBounds) {
  const std::string user_key = "my_key";

  // LowerBound(user_key) -> Version should be kMaxVersion
  std::string lower = InternalKey::LowerBound(user_key);
  ASSERT_FALSE(lower.empty());
  InternalKey decoded_lower = InternalKey::Decode(lower.data());
  EXPECT_EQ(decoded_lower.user_key_, user_key);
  EXPECT_EQ(decoded_lower.version_, kMaxVersion);

  // LowerBound(user_key, version)
  Version v = 100;
  std::string lower_v = InternalKey::LowerBound(user_key, v);
  ASSERT_FALSE(lower_v.empty());
  InternalKey decoded_lower_v = InternalKey::Decode(lower_v.data());
  EXPECT_EQ(decoded_lower_v.user_key_, user_key);
  EXPECT_EQ(decoded_lower_v.version_, v);

  // UpperBound(user_key) -> Version should be 0
  std::string upper = InternalKey::UpperBound(user_key);
  ASSERT_FALSE(upper.empty());
  InternalKey decoded_upper = InternalKey::Decode(upper.data());
  EXPECT_EQ(decoded_upper.user_key_, user_key);
  EXPECT_EQ(decoded_upper.version_, 0);
}

}  // namespace cckv::internal
