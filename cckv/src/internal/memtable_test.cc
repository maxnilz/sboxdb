#include "memtable.h"

#include <format>
#include <ostream>
#include <string_view>

#include "gtest/gtest.h"
#include "memory.h"

namespace cckv::internal {
struct Key {
  Key(std::string&& user_key, Version version, ValueType typ)
      : user_key_(std::move(user_key)), version_(version), type_(typ) {};
  std::string user_key_;
  Version version_;
  ValueType type_;
};

class MemTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    wbm_ = NewWriteBufferManager();
    tracker_ = std::make_unique<AllocTracker>(wbm_.get());
    allocator_ = NewAllocator(tracker_.get());
    table_ = NewMemTable(allocator_.get());

    std::vector<std::pair<Key, Slice>> cases = {
        {Key("bar", 3, ValueType::kTypeValue), Slice("bar1")},
        {Key("baz", 1, ValueType::kTypeValue), Slice("baz1")},
        {Key("foo", 1, ValueType::kTypeValue), Slice("foo1")},
        {Key("foo", 2, ValueType::kTypeDeletion), Slice()},
        {Key("foo", 3, ValueType::kTypeValue), Slice("foo2")},
    };

    for (auto& c : cases) {
      auto& key = c.first;
      auto& value = c.second;
      table_->Add(InternalKey(Slice(key.user_key_), key.version_, key.type_), Slice(value));
    }
  }

  std::unique_ptr<WriteBufferManager> wbm_;
  std::unique_ptr<AllocTracker> tracker_;
  std::unique_ptr<Allocator> allocator_;
  std::unique_ptr<MemTable> table_;
};

TEST_F(MemTableTest, MapMemTableGet) {
  std::string value;
  auto status = table_->Get(Slice("foo"), 1, &value);
  ASSERT_EQ(status.code(), Status::kOk);
  ASSERT_EQ(value, "foo1");

  value.clear();
  status = table_->Get(Slice("foo"), 2, &value);
  ASSERT_EQ(status.code(), Status::kNotFound);

  value.clear();
  status = table_->Get(Slice("foo"), 3, &value);
  ASSERT_EQ(status.code(), Status::kOk);
  ASSERT_EQ(value, "foo2");

  value.clear();
  status = table_->Get(Slice("foo"), 4, &value);
  ASSERT_EQ(status.code(), Status::kOk);
  ASSERT_EQ(value, "foo2");
}

TEST_F(MemTableTest, MapMapTableGetWithTombstones) {
  std::vector<std::pair<Key, Slice>> kvs = {
      {Key("a", 1, ValueType::kTypeValue), Slice("a@1")},
      {Key("b", 1, ValueType::kTypeValue), Slice("b@1")},
      {Key("c", 1, ValueType::kTypeValue), Slice("c@1")},
      {Key("d", 1, ValueType::kTypeValue), Slice("d@1")},

      {Key("a", 2, ValueType::kTypeValue), Slice("a@2")},
      {Key("b", 2, ValueType::kTypeValue), Slice("b@2")},
      {Key("c", 2, ValueType::kTypeValue), Slice("c@2")},

      {Key("a", 3, ValueType::kTypeValue), Slice("a@3")},
      {Key("b", 3, ValueType::kTypeValue), Slice("b@3")},
      {Key("c", 3, ValueType::kTypeValue), Slice("c@3")},
      //    |-----4-------------------)
      //              |-3-----)
      //    |--2------)
      // ---a---------b-------c--------d------
      {Key("a", 2, ValueType::kTypeRangeDeletion), Slice("b")},
      {Key("b", 3, ValueType::kTypeRangeDeletion), Slice("c")},
      {Key("a", 4, ValueType::kTypeRangeDeletion), Slice("d")},
  };
  for (auto& kv : kvs) {
    auto& key = kv.first;
    auto& value = kv.second;
    table_->Add(InternalKey(Slice(key.user_key_), key.version_, key.type_), Slice(value));
  }

  // get in case of we have tombstones
  std::vector<std::tuple<std::string, std::string, Version, Status::Code, std::string>> cases = {
      {"Get a@1", "a", 1, Status::kOk, "a@1"},    {"Get a@2", "a", 2, Status::kNotFound, ""},
      {"Get a@3", "a", 3, Status::kOk, "a@3"},    {"Get a@4", "a", 4, Status::kNotFound, ""},
      {"Get a@5", "a", 5, Status::kNotFound, ""},

      {"Get b@1", "b", 1, Status::kOk, "b@1"},    {"Get b@2", "b", 2, Status::kOk, "b@2"},
      {"Get b@3", "b", 3, Status::kNotFound, ""}, {"Get b@4", "b", 4, Status::kNotFound, ""},
      {"Get b@5", "b", 5, Status::kNotFound, ""},

      {"Get c@1", "c", 1, Status::kOk, "c@1"},    {"Get c@2", "c", 2, Status::kOk, "c@2"},
      {"Get c@3", "c", 3, Status::kOk, "c@3"},    {"Get c@4", "c", 4, Status::kNotFound, ""},
      {"Get c@5", "c", 5, Status::kNotFound, ""},
  };
  for (auto& [name, k, v, expect_cd, expect_v] : cases) {
    std::string value;
    auto status = table_->Get(k, v, &value);
    ASSERT_EQ(status.code(), expect_cd)
        << name << ", expect code: " << expect_cd << ", got code: " << status.code();
    if (expect_cd == Status::kOk) {
      ASSERT_EQ(value, expect_v) << name << "expect value: " << expect_v
                                 << ", got value: " << value;
    }
  }
}

auto FormatSlicePairs(const std::vector<std::pair<Slice, Slice>>& a,
                      std::ostream& out) -> std::ostream& {
  bool first = true;
  for (const auto& [key, value] : a) {
    if (!first) {
      out << ", ";
    }
    first = false;
    out << std::format("{}:{}", std::string_view(key.Data(), key.Size()),
                       std::string_view(value.Data(), value.Size()));
  }
  return out;
}
}  // namespace cckv::internal

namespace cckv {
// The stream overload was originally inside cckv::internal, but ADL for
// std::vector<std::pair<cckv::Slice, cckv::Slice>> only looks in std and the associated namespace
// of Slice, which is cckv (not cckv::internal). GTest tried to stream the vector in ASSERT_EQ,
// couldn’t find operator<<, and the build failed. Moving FormatSlicePairs/operator<< into namespace
// cckv made the overload visible to ADL, eliminating the compile error.
//
// For a function call like os << std::pair<A, B>{...} (or a container of those), ADL collects the
// associated namespaces of all template arguments:
//
// - For std::pair<A, B>, both A’s and B’s namespaces are considered, plus std (from the primary
// template).
// - For std::vector<T>, ADL looks at std and T’s associated namespaces.
// - Nested combos (vector of pairs) merge all of those: std, plus the associated namespaces of A
// and B.
//
// So if A lives in ns1 and B lives in ns2, an operator<< defined in either ns1 or ns2 is eligible
// via. ADL when streaming a std::pair<A, B> (or a container of it). If the overload only lives in
// some other namespace, ADL won’t find it unless you qualify the call or bring it into scope.
//
// Argument-dependent lookup (ADL) is C++’s rule that when you call a function without namespace
// qualification, the compiler searches not only the current scope but also the namespaces
// associated with the argument types (e.g., the namespace where a type is declared, and
// namespaces of its template arguments). That’s why putting operator<< in namespace cckv lets the
// compiler find it when streaming a std::vector<std::pair<cckv::Slice, cckv::Slice>>: ADL considers
// cckv (from Slice) and finds the overload.
//
inline auto operator<<(std::ostream& out,
                       const std::vector<std::pair<Slice, Slice>>& a) -> std::ostream& {
  return cckv::internal::FormatSlicePairs(a, out);
}
}  // namespace cckv
