#include "memtable.h"

#include <format>
#include <ostream>
#include <string_view>

#include "gtest/gtest.h"
#include "memory.h"

namespace cckv::internal {
TEST(FunctionTest, Memcpy) {
  char buf[4] = {};
  char* src = nullptr;
  memcpy(buf, src, 0);
  printf("hello");
}

class MemTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    wbm_ = NewWriteBufferManager();
    tracker_ = std::make_unique<AllocTracker>(wbm_.get());
    allocator_ = NewAllocator(tracker_.get());
    table_ = NewMemTable(allocator_.get());

    struct Key {
      Key(std::string&& user_key, Version version, ValueType typ)
          : user_key_(std::move(user_key)), version_(version), type_(typ) {};
      std::string user_key_;
      Version version_;
      ValueType type_;
    };

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

TEST_F(MemTableTest, MapMemTableScan) {
  const auto kv = [](std::string_view key, std::string_view value) {
    return std::make_pair(Slice(key), Slice(value));
  };
  const auto opts = [](Version version, std::optional<std::string_view> lower,
                       std::optional<std::string_view> upper) {
    return LookupOptions{
        .version_ = version,
        .lower_ = lower ? std::optional<Slice>(Slice(*lower)) : std::nullopt,
        .upper_ = upper ? std::optional<Slice>(Slice(*upper)) : std::nullopt,
    };
  };
  struct ScanCase {
    std::string name_;
    LookupOptions options_;
    std::vector<std::pair<Slice, Slice>> expected_;
  };
  std::vector<ScanCase> scan_cases = {
      {"v2_from_bar", opts(2, "bar", std::nullopt), {kv("baz", "baz1")}},
      {"v3_from_bar",
       opts(3, "bar", std::nullopt),
       {
           kv("bar", "bar1"),
           kv("baz", "baz1"),
           kv("foo", "foo2"),
       }},
      {"v1_bar_to_foo",
       opts(1, "bar", "foo"),
       {
           kv("baz", "baz1"),
       }},
      {"v1_from_foo",
       opts(1, "foo", std::nullopt),
       {
           kv("foo", "foo1"),
       }},
      {"v2_from_foo", opts(2, "foo", std::nullopt), {}},
  };
  for (const auto& c : scan_cases) {
    auto iter = table_->NewIterator(c.options_);
    std::vector<std::pair<Slice, Slice>> results;
    while (iter->Valid()) {
      results.emplace_back(iter->Key(), iter->Value());
      iter->Next();
    }
    ASSERT_EQ(c.expected_, results)
        << c.name_ << " expected: " << c.expected_ << ", got: " << results;
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
