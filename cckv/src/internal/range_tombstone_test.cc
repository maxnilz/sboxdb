#include "range_tombstone.h"

#include <algorithm>

#include "gtest/gtest.h"
#include "internal_iterator.h"

namespace cckv::internal {

struct Range {
  Range(std::string&& a, std::string&& b, SeqNum seq) {
    user_start_key_ = std::move(a);
    start_ = InternalKey(user_start_key_, seq, kTypeRangeDeletion);

    std::string user_end_key = std::move(b);
    InternalKey end(user_end_key, seq, kTypeRangeDeletion);
    // copy end key into value by encode
    value_.resize(end.EncodedLen());
    end.Encode(value_.data());
  }

  // Ctor required by std::vector(std::initializer_list<T> init)
  Range(const Range& other) : user_start_key_(other.user_start_key_), value_(other.value_) {
    start_ = InternalKey(user_start_key_, other.start_.seq_, other.start_.type_);
  }

  InternalKey start_;
  std::string user_start_key_;  // hold the memory for slice in start_
  std::string value_;           // encoded with end
};

using range_list = std::initializer_list<Range>;

class Ranges : public InternalIterator {
 public:
  Ranges(std::initializer_list<Range> ranges) : ranges_(ranges) /*init with ctor*/ {};
  ~Ranges() override = default;
  auto Valid() const -> bool override { return it_ != ranges_.end(); }
  void SeekToFirst() override { it_ = ranges_.begin(); }
  void Seek(const Slice& user_key) override { /* NOT NEEDED */ }
  auto Next() -> void override { ++it_; }
  auto Key() const -> InternalKey override { return it_->start_; }
  auto UserKey() const -> Slice override { return it_->start_.user_key_; }
  auto Value() const -> Slice override { return it_->value_; }
  auto Status() const -> cckv::Status override { return Status::Ok(); }

 private:
  std::vector<Range> ranges_;
  std::vector<Range>::iterator it_;
};

TEST(RangeTombstone, Fragment) {
  struct Case {
    Case(std::string name, std::initializer_list<Range> ranges, std::string expect)
        : name_(std::move(name)) {
      ranges_ = std::make_unique<Ranges>(ranges);
      expect_ = std::move(expect);
    }
    std::string name_;
    std::unique_ptr<Ranges> ranges_;
    std::string expect_;
  };
  std::vector<Case> cases;
  // Case 1
  //    |---1----|  |---2-----|
  //  --a--------c--e---------g--
  cases.emplace_back("case 1", range_list{Range{"a", "c", 1}, Range{"e", "g", 2}},
                     "[a, c)@1, [e, g)@2");
  // Case 2
  //         |-----2-----|
  //   |----1---|
  // --a-----c--e--------g-------
  cases.emplace_back("case 2", range_list{Range{"a", "e", 1}, Range{"c", "g", 2}},
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@2");
  // Case 3
  //          |---2-----------|
  //    |------1---|    |---3------|
  // ---a-----c----e----g-----i----k---
  cases.emplace_back("case 3",
                     range_list{
                         Range{"a", "e", 1},
                         Range{"c", "i", 2},
                         Range{"g", "k", 3},
                     },
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@2, [g, i)@3, [g, i)@2, [i, k)@3");
  // Case 4
  //          |--------2---------------|
  //    |-----1----|    |---3------|
  // ---a-----c----e----g----------i---k---
  cases.emplace_back("case 4",

                     range_list{
                         Range{"a", "e", 1},
                         Range{"c", "k", 2},
                         Range{"g", "i", 3},
                     },
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@2, [g, i)@3, [g, i)@2, [i, k)@2");
  // Case 5
  //
  //  |----------1-------------|
  //      |------2---|    |-------3--|
  // -a---c----------e----g----i-----k----
  cases.emplace_back("case 5",
                     range_list{
                         Range{"a", "i", 1},
                         Range{"c", "e", 2},
                         Range{"g", "k", 3},
                     },
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@1, [g, i)@3, [g, i)@1, [i, k)@3");
  // Case 6
  //
  //   |----1------------------------------|
  //      |--2-------|    |-----3----|
  // --a--c----------e----g----------i-----k--
  cases.emplace_back("case 6",
                     range_list{
                         Range{"a", "k", 1},
                         Range{"c", "e", 2},
                         Range{"g", "i", 3},
                     },
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@1, [g, i)@3, [g, i)@1, [i, k)@1");
  // Case 7
  //                         |--4-----|
  //                   |--3-----------|
  //         |---2--------------------|
  //   |---1------|
  // --a-----c----e----g-----i--------k--
  cases.emplace_back("case 7",
                     range_list{
                         Range{"a", "e", 1},
                         Range{"c", "k", 2},
                         Range{"g", "k", 3},
                         Range{"i", "k", 4},
                     },
                     "[a, c)@1, [c, e)@2, [c, e)@1, [e, g)@2, [g, i)@3, [g, i)@2, "
                     "[i, k)@4, [i, k)@3, [i, k)@2");

  for (auto& c : cases) {
    std::unique_ptr<InternalIterator> it = std::move(c.ranges_);
    FragmentedRangeTombstones frag(it);
    auto got = frag.ToString();
    EXPECT_EQ(got, c.expect_) << c.name_ << " expected: " << c.expect_ << ", got: " << got;
  }
}

TEST(RangeTombstone, MaxCoveringSeq) {
  //            |--------4---------|
  //            |--------3---------|
  //      |--1---------|     |----5-------|
  // -----b-----d------f-----h----j-------l---
  const std::unique_ptr<InternalIterator> it = std::make_unique<Ranges>(
      range_list{{"b", "f", 1}, {"d", "j", 3}, {"d", "j", 4}, {"h", "l", 5}});
  FragmentedRangeTombstones frag(it);
  const std::string expect =
      "[b, d)@1, [d, f)@4, [d, f)@3, [d, f)@1, [f, h)@4, [f, h)@3, [h, j)@5, [h, j)@4, [h, j)@3, "
      "[j, l)@5";
  const auto got = frag.ToString();
  EXPECT_EQ(expect, got);
  std::vector<std::tuple<std::string, SeqNum, SeqNum>> cases{
      {"a", 0, 0},
      // b
      {"b", 0, 0},
      {"b", 1, 1},
      {"b", 2, 1},
      // c
      {"c", 0, 0},
      {"c", 1, 1},
      {"c", 2, 1},
      // d
      {"d", 0, 0},
      {"d", 1, 1},
      {"d", 2, 1},
      {"d", 3, 3},
      {"d", 4, 4},
      // i
      {"i", 0, 0},
      {"i", 1, 0},
      {"i", 2, 0},
      {"i", 3, 3},
      {"i", 4, 4},
      {"i", 5, 5},
      {"i", 6, 5},
  };
  for (auto& [user_key, as_of, expect_seq] : cases) {
    auto got_seq = frag.MaxCoveringSeq(user_key, as_of);
    EXPECT_EQ(got_seq, expect_seq)
        << "max covering for " << user_key << ", expect: " << expect_seq << ", got: " << got_seq;
  }
}

TEST(RangeTombstone, Iterator) {
  //            |--------4---------|
  //            |--------3---------|
  //      |--1---------|     |----5-------|
  // -----b-----d------f-----h----j-------l---
  const std::unique_ptr<InternalIterator> it = std::make_unique<Ranges>(range_list{
      {"b", "f", 1},
      {"d", "j", 3},
      {"d", "j", 4},
      {"h", "l", 5},
  });
  auto frag = FragmentedRangeTombstones(it);
  std::vector<std::pair<SeqNum, std::vector<std::string>>> cases{
      {0, {}},
      {1,
       {
           "[b, d)@1",
           "[d, f)@1",
       }},
      {2,
       {
           "[b, d)@1",
           "[d, f)@1",
       }},
      {3,
       {
           "[b, d)@1",
           "[d, f)@3",
           "[f, h)@3",
           "[h, j)@3",
       }},
      {4,
       {
           "[b, d)@1",
           "[d, f)@4",
           "[f, h)@4",
           "[h, j)@4",
       }},
      {5,
       {
           "[b, d)@1",
           "[d, f)@4",
           "[f, h)@4",
           "[h, j)@5",
           "[j, l)@5",
       }},
      {6,
       {
           "[b, d)@1",
           "[d, f)@4",
           "[f, h)@4",
           "[h, j)@5",
           "[j, l)@5",
       }},
  };
  for (auto& [upper_bound, expected_res] : cases) {
    auto iter = FragmentedRangeTombstoneIterator(frag, upper_bound);
    iter.SeekToFirst();
    std::vector<std::string> result;
    while (iter.Valid()) {
      auto start = iter.StartKey();
      auto end = iter.EndKey();
      auto seq = iter.Seq();
      auto res =
          std::format("[{}, {})@{}", start.user_key_.ToString(), end.user_key_.ToString(), seq);
      result.push_back(std::move(res));
      iter.Next();
    }
    EXPECT_EQ(expected_res, result);
  }
}

TEST(RangeTombstone, PlayMaxCoveringBoundary) {
  std::vector<int> seqs{7, 5, 3};
  std::vector<std::pair<int, int>> cases{
      {7, 7},
      {6, 5},
      {100, 7},
  };
  for (auto& [seq, expect] : cases) {
    auto pos = std::ranges::lower_bound(seqs.begin(), seqs.end(), seq, std::greater<>{});
    EXPECT_EQ(*pos, expect);
  }
}

TEST(RangeTombstone, PlayArrayBoundary) {
  std::vector<int> data{1, 1, 2, 3, 3, 3, 3, 4, 4, 4, 6, 6, 8};
  {
    auto lower = std::ranges::lower_bound(data.begin(), data.end(), 4);
    std::ranges::copy(lower, data.end(), std::ostream_iterator<int>(std::cout, " "));
    std::cout << '\n';
  }
  {
    auto lower = std::ranges::lower_bound(data.begin(), data.end(), 7);
    std::ranges::copy(lower, data.end(), std::ostream_iterator<int>(std::cout, " "));
    std::cout << '\n';
  }
  {
    auto upper = std::ranges::upper_bound(data.begin(), data.end(), 4);
    std::ranges::copy(upper, data.end(), std::ostream_iterator<int>(std::cout, " "));
    std::cout << '\n';
  }
  {
    auto upper = std::ranges::upper_bound(data.begin(), data.end(), 7);
    std::ranges::copy(upper, data.end(), std::ostream_iterator<int>(std::cout, " "));
    std::cout << '\n';
  }
}
}  // namespace cckv::internal