#include "memtable.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>

#include "internal_iterator.h"
#include "memory.h"
#include "range_tombstone.h"

namespace cckv::internal {
namespace {
using Table = std::map<const char*, const char*, InternalKeyComparator>;
class MapMemTable : public MemTable {
 public:
  explicit MapMemTable(Allocator* allocator) : allocator_(allocator) {}
  ~MapMemTable() override = default;

  auto Add(const InternalKey& ikey, const Slice& value) -> Status override {
    const size_t key_size = ikey.EncodedLen();
    char* key_buf = allocator_->Allocate(key_size);
    ikey.Encode(key_buf);

    // if it is a range deletion, encode the value
    // as end_key with seq info, e.g., the entry
    // would represent [ikey, end_key).
    if (ikey.type_ == kTypeRangeDeletion) {
      InternalKey end_key(value, ikey.seq_, ikey.type_);

      size_t end_key_size = end_key.EncodedLen();
      size_t value_size = VarintLength(end_key_size) + end_key_size;

      char* value_buf = allocator_->Allocate(value_size);
      char* buf = value_buf;
      buf = EncodeVarint32(buf, end_key_size);
      end_key.Encode(buf);

      range_del_table_.insert_or_assign(key_buf, value_buf);

      return Status::Ok();
    }

    const uint32_t value_size = VarintLength(value.Size()) + value.Size();
    char* value_buf = allocator_->Allocate(value_size);
    char* p = value_buf;
    p = EncodeVarint32(p, value.Size());
    memcpy(p, value.Data(), value.Size());

    // Latest entry for the encoded internal key wins.
    table_.insert_or_assign(key_buf, value_buf);

    return Status::Ok();
  }

  auto Get(const Slice& user_key, SeqNum as_of, std::string* value) -> Status override {
    std::string buf = InternalKey::LowerBound(user_key, as_of).ToString();
    auto it = table_.lower_bound(buf.data());

    if (it == table_.end()) {
      return Status::NotFound();
    }

    InternalKey ikey = InternalKey::Decode(it->first);
    if (ikey.user_key_ != user_key) {
      return Status::NotFound();
    }

    if (ikey.type_ == kTypeDeletion) {
      // deleted explicitly
      return Status::NotFound();
    }

    assert(ikey.type_ == kTypeValue);

    // check range deletion
    std::unique_ptr<InternalIterator> range_del_iter =
        std::make_unique<Iterator>(&range_del_table_);
    auto tombstones = FragmentedRangeTombstones(range_del_iter);
    auto max_covering_seq = tombstones.MaxCoveringSeq(user_key, as_of);
    if (ikey.seq_ <= max_covering_seq) {
      // covered by the range tombstone
      return Status::NotFound();
    }

    // decode the value
    uint32_t value_size;
    const char* p = it->second;
    p = DecodeVarint32(p, p + 5, &value_size);
    value->assign(p, value_size);
    return Status::Ok();
  }

  auto NewIterator() const -> std::unique_ptr<InternalIterator> override {
    auto iter = std::make_unique<Iterator>(&table_);
    return iter;
  }

  auto NewRangeTombstoneIterator(SeqNum upper_bound)
      -> std::unique_ptr<FragmentedRangeTombstoneIterator> override {
    if (range_del_table_.empty()) {
      return nullptr;
    }
    std::unique_ptr<InternalIterator> range_del_iter =
        std::make_unique<Iterator>(&range_del_table_);
    auto tombstones = FragmentedRangeTombstones(range_del_iter);
    return std::make_unique<FragmentedRangeTombstoneIterator>(std::move(tombstones), upper_bound);
  }

  class Iterator : public InternalIterator {
   public:
    explicit Iterator(const Table* table)
        : table_(table), first_(table->begin()), end_(table->end()) {}

    ~Iterator() override = default;

    auto Valid() const -> bool override { return it_ != end_; }

    void SeekToFirst() override { it_ = first_; }

    // Advance to the first entry with a key >= user_key
    void Seek(const Slice& user_key) override {
      const auto ikey = InternalKey::LowerBound(user_key);
      it_ = std::ranges::lower_bound(
          *table_, ikey, InternalKeyComparator(),
          [](auto& kv) -> InternalKey { return InternalKey::Decode(kv.first); });
    }

    void Next() override {
      if (it_ != end_) {
        ++it_;
      }
    }

    auto Key() const -> InternalKey override { return InternalKey::Decode(it_->first); }

    auto UserKey() const -> Slice override { return Key().user_key_; }

    auto Value() const -> Slice override {
      const auto* p = it_->second;
      uint32_t value_size;
      p = DecodeVarint32(p, p + 5, &value_size);
      return Slice{p, value_size};
    }

    auto Status() const -> cckv::Status override { return cckv::Status::Ok(); }

    const Table* table_;
    Table::const_iterator first_;
    Table::const_iterator it_;
    Table::const_iterator end_;
  };

 private:
  Allocator* allocator_;
  Table table_;
  Table range_del_table_;
};
}  // namespace

auto NewMemTable(Allocator* allocator) -> std::unique_ptr<MemTable> {
  return std::make_unique<MapMemTable>(allocator);
}

}  // namespace cckv::internal
