#include "memtable.h"

#include <cstring>
#include <map>
#include <memory>
#include <string>

#include "memory.h"

namespace cckv::internal {
namespace {
class MapMemTable : public MemTable {
 public:
  using Table = std::map<const char*, const char*, InternalKeyComparator>;

  explicit MapMemTable(Allocator* allocator) : allocator_(allocator) {}
  ~MapMemTable() override = default;

  auto Add(const InternalKey& ikey, const Slice& value) -> Status override {
    const size_t key_size = ikey.EncodedLen();
    char* key_buf = allocator_->Allocate(key_size);
    ikey.Encode(key_buf);

    const uint32_t value_size = VarintLength(value.Size()) + value.Size();
    char* value_buf = allocator_->Allocate(value_size);
    char* p = value_buf;
    p = EncodeVarint32(p, value.Size());
    memcpy(p, value.Data(), value.Size());

    // Latest entry for the encoded internal key wins.
    auto& table = ikey.type_ == kTypeRangeDeletion ? range_del_table_ : table_;
    table.insert_or_assign(key_buf, value_buf);

    return Status::Ok();
  }

  auto Get(const Slice& user_key, Version as_of, std::string* value) -> Status override {
    // The InternalKeyComparator sorts (UserKey ASC, Version DESC).
    // We want the first entry where Version <= as_of.
    // Since "Greater Version" < "Lower Version" in the comparator logic:
    // - Entries with Version > as_of are considered "smaller" (come before).
    // - lower_bound skips those and lands on the first entry where Version <= as_of.
    std::string buf = InternalKey::LowerBound(user_key, as_of);
    auto it = table_.lower_bound(buf.data());

    if (it == table_.end()) {
      return Status::NotFound();
    }

    InternalKey ikey = InternalKey::Decode(it->first);
    if (ikey.user_key_ != user_key) {
      return Status::NotFound();
    }

    if (ikey.type_ == ValueType::kTypeDeletion) {
      // deleted explicitly
      return Status::NotFound();
    }

    assert(ikey.type_ == ValueType::kTypeValue);

    // TODO: check range deletion tombstones

    // decode the value
    uint32_t value_size;
    const char* p = it->second;
    p = DecodeVarint32(p, p + 5, &value_size);
    value->assign(p, value_size);
    return Status::Ok();
  }

  auto NewIterator(const LookupOptions& opts) const -> std::unique_ptr<cckv::Iterator> override {
    auto iter = std::make_unique<Iterator>(&table_, opts);
    return iter;
  }

  class Iterator : public cckv::Iterator {
   public:
    Iterator(const Table* table, const LookupOptions& opts) : table_(table), opts_(opts) {
      it_ = opts.lower_.has_value()
                ? table->lower_bound(InternalKey::LowerBound(opts.lower_.value()).data())
                : table->begin();
      end_ = opts.upper_.has_value()
                 ? table->lower_bound(InternalKey::LowerBound(opts.upper_.value()).data())
                 : table->end();
      FindNextVisible();
    }

    ~Iterator() override = default;

    auto Valid() const -> bool override { return it_ != end_; }

    void Seek(const Slice& user_key) override {
      Slice key = user_key;
      if (opts_.lower_.has_value() && CompareUserKey(user_key, *opts_.lower_) < 0) {
        // seek should honor the lower bound if the given
        // user_key is less than lower bound.
        key = *opts_.lower_;
      }
      while (it_ != end_) {
        InternalKey ikey = InternalKey::Decode(it_->first);
        if (CompareUserKey(ikey.user_key_, key) < 0) {
          SkipUserKey(ikey.user_key_);
          continue;
        }
        break;
      }
      FindNextVisible();
    }

    void Next() override {
      if (it_ == end_) {
        return;
      }
      ++it_;
      FindNextVisible();
    }

    auto Key() const -> Slice override {
      InternalKey ikey = InternalKey::Decode(it_->first);
      return ikey.user_key_;
    }

    auto Value() const -> Slice override {
      const auto* p = it_->second;
      uint32_t value_size;
      p = DecodeVarint32(p, p + 5, &value_size);
      return Slice{p, value_size};
    }

    auto Status() const -> cckv::Status override { return cckv::Status::Ok(); }

   private:
    void FindNextVisible() {
      while (it_ != end_) {
        InternalKey ikey = InternalKey::Decode(it_->first);
        if (opts_.upper_.has_value() && CompareUserKey(ikey.user_key_, *opts_.upper_) >= 0) {
          it_ = end_;
          return;  // exceed the upper bound
        }
        if (ikey.version_ > opts_.version_) {
          ++it_;
          continue;
        }
        if (ikey.type_ == ValueType::kTypeDeletion) {
          SkipUserKey(ikey.user_key_);
          continue;
        }

        // TODO: check range deletion tombstones

        // Found the visible item
        break;
      }
    }

    void SkipUserKey(const Slice& user_key) {
      while (it_ != end_) {
        InternalKey ikey = InternalKey::Decode(it_->first);
        if (CompareUserKey(ikey.user_key_, user_key) != 0) {
          break;
        }
        ++it_;
      }
    }

    const Table* table_;
    const LookupOptions& opts_;
    Table::const_iterator it_;
    Table::const_iterator end_;
  };

 private:
  static auto CompareUserKey(const Slice& a, const Slice& b) -> int {
    const size_t min_len = std::min(a.Size(), b.Size());
    int cmp = memcmp(a.Data(), b.Data(), min_len);
    if (cmp != 0) {
      return cmp;
    }
    if (a.Size() < b.Size()) {
      return -1;
    }
    if (a.Size() > b.Size()) {
      return 1;
    }
    return 0;
  }
  Allocator* allocator_;
  Table table_;
  Table range_del_table_;
};
}  // namespace

auto NewMemTable(Allocator* allocator) -> std::unique_ptr<MemTable> {
  return std::make_unique<MapMemTable>(allocator);
}

}  // namespace cckv::internal
