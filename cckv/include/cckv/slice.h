#pragma once
#include <cstring>
#include <string>

namespace cckv {
class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(nullptr), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char* data, const size_t n) : data_(data), size_(n) {}

  // Create a slice that refer to the content of s implicitly
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // Create a slice that refer to the contents of sv implicitly
  Slice(const std::string_view& sv) : data_(sv.data()), size_(sv.size()) {}

  // Create a slice that refer to s[0, strlen(s)-1] implicitly
  Slice(const char* s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

  // Return a pointer to the beginning of the referenced data
  auto Data() const -> const char* { return data_; }

  // Return the length (in bytes) of the referenced data
  auto Size() const -> size_t { return size_; }

  // Return true iff the length of the referenced data is zero
  auto Empty() const -> bool { return size_ == 0; }

  auto operator<=>(const Slice& other) const {
    const size_t min_len = (size_ < other.size_) ? size_ : other.size_;
    int cmp = memcmp(data_, other.data_, min_len);

    if (cmp < 0) {
      return std::strong_ordering::less;
    }
    if (cmp > 0) {
      return std::strong_ordering::greater;
    }
    // Common prefix is equal, compare by length
    if (size_ < other.size_) {
      return std::strong_ordering::less;
    }
    if (size_ > other.size_) {
      return std::strong_ordering::greater;
    }
    return std::strong_ordering::equal;
  }

  // Define the equality explicitly, In C++20, the rules are:
  // 1. Defaulted `<=>`: If declare auto operator<=>(...) = default;, the compiler
  //    will automatically generate a matching operator==.
  // 2. Custom `<=>`: If operator<=> is customized, the compiler does not generate operator==.
  auto operator==(const Slice& other) const -> bool {
    return ((size_ == other.size_) && (memcmp(data_, other.data_, size_) == 0));
  }

 private:
  const char* data_;
  size_t size_;
};
}  // namespace cckv