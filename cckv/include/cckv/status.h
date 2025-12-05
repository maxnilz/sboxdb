#pragma once
#include <cassert>
#include <memory>
#include <optional>

#include "slice.h"

namespace cckv {
class Status {
 public:
  enum Code : unsigned char { kOk = 0, kNotFound, kInvalidArgument, kIOError, kMaxCode };
  enum Subcode : unsigned char { kNone = 0, kPathNotFound, kMaxSubCode };

  Status() : code_(kOk), subcode_(kNone), message_(nullptr) {}
  ~Status() = default;

  // Copy the specified status.
  Status(const Status& s);
  auto operator=(const Status& s) -> Status&;
  auto static Ok() -> Status { return Status(); }

  auto static NotFound(Subcode subcode = kNone) -> Status { return Status(kNotFound, subcode); }
  auto static NotFound(Subcode subcode, const Slice& msg) -> Status {
    return Status(kNotFound, subcode, msg);
  }
  auto static InvalidArgument(const Slice& msg, const Slice& msgv = Slice()) -> Status {
    return Status(kInvalidArgument, kNone, msg, msgv);
  }

  // Since the move ctor is noexcept indeed, mark it so that
  // the standard containers (e.g. std::vector) will prefer moving
  // during reallocation(only if the move is noexcept), e.g., when
  // size exceed the capacity of the container.
  Status(Status&& s) noexcept;

  auto operator=(Status&& s) noexcept -> Status&;

  // NOLINTBEGIN
  auto code() const -> Code { return code_; }
  auto subcode() const -> Subcode { return subcode_; }
  auto ok() -> bool { return code_ == kOk; }
  auto message() const -> const char* { return message_.get(); }
  // NOLINTEND

 protected:
  explicit Status(Code code, Subcode subcode = kNone);
  Status(Code code, const Slice& msg);
  Status(Code code, Subcode subcode, const Slice& msg);
  Status(Code code, Subcode subcode, const Slice& msg, const Slice& msgv);

  template <typename T>
  friend class StatusOr;

  static auto CopyMessage(const char* s) -> std::unique_ptr<const char[]>;

  Code code_;
  Subcode subcode_;
  std::unique_ptr<const char[]> message_{nullptr};
};

template <typename T>
class StatusOr {
 public:
  // NOLINTBEGIN
  // Implicit conversions are intentional to support:
  //  StatusOr<int> result = 42;
  //  StatusOr<int> result = Status::NotFound();
  StatusOr(const T& v) : ok_(true), value_(v) {}
  StatusOr(const Status& s) : value_(std::nullopt), status_(s) {}
  StatusOr(T&& v) : ok_(true), value_(std::move(v)) {}
  StatusOr(Status&& s) : value_(std::nullopt), status_(std::move(s)) {}
  // NOLINTEND

  // NOLINTBEGIN
  auto ok() const -> bool { return ok_; }
  auto status() const -> const Status& { return ok_ ? kOk : status_; }
  // Return a reference to the value, it is an UB if
  // the status is not ok.
  auto value() -> T& {
    assert(ok_);
    return *value_;
  }
  // Return a right reference to the value, it is an UB
  // if the status is not ok.
  auto value() const -> const T& {
    assert(ok_);
    return *value_;
  }
  auto errmsg() const -> const char* {
    assert(!ok_);
    return status_.message_.get();
  }
  // NOLINTEND

 private:
  // inline is required to avoid ODR (One Definition Rule) violations
  // for in-place init when the template is instantiated in multiple
  // translation units.
  inline static const Status kOk = Status::Ok();

  bool ok_{false};
  std::optional<T> value_{std::nullopt};
  Status status_{Status::kOk};
};

#define RETURN_IF_ERROR(expr)          \
  do {                                 \
    auto _status = (expr);             \
    if (!_status.ok()) return _status; \
  } while (0)

#define ASSIGN_OR_RETURN(lhs, rexpr)    \
  do {                                  \
    auto _result = (rexpr);             \
    if (!_result.ok()) {                \
      return _result.status();          \
    }                                   \
    (lhs) = std::move(_result.value()); \
  } while (0)

}  // namespace cckv