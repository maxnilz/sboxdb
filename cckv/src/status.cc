#include "cckv/status.h"

#include <cstring>

namespace cckv {
Status::Status(const Status& s) : code_(s.code_), subcode_(s.subcode_) {
  message_ = (s.message_ == nullptr) ? nullptr : CopyMessage(s.message_.get());
}

auto Status::operator=(const Status& s) -> Status& {
  if (this == &s) {
    return *this;
  }
  code_ = s.code_;
  subcode_ = s.subcode_;
  message_ = (s.message_ == nullptr) ? nullptr : CopyMessage(s.message_.get());
  return *this;
}
Status::Status(Status&& s) noexcept {
  code_ = s.code_;
  subcode_ = s.subcode_;
  message_ = std::move(s.message_);
}

auto Status::operator=(Status&& s) noexcept -> Status& {
  if (this == &s) {
    return *this;
  }
  code_ = s.code_;
  subcode_ = s.subcode_;
  message_ = std::move(s.message_);
  return *this;
}
Status::Status(const Code code, const Subcode subcode) : Status(code, subcode, Slice()) {}

Status::Status(Code code, const Slice& msg) : Status(code, kNone, msg) {}

Status::Status(const Code code, const Subcode subcode, const Slice& msg)
    : code_(code), subcode_(subcode) {
  message_ = msg.Empty() ? nullptr : CopyMessage(msg.Data());
}

Status::Status(Code code, Subcode subcode, const Slice& msg, const Slice& msgv)
    : code_(code), subcode_(subcode) {
  const size_t msg_len = msg.Size();
  const size_t msgv_len = msgv.Size();
  const size_t size = msg_len + (msgv_len > 0 ? 2 + msgv_len : 0);
  char* result = new char[size + 1];  // +1 for c-style string
  memcpy(result, msg.Data(), msg_len);
  if (msgv_len > 0) {
    result[msg_len] = ':';
    result[msg_len + 1] = ' ';
    memcpy(result + msg_len + 2, msgv.Data(), msgv_len);
  }
  result[size] = '\0';
  message_.reset(result);
}

auto Status::CopyMessage(const char* s) -> std::unique_ptr<const char[]> {
  const size_t cch = std::strlen(s) + 1;  // +1 for the null terminator
  char* buf = new char[cch];
  std::strncpy(buf, s, cch);
  return std::unique_ptr<const char[]>(buf);
}
}  // namespace cckv