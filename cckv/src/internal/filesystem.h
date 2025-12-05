#pragma once
#include "cckv/status.h"

namespace cckv::internal {

class WritableFile {
 public:
  WritableFile() = default;

  // No copying allowed
  WritableFile(const WritableFile&) = delete;
  void operator=(const WritableFile&) = delete;

  virtual ~WritableFile() = default;

  virtual auto Append(const Slice& data) -> Status = 0;

  // Positioned write.
  virtual auto PWrite(const Slice& data, uint64_t offset) -> Status = 0;

  // fsync the kernel dirty pages for the give to stable media explicitly.
  // There are different ways to sync the dirty pages in page cache to stable media:
  //  1. fsync
  //  2. fdatasync
  //  3. open file with O_SYNC/O_DSYNC so each write behaves like a write + sync.
  //  4. for mmaped IO: msync(addr, len, MS_SYNC)
  virtual auto Sync() -> Status = 0;
  // close the file.
  virtual auto Close() -> Status = 0;

  virtual auto Size() const -> uint64_t = 0;
  virtual auto Path() const -> const std::string& = 0;
};

class RandomAccessFile {
 public:
  RandomAccessFile() = default;

  // No copying allowed
  RandomAccessFile(const RandomAccessFile&) = delete;
  auto operator=(RandomAccessFile&) -> RandomAccessFile& = delete;

  virtual ~RandomAccessFile() = default;

  virtual auto Read(size_t offset, size_t n, Slice* result, char* scratch) -> Status = 0;

  virtual auto Size() const -> StatusOr<uint64_t> = 0;
  virtual auto Path() const -> const std::string& = 0;
};

class SequentialFile {
 public:
  SequentialFile() = default;

  // No copying allowed
  SequentialFile(const SequentialFile&) = delete;
  auto operator=(SequentialFile&) -> SequentialFile& = delete;

  virtual ~SequentialFile() = default;

  virtual auto Read(size_t n, Slice* result, char* scratch) -> Status = 0;

  virtual auto Path() const -> const std::string& = 0;
};

struct FileOptions {};

class FileSystem {
 public:
  FileSystem() = default;

  // No copying allowed
  FileSystem(const FileSystem&) = delete;
  auto operator=(FileSystem&) -> FileSystem& = delete;

  virtual ~FileSystem() = default;

  // Create a writable file for writing. Delete any existing file with the same
  // name and create a new file.
  virtual auto NewWritableFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<WritableFile>> = 0;

  // Open an existing file for writing. the writing pointer point to the EOF. If
  // the file does not exist, creates it.
  virtual auto OpenWritableFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<WritableFile>> = 0;

  // Open an existing file for random read. If the file does not exist, return a
  // non-OK status.
  virtual auto OpenRandomAccessFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<RandomAccessFile>> = 0;

  // Open an existing file for sequential read. If the file does not exist, return
  // a non-OK status.
  virtual auto OpenSequentialFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<SequentialFile>> = 0;
};

class IOStatus : public Status {
 public:
  static auto PathNotFound(const Slice& msg, const Slice& msgv = Slice()) -> IOStatus {
    return IOStatus(kIOError, kPathNotFound, msg, msgv);
  }

  static auto Unknown(const Slice& msg, const Slice& msgv = Slice()) -> IOStatus {
    return IOStatus(kIOError, msg, msgv);
  }

 private:
  IOStatus(Code code, const Slice& msg, const Slice& msgv = Slice())
      : IOStatus(code, kNone, msg, msgv) {}
  IOStatus(Code code, Subcode subcode, const Slice& msg, const Slice& msgv = Slice()) {
    assert(code != kOk);
    assert(subcode != kMaxSubCode);

    code_ = code;
    subcode_ = subcode;

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
};
}  // namespace cckv::internal
