#pragma once
#include "filesystem.h"

namespace cckv::internal {
class PosixFileSystem final : public FileSystem {
 public:
  PosixFileSystem() = default;

  ~PosixFileSystem() override = default;

  PosixFileSystem(PosixFileSystem&) = delete;
  auto operator=(PosixFileSystem&) -> PosixFileSystem& = delete;

  auto NewWritableFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<WritableFile>> override;
  auto OpenWritableFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<WritableFile>> override;
  auto OpenRandomAccessFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<RandomAccessFile>> override;
  auto OpenSequentialFile(const std::string& fname, const FileOptions& options)
      -> StatusOr<std::unique_ptr<SequentialFile>> override;

 private:
  static auto OpenWritableFile(const std::string& fname, bool reopen, const FileOptions& options)
      -> StatusOr<std::unique_ptr<WritableFile>>;
  static auto EnsureDirectories(const std::string& fname) -> Status;
};

class PosixWritableFile final : public WritableFile {
 public:
  PosixWritableFile(std::string fname, int fd, size_t initial_size);

  ~PosixWritableFile() override;

  auto Append(const Slice& data) -> Status override;
  auto PWrite(const Slice& data, uint64_t offset) -> Status override;
  auto Sync() -> Status override;
  auto Close() -> Status override;

  auto Path() const -> const std::string& override;
  auto Size() const -> uint64_t override;

 private:
  std::string filename_;
  int fd_;
  size_t filesize_;
};

class PosixRandomAccessFile final : public RandomAccessFile {
 public:
  PosixRandomAccessFile(std::string fname, int fd);

  ~PosixRandomAccessFile() override;

  auto Read(uint64_t offset, size_t n, Slice* result, char* scratch) -> Status override;
  auto Size() const -> StatusOr<uint64_t> override;
  auto Path() const -> const std::string& override;

 private:
  std::string filename_;
  int fd_;
};

class PosixSequentialFile final : public SequentialFile {
 public:
  PosixSequentialFile(std::string fname, int fd);

  ~PosixSequentialFile() override = default;

  auto Read(size_t n, Slice* result, char* scratch) -> Status override;
  auto Path() const -> const std::string& override;

 private:
  std::string filename_;
  int fd_;
};
}  // namespace cckv::internal