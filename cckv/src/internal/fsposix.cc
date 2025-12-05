#include "fsposix.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <filesystem>
#include <format>
#include <utility>

namespace cckv::internal {

// TU-local helpers that have no header declaration
namespace {
auto ErrnoStr(int err_num) -> std::string {
  char buf[1024];
  buf[0] = '\0';  // ensure null-terminated if strerror_r fails
  // Using GNU strerror_r, returns a pointer to a string containing the
  // error message.  This may be either a pointer to a string that the
  // function stores in buf, or a pointer to some (immutable) static
  // string (in which case buf is unused).
  char* ptr = strerror_r(err_num, buf, sizeof(buf));
  return std::string(ptr);
}

auto IOErrmsg(const std::string& path, const std::string& msg) -> std::string {
  if (msg.empty()) {
    return path;
  }
  return msg + ": " + path;
}

auto IOError(const std::string& msg, const std::string& path, int err_num) -> IOStatus {
  switch (err_num) {
    case ENOENT:
      return IOStatus::PathNotFound(IOErrmsg(path, msg), ErrnoStr(err_num));
    default:
      return IOStatus::Unknown(IOErrmsg(path, msg), ErrnoStr(err_num));
  }
}
}  // namespace

auto PosixFileSystem::NewWritableFile(const std::string& fname, const FileOptions& options)
    -> StatusOr<std::unique_ptr<WritableFile>> {
  return OpenWritableFile(fname, false, options);
}
auto PosixFileSystem::OpenWritableFile(const std::string& fname, const FileOptions& options)
    -> StatusOr<std::unique_ptr<WritableFile>> {
  return OpenWritableFile(fname, true, options);
}

auto PosixFileSystem::OpenRandomAccessFile(const std::string& fname, const FileOptions& options)
    -> StatusOr<std::unique_ptr<RandomAccessFile>> {
  int flags = O_RDONLY | O_CLOEXEC;
  int fd = open(fname.c_str(), flags);
  if (fd < 0) {
    return IOError("Opening file for random read", fname, errno);
  }
  std::unique_ptr<RandomAccessFile> random_access_file =
      std::make_unique<PosixRandomAccessFile>(fname, fd);
  return StatusOr(std::move(random_access_file));
}

auto PosixFileSystem::OpenSequentialFile(const std::string& fname, const FileOptions& options)
    -> StatusOr<std::unique_ptr<SequentialFile>> {
  int flags = O_RDONLY | O_CLOEXEC;
  int fd = open(fname.c_str(), flags);
  if (fd < 0) {
    return IOError("Opening file for sequential read", fname, errno);
  }
  std::unique_ptr<SequentialFile> sequential_file =
      std::make_unique<PosixSequentialFile>(fname, fd);
  return StatusOr(std::move(sequential_file));
}

auto PosixFileSystem::OpenWritableFile(const std::string& fname, bool reopen,
                                       [[maybe_unused]] const FileOptions& options)
    -> StatusOr<std::unique_ptr<WritableFile>> {
  // Ensure the parent directories
  auto s = EnsureDirectories(fname);
  if (!s.ok()) {
    return StatusOr<std::unique_ptr<WritableFile>>(std::move(s));
  }

  int flags = reopen ? (O_CREAT | O_APPEND) : (O_CREAT | O_TRUNC);
  // Note: we should avoid O_APPEND here due to ta the following bug:
  // POSIX requires that opening a file with the O_APPEND flag should
  // have no affect on the location at which pwrite() writes data.
  // However, on Linux, if a file is opened with O_APPEND, pwrite()
  // appends data to the end of the file, regardless of the value of
  // offset.
  // More info here: https://linux.die.net/man/2/pwrite
  flags |= O_WRONLY;
  flags |= O_CLOEXEC;

  // TODO: support direct io.

  mode_t mode = 0644;
  int fd = open(fname.c_str(), flags, mode);
  if (fd < 0) {
    return IOError("Opening file for writing", fname, errno);
  }
  size_t initial_file_size = 0;
  if (reopen) {
    struct stat sb {};
    if (fstat(fd, &sb) < 0) {
      close(fd);
      return IOError("fstat a file for size with fd " + std::to_string(fd), fname, errno);
    }
    initial_file_size = sb.st_size;
  }
  std::unique_ptr<WritableFile> writable_file =
      std::make_unique<PosixWritableFile>(fname, fd, initial_file_size);
  return StatusOr(std::move(writable_file));
}

auto PosixFileSystem::EnsureDirectories(const std::string& fname) -> Status {
  namespace fs = std::filesystem;
  fs::path path(fname);
  fs::path parent = path.parent_path();
  if (path.empty()) {
    return Status::Ok();
  }

  std::error_code ec;
  bool ok = fs::exists(parent, ec);
  if (ec) {
    return IOError("checking parent dir existence", parent.string(), ec.value());
  }
  if (ok) {
    return Status::Ok();
  }

  fs::create_directories(parent, ec);
  if (ec) {
    return IOError("creating parent dir", parent.string(), ec.value());
  }

  int fd = open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return IOError("open parent dir for sync", parent.string(), errno);
  }

  if (fsync(fd) < 0) {
    close(fd);
    return IOError("fsync parent dir", parent.string(), errno);
  }
  close(fd);

  return Status::Ok();
}

PosixWritableFile::PosixWritableFile(std::string fname, int fd, size_t initial_size)
    : filename_(std::move(fname)), fd_(fd), filesize_(initial_size) {}
PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    close(fd_);
  }
}

auto PosixWritableFile::Append(const Slice& data) -> Status {
  const char* ptr = data.Data();

  ssize_t r = 0;
  size_t left = data.Size();
  while (left > 0) {
    r = write(fd_, ptr, left);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    left -= r;
  }
  if (r < 0) {
    return IOError("append to file", filename_, errno);
  }
  filesize_ += data.Size();
  return Status::Ok();
}

auto PosixWritableFile::PWrite(const Slice& data, uint64_t offset) -> Status {
  const char* ptr = data.Data();

  ssize_t r = 0;
  size_t left = data.Size();
  while (left > 0) {
    r = pwrite(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;

    left -= r;
  }
  if (r < 0) {
    return IOError("pwrite to file", filename_, errno);
  }
  filesize_ = std::max(offset + data.Size(), filesize_);
  return Status::Ok();
}

auto PosixWritableFile::Sync() -> Status {
  if (fdatasync(fd_) < 0) {
    return IOError("fdatasync", filename_, errno);
  }
  return Status::Ok();
}

auto PosixWritableFile::Close() -> Status {
  if (close(fd_) < 0) {
    return IOError("close file after writing", filename_, errno);
  }
  fd_ = -1;
  return Status::Ok();
}

auto PosixWritableFile::Path() const -> const std::string& { return filename_; }

auto PosixWritableFile::Size() const -> uint64_t { return filesize_; }

PosixRandomAccessFile::PosixRandomAccessFile(std::string fname, int fd)
    : filename_(std::move(fname)), fd_(fd) {}
PosixRandomAccessFile::~PosixRandomAccessFile() {
  assert(fd_);
  close(fd_);
}
auto PosixRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                 char* scratch) -> Status {
  char* ptr = scratch;

  // The number of bytes pread/read may be less than requested,
  // even if no error occurs. and it can happen in case of non-direct
  // io as well.

  ssize_t r = 0;
  size_t left = n;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;

    left -= r;
  }
  if (r < 0) {
    return IOError(std::format("pread offset {} len {}", offset, n), filename_, errno);
  }
  *result = Slice(scratch, n - left);
  return Status::Ok();
}

auto PosixRandomAccessFile::Size() const -> StatusOr<uint64_t> {
  struct stat sb {};
  if (fstat(fd_, &sb) < 0) {
    return StatusOr<uint64_t>(
        IOError(std::format("fstat a file for size with fd {}", fd_), filename_, errno));
  }
  return StatusOr<uint64_t>(sb.st_size);
}

auto PosixRandomAccessFile::Path() const -> const std::string& { return filename_; }

PosixSequentialFile::PosixSequentialFile(std::string fname, int fd)
    : filename_(std::move(fname)), fd_(fd) {}

auto PosixSequentialFile::Read(size_t n, Slice* result, char* scratch) -> Status {
  char* ptr = scratch;
  ssize_t r = 0;
  size_t left = n;
  while (left > 0) {
    r = read(fd_, ptr, left);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    left -= r;
  }
  if (r < 0) {
    return IOError(std::format("read len {}", n), filename_, errno);
  }
  *result = Slice(scratch, n - left);
  return Status::Ok();
}

auto PosixSequentialFile::Path() const -> const std::string& { return filename_; }

}  // namespace cckv::internal