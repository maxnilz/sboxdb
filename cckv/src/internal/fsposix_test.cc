#include "fsposix.h"

#include <filesystem>
#include <format>

#include "gtest/gtest.h"

namespace cckv::internal {

TEST(PosixFileSystem, Basic) {
  // create temporary directory
  char tmpl[] = "/tmp/cckv_posixfs_text_XXXXXX";
  char* tmpdir = mkdtemp(tmpl);
  ASSERT_NE(tmpdir, nullptr);
  std::string base = std::string(tmpdir);

  std::string fname = std::format("{}/nested/dir/file.txt", base);

  PosixFileSystem pfs;

  // create writable file and write content
  auto w = pfs.NewWritableFile(fname, FileOptions());
  ASSERT_TRUE(w.ok()) << w.errmsg();
  auto& writer = w.value();

  std::string content = "Hello, PosixFileSystem";
  auto s = writer->Append(content);
  ASSERT_TRUE(s.ok()) << s.message();

  s = writer->PWrite("h", 0);
  ASSERT_TRUE(s.ok()) << s.message();
  content[0] = 'h';
  ASSERT_EQ(content.size(), writer->Size());

  // check if file exists
  namespace fs = std::filesystem;
  ASSERT_TRUE(fs::exists(fname));

  // read using sequential file
  auto ssr = pfs.OpenSequentialFile(fname, FileOptions());
  ASSERT_TRUE(ssr.ok()) << ssr.errmsg();
  auto& sr = ssr.value();

  std::vector<char> seq_scratch(content.size());
  Slice seq_res;
  s = sr->Read(content.size(), &seq_res, seq_scratch.data());
  ASSERT_TRUE(s.ok()) << s.message();
  ASSERT_EQ(content, std::string(seq_res.Data(), seq_res.Size()));

  // read using random access file
  auto srr = pfs.OpenRandomAccessFile(fname, FileOptions());
  ASSERT_TRUE(ssr.ok()) << srr.errmsg();
  auto& rr = srr.value();

  std::vector<char> r_scratch(content.size());
  Slice r_res;
  s = rr->Read(0, content.size(), &r_res, r_scratch.data());
  ASSERT_TRUE(s.ok()) << s.message();
  ASSERT_EQ(content, std::string(r_res.Data(), r_res.Size()));
}
}  // namespace cckv::internal
