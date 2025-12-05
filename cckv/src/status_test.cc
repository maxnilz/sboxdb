#include <gtest/gtest.h>

#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>

namespace cckv {
TEST(HelloWorld, Hello) {
  std::string hello = "Hello, World!";
  std::cout << std::format("{}\n", hello);

  int value = 42;
  std::cout << std::format("value = {}, hex = {:#x}\n", value, value);

  EXPECT_EQ(hello, "Hello, World!");
}

TEST(HelloWorld, String) {
  std::string s;
  s.resize(10);
  s.append("abc");
  std::cout << s.size();
}

// Scan the project .h and .cc files, flat
// them into a string buffer, in the format of:
// path/to/file
// ```
// file content
// ```
TEST(HelloWorld, DISABLED_ScanProjectFiles) {
  namespace fs = std::filesystem;

  const fs::path root = fs::current_path().parent_path();

  const fs::path out_path = root / "code_overview.md";
  std::ofstream out(out_path, std::ios::binary);
  ASSERT_TRUE(out.is_open()) << "Failed to open: " << out_path.string();

  std::set<std::string> ignore_files{
      "_test.cc",
      ".cc",
  };
  // lambda to scan a dir
  auto scan_dir = [&](const fs::path& dir) {
    for (const auto& entry : fs::recursive_directory_iterator(dir)) {
      if (!entry.is_regular_file()) {
        continue;
      }
      const fs::path& file_path = entry.path();
      if (std::string ext = file_path.extension().string(); ext != ".h" && ext != ".cc") {
        continue;
      }

      bool ignored = false;
      std::string filename(file_path.filename());
      for (const auto& it : ignore_files) {
        if (filename.ends_with(it)) {
          ignored = true;
          break;
        }
      }
      if (ignored) {
        continue;
      }

      fs::path rel_path = fs::relative(file_path, root);
      out << rel_path.string() << "\n";
      out << "```cpp\n";

      // read file content
      if (std::ifstream file(file_path); file.good()) {
        char last = 0;
        std::istreambuf_iterator<char> it(file.rdbuf());
        std::istreambuf_iterator<char> end;
        for (; it != end; ++it) {
          last = *it;
          out.put(last);
        }
        // check whether the file end up with a newline.
        if (last != 0 && last != '\n') {
          out.put('\n');
        }
      }
      out << "```\n\n";
    }
  };

  const fs::path inc_dir = root / "include";
  if (fs::exists(inc_dir)) {
    scan_dir(inc_dir);
  }

  const fs::path src_dir = root / "src";
  if (fs::exists(src_dir)) {
    scan_dir(src_dir);
  }
}
}  // namespace cckv
