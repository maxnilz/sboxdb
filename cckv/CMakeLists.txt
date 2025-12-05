cmake_minimum_required(VERSION 4.0)
project(
  cckv
  LANGUAGES C CXX
  VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(CCKV_ENABLE_COVERAGE "Enable coverage instrumentation for tests" OFF)

# cmake-format: off
### Core dumps for debugging
# 1. Enable on prod: ulimit -c unlimited (or systemd LimitCORE=infinity)
# 2. Check location: cat /proc/sys/kernel/core_pattern
# 3. Debug: gdb/lldb ./binary ./core.12345
# cmake-format: on
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=return-type")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -fno-omit-frame-pointer -DNDEBUG")

add_library(cckv STATIC)

target_sources(
  cckv
  PRIVATE src/status.cc
          src/internal/memory.h
          src/internal/memory.cc
          src/internal/codec.h
          src/internal/codec.cc
          src/internal/filesystem.h
          src/internal/fsposix.h
          src/internal/fsposix.cc
          src/internal/memtable.h
          src/internal/memtable.cc
          src/internal/storage.h
          src/internal/storage.cc)

target_include_directories(cckv PUBLIC include)

# cmake-format: off
### Tests with Google Test and CTest combination
# - CTest: The cmake builtin test runner
# - Google Test: C++ test framework with TEST() macros
# - gtest_discover_tests: Act as bridge that auto-registers GTest cases with CTest
# cmake-format: on
include(CTest) # Provides BUILD_TESTING option (default: ON)
if(BUILD_TESTING)
  enable_testing() # Allows add_test() commands and creates CTestTestfile.cmake
  add_subdirectory(third_party)

  add_executable(
    cckv_test src/internal/codec_test.cc src/internal/memory_test.cc src/internal/memtable_test.cc
              src/internal/fsposix_test.cc src/status_test.cc)

  target_link_libraries(
    cckv_test PRIVATE cckv GTest::gtest # Core framework (TEST(), EXPECT_EQ, etc.)
                      GTest::gtest_main # Provides main() - no need to write it
  )

  if(CCKV_ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(STATUS "Coverage enabled: adding --coverage to compile/link flags")
    # apply coverage to cckv with PUBLIC and let dependents, i.e., cckv_test, inherit it
    target_compile_options(cckv PUBLIC --coverage -O0 -g)
    target_link_options(cckv PUBLIC --coverage)

    message(STATUS "Coverage enabled: generate CTestCustom.cmake for coverage exclusions")
    file(WRITE ${CMAKE_BINARY_DIR}/CTestCustom.cmake "set(CTEST_CUSTOM_COVERAGE_EXCLUDE\n")
    file(APPEND ${CMAKE_BINARY_DIR}/CTestCustom.cmake "  \".*_test.cc\"\n")
    file(APPEND ${CMAKE_BINARY_DIR}/CTestCustom.cmake "  \".*/third_party/.*\"\n")
    file(APPEND ${CMAKE_BINARY_DIR}/CTestCustom.cmake "  \".*/_deps/.*\"\n")
    file(APPEND ${CMAKE_BINARY_DIR}/CTestCustom.cmake ")\n")
  endif()

  include(GoogleTest) # Provides gtest_discover_tests()
  # cmake-format: off
  ### gtest_discover_tests(cckv_test) basically doing the following:
  # - Builds cckv_test executable
  # - Runs it with --gtest_list_tests to discover all TEST() cases
  # - Registers each test individually with CTest
  # - Each Test() becomes a separate CTest entry
  # cmake-format: on
  gtest_discover_tests(cckv_test)
endif()
