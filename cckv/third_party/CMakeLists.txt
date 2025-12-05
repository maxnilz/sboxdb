# third_party/CMakeLists.txt
include(FetchContent)

# Fetch Google Test, maybe consider the fused-src distribution(deprecated) for
# offline vendoring?
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/releases/download/v1.17.0/googletest-1.17.0.tar.gz
)

# Prevent GTest from overriding compiler/linker settings
set(gtest_force_shared_crt
    ON
    CACHE BOOL "" FORCE)

set(BUILD_GMOCK
    OFF
    CACHE BOOL "" FORCE) # Optional: disable GMock if not needed

set(INSTALL_GTEST
    OFF
    CACHE BOOL "" FORCE) # Don't install GTest

FetchContent_MakeAvailable(googletest)
