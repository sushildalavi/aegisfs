cmake_minimum_required(VERSION 3.20)
project(AegisFS VERSION 0.1.0 LANGUAGES CXX)

# ---- Standard ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

# ---- Options ----
option(AEGISFS_BUILD_TESTS "Build unit/integration tests" ON)
option(AEGISFS_ENABLE_ASAN "Build with AddressSanitizer+UBSan" OFF)
option(AEGISFS_ENABLE_TSAN "Build with ThreadSanitizer" OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

if(AEGISFS_ENABLE_ASAN)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
endif()
if(AEGISFS_ENABLE_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

# ---- OpenSSL (SHA-256) ----
# Help CMake find Homebrew OpenSSL on macOS.
if(APPLE AND NOT OPENSSL_ROOT_DIR)
  foreach(_p /opt/homebrew/opt/openssl@3 /usr/local/opt/openssl@3)
    if(EXISTS ${_p})
      set(OPENSSL_ROOT_DIR ${_p})
    endif()
  endforeach()
endif()
find_package(OpenSSL REQUIRED)

find_package(Threads REQUIRED)

# ---- Core library ----
add_library(aegisfsCore
  src/hash.cpp
  src/ring.cpp
  src/protocol.cpp
  src/storage.cpp
  src/manifest.cpp
  src/net.cpp
  src/membership.cpp
  src/replication.cpp
  src/client.cpp
  src/cli.cpp
  src/node.cpp
  src/metrics.cpp
)
target_include_directories(aegisfsCore PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(aegisfsCore PUBLIC OpenSSL::Crypto OpenSSL::SSL Threads::Threads)

# ---- Executable ----
add_executable(aegisfs src/main.cpp)
target_link_libraries(aegisfs PRIVATE aegisfsCore)

# ---- Tests ----
if(AEGISFS_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
    URL_HASH SHA256=7b42b4d6ed48810c5362c265a17faebe90dc2373c885e5216439d37927f02926
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  add_subdirectory(tests)
endif()
