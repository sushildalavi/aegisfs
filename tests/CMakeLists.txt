include(GoogleTest)

# One test binary per logical area keeps failures isolated and parallelizable.
set(AEGISFS_TEST_SRCS
  unit/hash_test.cpp
  unit/ring_test.cpp
  unit/protocol_test.cpp
  unit/manifest_test.cpp
  unit/storage_test.cpp
  unit/membership_test.cpp
  unit/net_test.cpp
  unit/common_test.cpp
  unit/metrics_test.cpp
  unit/threadpool_test.cpp
  integration/cluster_test.cpp
  integration/replication_test.cpp
  concurrency/concurrency_test.cpp
  concurrency/stress_test.cpp
  chaos/failure_test.cpp
)

foreach(src ${AEGISFS_TEST_SRCS})
  get_filename_component(name ${src} NAME_WE)
  add_executable(${name} ${src})
  target_link_libraries(${name} PRIVATE aegisfs_core GTest::gtest_main)
  target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/tests)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 120)
endforeach()
