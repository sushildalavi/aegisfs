include(GoogleTest)

# One test binary per logical area keeps failures isolated and parallelizable.
set(AEGISFS_TEST_SRCS
  unit/hashTest.cpp
  unit/ringTest.cpp
  unit/protocolTest.cpp
  unit/manifestTest.cpp
  unit/storageTest.cpp
  unit/membershipTest.cpp
  unit/netTest.cpp
  unit/commonTest.cpp
  unit/metricsTest.cpp
  unit/threadpoolTest.cpp
  integration/clusterTest.cpp
  integration/replicationTest.cpp
  concurrency/concurrencyTest.cpp
  concurrency/stressTest.cpp
  chaos/failureTest.cpp
  chaos/partitionTest.cpp
)

foreach(src ${AEGISFS_TEST_SRCS})
  get_filename_component(name ${src} NAME_WE)
  add_executable(${name} ${src})
  target_link_libraries(${name} PRIVATE aegisfsCore GTest::gtest_main)
  target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/tests)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 120)
endforeach()
