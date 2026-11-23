#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "aegisfs/threadpool.hpp"

using namespace aegisfs;

TEST(ThreadPoolTest, RunsAllTasks) {
  std::atomic<int> count{0};
  {
    ThreadPool pool(4);
    for (int i = 0; i < 1000; ++i) pool.submit([&] { count.fetch_add(1); });
    pool.shutdown();  // joins workers -> all submitted tasks complete
  }
  EXPECT_EQ(count.load(), 1000);
}

TEST(ThreadPoolTest, ConcurrentExecution) {
  // With 4 workers, 4 tasks that each sleep 50ms should finish well under 200ms (serial time).
  ThreadPool pool(4);
  std::atomic<int> done{0};
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 4; ++i)
    pool.submit([&] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); done.fetch_add(1); });
  while (done.load() < 4) std::this_thread::sleep_for(std::chrono::milliseconds(2));
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - start).count();
  EXPECT_LT(elapsed, 180);  // concurrent, not serial (which would be ~200ms)
}

TEST(ThreadPoolTest, BackpressureBlocksWhenFull) {
  // A tiny queue + 1 slow worker: submit must not drop tasks; all eventually run.
  ThreadPool pool(1, /*maxQueue=*/2);
  std::atomic<int> count{0};
  for (int i = 0; i < 20; ++i) pool.submit([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    count.fetch_add(1);
  });
  pool.shutdown();
  EXPECT_EQ(count.load(), 20);
}
