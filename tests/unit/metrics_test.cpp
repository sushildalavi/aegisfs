#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "aegisfs/metrics.hpp"

using namespace aegisfs;

TEST(MetricsTest, CountersAccumulate) {
  Metrics& m = Metrics::instance();
  uint64_t before = m.get("unit.counter");
  m.incr("unit.counter");
  m.incr("unit.counter", 4);
  EXPECT_EQ(m.get("unit.counter"), before + 5);
}

TEST(MetricsTest, CountersAreThreadSafe) {
  Metrics& m = Metrics::instance();
  uint64_t before = m.get("unit.mt");
  std::vector<std::thread> ts;
  for (int i = 0; i < 8; ++i)
    ts.emplace_back([&] { for (int j = 0; j < 1000; ++j) m.incr("unit.mt"); });
  for (auto& t : ts) t.join();
  EXPECT_EQ(m.get("unit.mt"), before + 8000);
}

TEST(MetricsTest, LatencyPercentiles) {
  Metrics& m = Metrics::instance();
  for (uint64_t v = 1; v <= 100; ++v) m.record_latency("unit.lat", v);
  auto s = m.latency("unit.lat");
  EXPECT_EQ(s.count, 100u);
  EXPECT_GE(s.p50, 40u);
  EXPECT_LE(s.p50, 60u);
  EXPECT_GE(s.p99, 95u);
  EXPECT_EQ(s.max, 100u);
  EXPECT_GT(s.mean, 40.0);
  EXPECT_LT(s.mean, 60.0);
}

TEST(MetricsTest, ScopedTimerRecords) {
  Metrics& m = Metrics::instance();
  uint64_t before = m.latency("unit.scoped").count;
  { ScopedTimer t("unit.scoped"); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
  EXPECT_EQ(m.latency("unit.scoped").count, before + 1);
}
