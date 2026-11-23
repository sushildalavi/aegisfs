#include <gtest/gtest.h>

#include <atomic>
#include <random>
#include <thread>

#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

// Deterministic (seeded) mixed workload: many files created and appended to by several clients,
// then every file is read back and verified. Exercises the system under sustained concurrency.
TEST(Stress, MixedWorkloadThousandsOfOps) {
  const int CLIENTS = 8;
  const int FILES_PER_CLIENT = 10;
  const int APPENDS_PER_FILE = 8;  // ~8*10*8 = 640 appends + 80 creates ≈ 720 ops
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));

  std::string base = makeTempFile("stBase", 256, 1);
  std::string ap = makeTempFile("stAp", 64, 2);

  std::atomic<int> createFail{0}, appendFail{0};
  std::vector<std::thread> ts;
  for (int cidx = 0; cidx < CLIENTS; ++cidx) {
    ts.emplace_back([&, cidx] {
      Client c(cl.addr(cidx % cl.size()), "stress" + std::to_string(cidx));
      std::mt19937 rng(1000 + cidx);  // deterministic seed per client
      for (int f = 0; f < FILES_PER_CLIENT; ++f) {
        std::string name = "stress/c" + std::to_string(cidx) + "_f" + std::to_string(f);
        if (!c.create(base, name).ok()) { createFail.fetch_add(1); continue; }
        for (int a = 0; a < APPENDS_PER_FILE; ++a) {
          auto r = c.append(ap, name);
          if (!r.ok() && r.status != Status::Degraded) appendFail.fetch_add(1);
        }
      }
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(createFail.load(), 0);
  EXPECT_EQ(appendFail.load(), 0);

  // verify every file is readable and has the expected size
  uint64_t baseSz = fs::file_size(base), apSz = fs::file_size(ap);
  Client reader(cl.addr(0), "verifier");
  int checked = 0;
  for (int cidx = 0; cidx < CLIENTS; ++cidx) {
    for (int f = 0; f < FILES_PER_CLIENT; ++f) {
      std::string name = "stress/c" + std::to_string(cidx) + "_f" + std::to_string(f);
      std::string out = cl.root() + "/st_" + std::to_string(cidx) + "_" + std::to_string(f) + ".bin";
      auto g = reader.get(name, out);
      ASSERT_TRUE(g.ok()) << name << " -> " << statusName(g.status);
      EXPECT_EQ(fs::file_size(out), baseSz + APPENDS_PER_FILE * apSz) << name;
      ++checked;
    }
  }
  EXPECT_EQ(checked, CLIENTS * FILES_PER_CLIENT);
}
