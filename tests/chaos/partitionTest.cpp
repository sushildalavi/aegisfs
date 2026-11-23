#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "aegisfs/partition.hpp"
#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

// A point-to-point network partition, two nodes can't reach each other directly, but every
// other node can still reach both, must not cause either side to declare the other failed.
// Without SWIM's indirect probe ("ping-req"), a direct-probe failure alone would be enough to
// mark a perfectly healthy peer suspect; this test blocks only the direct path between two
// nodes and asserts neither one is ever marked down.
TEST(Chaos, IndirectProbeSurvivesPointToPointPartition) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  fault::partitionHealAll();

  std::string a = cl.addr(0);
  std::string b = cl.addr(1);
  fault::partitionBlock(a, b);

  // Give the failure detector several probe cycles to run (test fixture: probeIntervalMs=120,
  // suspectMs=300), long enough that, without indirect probing, both sides would have been
  // marked suspect and then failed by now.
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
  bool everFailed = false;
  while (std::chrono::steady_clock::now() < deadline) {
    if (cl.node(0)->membership()->aliveCount() < 5 || cl.node(1)->membership()->aliveCount() < 5) {
      everFailed = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_FALSE(everFailed) << "a point-to-point partition should not cause any node to be "
                               "marked down, since every other node can still reach both sides";

  fault::partitionHealAll();
}

// A node that is unreachable by everyone, not just one peer, must still be detected
// as failed. This guards against the indirect-probe fix above accidentally masking real failures.
TEST(Chaos, TrueIsolationStillDetectedAsFailed) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  fault::partitionHealAll();

  std::string isolated = cl.addr(2);
  for (size_t i = 0; i < cl.size(); ++i) {
    if (i == 2) continue;
    fault::partitionBlock(isolated, cl.addr(i));
  }

  bool detected = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
  while (std::chrono::steady_clock::now() < deadline) {
    if (cl.node(0)->membership()->aliveCount() < 5) {
      detected = true;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  EXPECT_TRUE(detected) << "a node unreachable by every peer must still be marked down";

  fault::partitionHealAll();
}
