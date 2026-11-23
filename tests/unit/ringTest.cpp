#include <gtest/gtest.h>

#include <map>
#include <set>

#include "aegisfs/hash.hpp"
#include "aegisfs/ring.hpp"

using namespace aegisfs;

static Ring makeRing(const std::vector<std::string>& ids) {
  std::vector<RingNode> ns;
  for (const auto& id : ids) ns.push_back(RingNode{id, id + ":9000", 0});
  Ring r;
  r.setMembers(ns);
  return r;
}

TEST(RingTest, StableAcrossInstances) {
  Ring a = makeRing({"n1", "n2", "n3", "n4", "n5"});
  Ring b = makeRing({"n5", "n3", "n1", "n4", "n2"});  // different insertion order
  auto ra = a.successors("some-file", 3);
  auto rb = b.successors("some-file", 3);
  ASSERT_EQ(ra.size(), rb.size());
  for (size_t i = 0; i < ra.size(); ++i) EXPECT_EQ(ra[i].nodeId, rb[i].nodeId);
}

TEST(RingTest, ThreeDistinct) {
  Ring r = makeRing({"n1", "n2", "n3", "n4", "n5"});
  auto reps = r.successors("f", 3);
  ASSERT_EQ(reps.size(), 3u);
  std::set<std::string> ids;
  for (auto& x : reps) ids.insert(x.nodeId);
  EXPECT_EQ(ids.size(), 3u);
}

TEST(RingTest, FewerThanRfWhenSmallRing) {
  Ring r = makeRing({"n1", "n2"});
  EXPECT_EQ(r.successors("f", 3).size(), 2u);
  EXPECT_TRUE(makeRing({}).successors("f", 3).empty());
}

TEST(RingTest, SuccessorsWrap) {
  Ring r = makeRing({"n1", "n2", "n3"});
  // For many filenames, the first successor is always the first node with rid >= file rid,
  // and the set wraps correctly (always returns min(3, N) nodes).
  for (int i = 0; i < 200; ++i) {
    std::string f = "file-" + std::to_string(i);
    auto reps = r.successors(f, 3);
    EXPECT_EQ(reps.size(), 3u);
  }
}

TEST(RingTest, AgreementAcrossNodes) {
  // Every node computes identical placement given the same membership.
  Ring r = makeRing({"a", "b", "c", "d", "e", "f", "g"});
  for (int i = 0; i < 100; ++i) {
    std::string f = "obj" + std::to_string(i);
    auto reps = r.successors(f, 3);
    EXPECT_TRUE(r.isReplica(f, reps[0].nodeId, 3));
    EXPECT_TRUE(r.isReplica(f, reps[1].nodeId, 3));
    EXPECT_TRUE(r.isReplica(f, reps[2].nodeId, 3));
  }
}

TEST(RingTest, MinimalMovement) {
  // Adding a node should move only a bounded fraction of keys.
  std::vector<std::string> base = {"a", "b", "c", "d", "e"};
  Ring before = makeRing(base);
  auto base2 = base;
  base2.push_back("f");
  Ring after = makeRing(base2);

  int moved = 0, total = 2000;
  for (int i = 0; i < total; ++i) {
    std::string key = "k" + std::to_string(i);
    if (before.successors(key, 1)[0].nodeId != after.successors(key, 1)[0].nodeId) ++moved;
  }
  // With 6 nodes, roughly 1/6 of primary ownership should move; allow generous slack.
  EXPECT_LT(moved, total / 2);
}

TEST(RingTest, LoadIsReasonablyBalanced) {
  // With many nodes and keys, primary ownership should be spread out (no node owns a huge share).
  Ring r = makeRing({"a","b","c","d","e","f","g","h","i","j"});  // 10 nodes
  std::map<std::string, int> owns;
  const int K = 10000;
  for (int i = 0; i < K; ++i) owns[r.successors("key" + std::to_string(i), 1)[0].nodeId]++;
  ASSERT_EQ(owns.size(), 10u);  // every node owns at least one key
  int maxo = 0, mino = K;
  for (auto& [n, c] : owns) { maxo = std::max(maxo, c); mino = std::min(mino, c); }
  // ideal ~1000 each; allow a wide band but no node should dominate (>3x ideal) or starve.
  EXPECT_LT(maxo, 3000);
  EXPECT_GT(mino, 100);
}

TEST(RingTest, IsReplicaNegative) {
  Ring r = makeRing({"n1", "n2", "n3", "n4", "n5"});
  auto reps = r.successors("obj", 3);
  // a node NOT among the 3 successors is not a replica
  for (auto& m : r.members()) {
    bool in = false;
    for (auto& x : reps) if (x.nodeId == m.nodeId) in = true;
    EXPECT_EQ(r.isReplica("obj", m.nodeId, 3), in);
  }
}

TEST(RingTest, FirstSuccessorMonotone) {
  Ring r = makeRing({"x", "y", "z", "w"});
  uint64_t frid = r.fileRid("hello");
  size_t idx = r.firstAtOrAfter(frid);
  auto reps = r.successors("hello", 1);
  ASSERT_EQ(reps.size(), 1u);
  EXPECT_EQ(r.members()[idx].nodeId, reps[0].nodeId);
}
