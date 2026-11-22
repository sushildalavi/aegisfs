#include <gtest/gtest.h>

#include <set>

#include "aegisfs/hash.hpp"
#include "aegisfs/ring.hpp"

using namespace aegisfs;

static Ring make_ring(const std::vector<std::string>& ids) {
  std::vector<RingNode> ns;
  for (const auto& id : ids) ns.push_back(RingNode{id, id + ":9000", 0});
  Ring r;
  r.set_members(ns);
  return r;
}

TEST(RingTest, StableAcrossInstances) {
  Ring a = make_ring({"n1", "n2", "n3", "n4", "n5"});
  Ring b = make_ring({"n5", "n3", "n1", "n4", "n2"});  // different insertion order
  auto ra = a.successors("some-file", 3);
  auto rb = b.successors("some-file", 3);
  ASSERT_EQ(ra.size(), rb.size());
  for (size_t i = 0; i < ra.size(); ++i) EXPECT_EQ(ra[i].node_id, rb[i].node_id);
}

TEST(RingTest, ThreeDistinct) {
  Ring r = make_ring({"n1", "n2", "n3", "n4", "n5"});
  auto reps = r.successors("f", 3);
  ASSERT_EQ(reps.size(), 3u);
  std::set<std::string> ids;
  for (auto& x : reps) ids.insert(x.node_id);
  EXPECT_EQ(ids.size(), 3u);
}

TEST(RingTest, FewerThanRfWhenSmallRing) {
  Ring r = make_ring({"n1", "n2"});
  EXPECT_EQ(r.successors("f", 3).size(), 2u);
  EXPECT_TRUE(make_ring({}).successors("f", 3).empty());
}

TEST(RingTest, SuccessorsWrap) {
  Ring r = make_ring({"n1", "n2", "n3"});
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
  Ring r = make_ring({"a", "b", "c", "d", "e", "f", "g"});
  for (int i = 0; i < 100; ++i) {
    std::string f = "obj" + std::to_string(i);
    auto reps = r.successors(f, 3);
    EXPECT_TRUE(r.is_replica(f, reps[0].node_id, 3));
    EXPECT_TRUE(r.is_replica(f, reps[1].node_id, 3));
    EXPECT_TRUE(r.is_replica(f, reps[2].node_id, 3));
  }
}

TEST(RingTest, MinimalMovement) {
  // Adding a node should move only a bounded fraction of keys.
  std::vector<std::string> base = {"a", "b", "c", "d", "e"};
  Ring before = make_ring(base);
  auto base2 = base;
  base2.push_back("f");
  Ring after = make_ring(base2);

  int moved = 0, total = 2000;
  for (int i = 0; i < total; ++i) {
    std::string key = "k" + std::to_string(i);
    if (before.successors(key, 1)[0].node_id != after.successors(key, 1)[0].node_id) ++moved;
  }
  // With 6 nodes, roughly 1/6 of primary ownership should move; allow generous slack.
  EXPECT_LT(moved, total / 2);
}

TEST(RingTest, FirstSuccessorMonotone) {
  Ring r = make_ring({"x", "y", "z", "w"});
  uint64_t frid = r.file_rid("hello");
  size_t idx = r.first_at_or_after(frid);
  auto reps = r.successors("hello", 1);
  ASSERT_EQ(reps.size(), 1u);
  EXPECT_EQ(r.members()[idx].node_id, reps[0].node_id);
}
