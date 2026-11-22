#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <thread>

#include "support/cluster_fixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

static std::vector<std::string> replica_addrs(Client& c, const std::string& name) {
  auto ls = c.ls(name);
  std::vector<std::string> out;
  for (int i = 0; i < ls.info.get_i64("replicas"); ++i)
    out.push_back(ls.info.get_or("r" + std::to_string(i) + "_addr"));
  return out;
}

// A file with 3 replicas survives 2 node failures: the surviving replica still serves it.
TEST(Chaos, TwoNodeKillStillReadable) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("tk", 300000, 11);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "crit/f").ok());

  auto reps = replica_addrs(c, "crit/f");
  ASSERT_EQ(reps.size(), 3u);

  // kill the first two replicas (the third moves up to primary and keeps the data)
  int a = cl.index_of_addr(reps[0]);
  int b = cl.index_of_addr(reps[1]);
  int survivor = cl.index_of_addr(reps[2]);
  ASSERT_GE(a, 0); ASSERT_GE(b, 0); ASSERT_GE(survivor, 0);
  cl.stop_node(a);
  cl.stop_node(b);

  // pick an alive entry node that is not one of the killed
  int entry = -1;
  for (size_t i = 0; i < cl.size(); ++i)
    if (cl.alive(i)) { entry = static_cast<int>(i); break; }
  ASSERT_GE(entry, 0);

  Client reader(cl.addr(entry), "reader");
  std::string out = cl.root() + "/tk_out.bin";
  auto g = reader.get("crit/f", out);
  ASSERT_TRUE(g.ok()) << status_name(g.status);
  EXPECT_EQ(file_sha_hex(in), file_sha_hex(out));
}

// After a replica fails, re-replication restores the data onto a new successor.
TEST(Chaos, ReReplicationRestoresReplica) {
  Cluster cl(6);
  ASSERT_TRUE(cl.await_convergence(6));
  std::string in = make_temp_file("rr", 120000, 22);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "rep/f").ok());
  auto reps = replica_addrs(c, "rep/f");
  ASSERT_EQ(reps.size(), 3u);

  // kill one replica
  int victim = cl.index_of_addr(reps[0]);
  ASSERT_GE(victim, 0);
  cl.stop_node(victim);

  // wait for failure detection + reconcile to push data to a new successor
  std::string in_sha = file_sha_hex(in);
  bool restored = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  int entry = -1;
  for (size_t i = 0; i < cl.size(); ++i)
    if (cl.alive(i)) { entry = static_cast<int>(i); break; }
  Client probe(cl.addr(entry), "probe");
  while (std::chrono::steady_clock::now() < deadline) {
    auto now_reps = replica_addrs(probe, "rep/f");
    if (now_reps.size() == 3u) {
      int have = 0;
      for (auto& addr : now_reps) {
        int idx = cl.index_of_addr(addr);
        if (idx < 0 || !cl.alive(static_cast<size_t>(idx))) continue;
        std::string out = cl.root() + "/rr_" + std::to_string(idx) + ".bin";
        if (probe.get_from_replica(addr, "rep/f", out).ok() && file_sha_hex(out) == in_sha) ++have;
      }
      if (have >= 3) { restored = true; break; }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  EXPECT_TRUE(restored) << "3 live replicas should hold the data after re-replication";
}

// A stopped node restarted with its data dir intact still serves its persisted data.
TEST(Chaos, RestartNodeRecoversData) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("rc", 80000, 33);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "dur/f").ok());
  auto reps = replica_addrs(c, "dur/f");
  int idx = cl.index_of_addr(reps[0]);
  ASSERT_GE(idx, 0);

  cl.stop_node(idx);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  cl.restart_node(idx);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // the restarted node still has its persisted replica (served with no repair)
  std::string out = cl.root() + "/dur_out.bin";
  auto r = c.get_from_replica(cl.addr(idx), "dur/f", out);
  ASSERT_TRUE(r.ok()) << status_name(r.status);
  EXPECT_EQ(file_sha_hex(in), file_sha_hex(out));
}

// Corrupting a replica's segment on disk is detected and repaired by anti-entropy.
TEST(Chaos, AntiEntropyRepairsCorruptSegment) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("ae", 60000, 66);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "ae/f").ok());
  std::string in_sha = file_sha_hex(in);

  auto reps = replica_addrs(c, "ae/f");
  int victim = cl.index_of_addr(reps[0]);
  ASSERT_GE(victim, 0);
  std::string seg = cl.root() + "/node" + std::to_string(victim) + "/files/" +
                    make_file_id("ae/f") + "/base.seg";
  ASSERT_TRUE(fs::exists(seg));

  // corrupt the segment: flip bytes in the middle
  {
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(100);
    char junk[64];
    std::memset(junk, 0xAB, sizeof(junk));
    f.write(junk, sizeof(junk));
  }
  // immediately, a pure replica read should fail the checksum
  std::string bad = cl.root() + "/ae_bad.bin";
  auto g0 = c.get_from_replica(reps[0], "ae/f", bad);
  EXPECT_NE(g0.status, Status::Ok);  // corruption detected

  // anti-entropy should repair it from a healthy peer within a few seconds
  bool repaired = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
  while (std::chrono::steady_clock::now() < deadline) {
    std::string out = cl.root() + "/ae_out.bin";
    auto g = c.get_from_replica(reps[0], "ae/f", out);
    if (g.ok() && file_sha_hex(out) == in_sha) { repaired = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  EXPECT_TRUE(repaired) << "anti-entropy should restore the corrupt segment";
}

// A graceful leave hands off the leaving node's replicas; files stay available.
TEST(Chaos, GracefulLeaveHandsOffData) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("gl", 40000, 55);
  Client c = cl.client(0, "c1");
  for (int i = 0; i < 4; ++i) ASSERT_TRUE(c.create(in, "lv/f" + std::to_string(i)).ok());

  // pick a replica of lv/f0 and ask it to leave gracefully
  auto reps = replica_addrs(c, "lv/f0");
  int leaver = cl.index_of_addr(reps[0]);
  ASSERT_GE(leaver, 0);
  Client lc(cl.addr(leaver), "lc");
  ASSERT_TRUE(lc.leave().ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // let handoff + gossip settle

  // all files still readable and correct from a non-leaver entry
  int entry = (leaver + 1) % static_cast<int>(cl.size());
  Client r(cl.addr(entry), "r");
  std::string in_sha = file_sha_hex(in);
  for (int i = 0; i < 4; ++i) {
    std::string out = cl.root() + "/gl_" + std::to_string(i) + ".bin";
    auto g = r.get("lv/f" + std::to_string(i), out);
    ASSERT_TRUE(g.ok()) << status_name(g.status);
    EXPECT_EQ(file_sha_hex(out), in_sha);
  }
}

// Joining a new node keeps the file available (safe handoff; no transient unavailability).
TEST(Chaos, JoinKeepsFilesAvailable) {
  Cluster cl(4);
  ASSERT_TRUE(cl.await_convergence(4));
  std::string in = make_temp_file("jn", 50000, 44);
  Client c = cl.client(0, "c1");
  for (int i = 0; i < 5; ++i) ASSERT_TRUE(c.create(in, "join/f" + std::to_string(i)).ok());

  cl.add_node();
  ASSERT_TRUE(cl.await_convergence(5, 8000));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // let rebalance settle

  // all files remain readable and byte-correct from any entry
  std::string in_sha = file_sha_hex(in);
  for (int i = 0; i < 5; ++i) {
    Client r(cl.addr(4), "r");
    std::string out = cl.root() + "/jn_" + std::to_string(i) + ".bin";
    auto g = r.get("join/f" + std::to_string(i), out);
    ASSERT_TRUE(g.ok()) << status_name(g.status);
    EXPECT_EQ(file_sha_hex(out), in_sha);
  }
}
