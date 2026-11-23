#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <thread>

#include "aegisfs/manifest.hpp"
#include "aegisfs/partition.hpp"
#include "aegisfs/replication.hpp"
#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

static std::vector<std::string> replicaAddrs(Client& c, const std::string& name) {
  auto ls = c.ls(name);
  std::vector<std::string> out;
  for (int i = 0; i < ls.info.getI64("replicas"); ++i)
    out.push_back(ls.info.getOr("r" + std::to_string(i) + "_addr"));
  return out;
}

// A file with 3 replicas survives 2 node failures: the surviving replica still serves it.
TEST(Chaos, TwoNodeKillStillReadable) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("tk", 300000, 11);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "crit/f").ok());

  auto reps = replicaAddrs(c, "crit/f");
  ASSERT_EQ(reps.size(), 3u);

  // kill the first two replicas (the third moves up to primary and keeps the data)
  int a = cl.indexOfAddr(reps[0]);
  int b = cl.indexOfAddr(reps[1]);
  int survivor = cl.indexOfAddr(reps[2]);
  ASSERT_GE(a, 0); ASSERT_GE(b, 0); ASSERT_GE(survivor, 0);
  cl.stopNode(a);
  cl.stopNode(b);

  // pick an alive entry node that is not one of the killed
  int entry = -1;
  for (size_t i = 0; i < cl.size(); ++i)
    if (cl.alive(i)) { entry = static_cast<int>(i); break; }
  ASSERT_GE(entry, 0);

  Client reader(cl.addr(entry), "reader");
  std::string out = cl.root() + "/tkOut.bin";
  auto g = reader.get("crit/f", out);
  ASSERT_TRUE(g.ok()) << statusName(g.status);
  EXPECT_EQ(fileShaHex(in), fileShaHex(out));
}

// After a replica fails, re-replication restores the data onto a new successor.
TEST(Chaos, ReReplicationRestoresReplica) {
  Cluster cl(6);
  ASSERT_TRUE(cl.awaitConvergence(6));
  std::string in = makeTempFile("rr", 120000, 22);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "rep/f").ok());
  auto reps = replicaAddrs(c, "rep/f");
  ASSERT_EQ(reps.size(), 3u);

  // kill one replica
  int victim = cl.indexOfAddr(reps[0]);
  ASSERT_GE(victim, 0);
  cl.stopNode(victim);

  // wait for failure detection + reconcile to push data to a new successor
  std::string inSha = fileShaHex(in);
  bool restored = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  int entry = -1;
  for (size_t i = 0; i < cl.size(); ++i)
    if (cl.alive(i)) { entry = static_cast<int>(i); break; }
  Client probe(cl.addr(entry), "probe");
  while (std::chrono::steady_clock::now() < deadline) {
    auto nowReps = replicaAddrs(probe, "rep/f");
    if (nowReps.size() == 3u) {
      int have = 0;
      for (auto& addr : nowReps) {
        int idx = cl.indexOfAddr(addr);
        if (idx < 0 || !cl.alive(static_cast<size_t>(idx))) continue;
        std::string out = cl.root() + "/rr_" + std::to_string(idx) + ".bin";
        if (probe.getFromReplica(addr, "rep/f", out).ok() && fileShaHex(out) == inSha) ++have;
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
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("rc", 80000, 33);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "dur/f").ok());
  auto reps = replicaAddrs(c, "dur/f");
  int idx = cl.indexOfAddr(reps[0]);
  ASSERT_GE(idx, 0);

  cl.stopNode(idx);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  cl.restartNode(idx);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // the restarted node still has its persisted replica (served with no repair)
  std::string out = cl.root() + "/durOut.bin";
  auto r = c.getFromReplica(cl.addr(idx), "dur/f", out);
  ASSERT_TRUE(r.ok()) << statusName(r.status);
  EXPECT_EQ(fileShaHex(in), fileShaHex(out));
}

// Corrupting a replica's segment on disk is detected and repaired by anti-entropy.
TEST(Chaos, AntiEntropyRepairsCorruptSegment) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("ae", 60000, 66);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "ae/f").ok());
  std::string inSha = fileShaHex(in);

  auto reps = replicaAddrs(c, "ae/f");
  int victim = cl.indexOfAddr(reps[0]);
  ASSERT_GE(victim, 0);
  std::string seg = cl.root() + "/node" + std::to_string(victim) + "/files/" +
                    makeFileId("ae/f") + "/base.seg";
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
  std::string bad = cl.root() + "/aeBad.bin";
  auto g0 = c.getFromReplica(reps[0], "ae/f", bad);
  EXPECT_NE(g0.status, Status::Ok);  // corruption detected

  // anti-entropy should repair it from a healthy peer within a few seconds
  bool repaired = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
  while (std::chrono::steady_clock::now() < deadline) {
    std::string out = cl.root() + "/aeOut.bin";
    auto g = c.getFromReplica(reps[0], "ae/f", out);
    if (g.ok() && fileShaHex(out) == inSha) { repaired = true; break; }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  EXPECT_TRUE(repaired) << "anti-entropy should restore the corrupt segment";
}

// A graceful leave hands off the leaving node's replicas; files stay available.
TEST(Chaos, GracefulLeaveHandsOffData) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("gl", 40000, 55);
  Client c = cl.client(0, "c1");
  for (int i = 0; i < 4; ++i) ASSERT_TRUE(c.create(in, "lv/f" + std::to_string(i)).ok());

  // pick a replica of lv/f0 and ask it to leave gracefully
  auto reps = replicaAddrs(c, "lv/f0");
  int leaver = cl.indexOfAddr(reps[0]);
  ASSERT_GE(leaver, 0);
  Client lc(cl.addr(leaver), "lc");
  ASSERT_TRUE(lc.leave().ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));  // let handoff + gossip settle

  // all files still readable and correct from a non-leaver entry
  int entry = (leaver + 1) % static_cast<int>(cl.size());
  Client r(cl.addr(entry), "r");
  std::string inSha = fileShaHex(in);
  for (int i = 0; i < 4; ++i) {
    std::string out = cl.root() + "/gl_" + std::to_string(i) + ".bin";
    auto g = r.get("lv/f" + std::to_string(i), out);
    ASSERT_TRUE(g.ok()) << statusName(g.status);
    EXPECT_EQ(fileShaHex(out), inSha);
  }
}

// Joining a new node keeps the file available (safe handoff; no transient unavailability).
TEST(Chaos, JoinKeepsFilesAvailable) {
  Cluster cl(4);
  ASSERT_TRUE(cl.awaitConvergence(4));
  std::string in = makeTempFile("jn", 50000, 44);
  Client c = cl.client(0, "c1");
  for (int i = 0; i < 5; ++i) ASSERT_TRUE(c.create(in, "join/f" + std::to_string(i)).ok());

  cl.addNode();
  ASSERT_TRUE(cl.awaitConvergence(5, 8000));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));  // let rebalance settle

  // all files remain readable and byte-correct from any entry
  std::string inSha = fileShaHex(in);
  for (int i = 0; i < 5; ++i) {
    Client r(cl.addr(4), "r");
    std::string out = cl.root() + "/jn_" + std::to_string(i) + ".bin";
    auto g = r.get("join/f" + std::to_string(i), out);
    ASSERT_TRUE(g.ok()) << statusName(g.status);
    EXPECT_EQ(fileShaHex(out), inSha);
  }
}

// If the primary replica for a file dies right as a create is issued — after this coordinator's
// placement still lists it as alive, before the failure detector catches up — the write to the
// primary fails. The coordinator must treat that as a hard failure rather than falling through
// to write the base on the secondaries instead: otherwise the primary comes back up still empty,
// and a second create with different content could commit a competing base there.
TEST(Chaos, PrimaryFailureDuringCreateDoesNotOrphanBase) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));

  Client probe = cl.client(0, "probe");
  auto ls = probe.ls("race/f");
  ASSERT_TRUE(ls.ok());
  std::string r0 = ls.info.getOr("r0_addr");
  std::string r1 = ls.info.getOr("r1_addr");
  std::string r2 = ls.info.getOr("r2_addr");
  int entryIdx = -1;
  for (size_t i = 0; i < cl.size(); ++i) {
    std::string a = cl.addr(i);
    if (a != r0 && a != r1 && a != r2) { entryIdx = static_cast<int>(i); break; }
  }
  ASSERT_GE(entryIdx, 0);

  std::string in = makeTempFile("race", 4096, 5);
  Client c(cl.addr(entryIdx), "c1");
  // A single attempt only: the client's own retry would otherwise succeed once the failure
  // detector catches up and legitimately re-places the file on a new primary, which is correct
  // self-healing behavior, not what this test is checking. This test is about the first attempt,
  // before the ring has caught up.
  Client::RetryPolicy once;
  once.maxAttempts = 1;
  c.setRetryPolicy(once);

  // Make the primary undialable without actually stopping it — a real kill takes long enough
  // (closing the listener, joining its background threads) that the failure detector has
  // usually already excluded it from placement by the time a racing create runs, which would
  // make this test pass for the wrong reason. Blocking the address directly keeps the node (and
  // therefore everyone's membership view of it) alive while still making the write fail, which
  // is the actual race this guards against: the coordinator's placement says reps[0] is healthy,
  // but the write to it fails anyway.
  fault::blockAddress(r0);
  auto r = c.create(in, "race/f");
  fault::unblockAllAddresses();
  EXPECT_FALSE(r.ok());
  EXPECT_NE(r.status, Status::Degraded)
      << "a failed primary write must never be reported as a degraded success";

  std::string fileId = makeFileId("race/f");
  for (const auto& addr : {r1, r2}) {
    Manifest m;
    EXPECT_FALSE(isOk(repl::fetchManifest(addr, fileId, m)))
        << "secondary " << addr << " must not hold an orphaned base when the primary write failed";
  }
}
