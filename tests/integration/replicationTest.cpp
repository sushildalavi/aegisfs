#include <gtest/gtest.h>

#include <map>
#include <thread>
#include <vector>

#include "aegisfs/replication.hpp"
#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

// Read-your-writes: a client's appends are visible on a subsequent get issued through a
// DIFFERENT entry node, because the coordinator only serves a replica meeting the watermark.
TEST(Replication, ReadYourWrites) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string base = makeTempFile("rywB", 100, 1);
  std::string ap = makeTempFile("rywA", 4096, 2);
  Client c("", "writer");

  c.setEntry(cl.addr(0));
  ASSERT_TRUE(c.create(base, "ryw/f").ok());
  for (int i = 0; i < 3; ++i) {
    // round-robin the entry node across appends to stress coordination
    c.setEntry(cl.addr(i % cl.size()));
    auto r = c.append(ap, "ryw/f");
    ASSERT_TRUE(r.ok() || r.status == Status::Degraded) << statusName(r.status);
  }
  // now read via yet another entry; must include all appended bytes
  c.setEntry(cl.addr(4));
  std::string out = cl.root() + "/rywOut.bin";
  auto g = c.get("ryw/f", out);
  ASSERT_TRUE(g.ok()) << statusName(g.status);
  EXPECT_EQ(fs::file_size(out), 100u + 3u * 4096u);
}

// Idempotency at the replica level: replaying the same opId must not duplicate the segment.
TEST(Replication, IdempotentReplay) {
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  std::string in = makeTempFile("idem", 500);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "idem/f").ok());

  // craft one append segment and push it twice with the SAME opId directly to a replica
  std::string fileId = makeFileId("idem/f");
  auto ls = c.ls("idem/f");
  ASSERT_TRUE(ls.ok());
  std::string replica = ls.info.getOr("r0Addr");

  std::string payload = "SOME-APPEND-BYTES";
  SegmentRef seg;
  seg.kind = SegKind::Append;
  seg.opId = "fixed-op-123";
  seg.clientId = "c1";
  seg.clientSeq = 1;
  seg.lamportTs = 50;
  seg.size = payload.size();
  seg.sha = sha256(payload);
  seg.diskName = "append-fixed.seg";

  auto src = [&](char* b, size_t cap) -> size_t {
    static size_t off = 0;
    size_t n = std::min(cap, payload.size() - off);
    std::memcpy(b, payload.data() + off, n);
    off += n;
    return n;
  };
  ASSERT_EQ(repl::replicateSegment(replica, fileId, "idem/f", "", seg, payload.size(), src, false),
            Status::Ok);
  // second identical push (fresh source) — must be a no-op on the manifest
  size_t off2 = 0;
  auto src2 = [&](char* b, size_t cap) -> size_t {
    size_t n = std::min(cap, payload.size() - off2);
    std::memcpy(b, payload.data() + off2, n);
    off2 += n;
    return n;
  };
  ASSERT_EQ(repl::replicateSegment(replica, fileId, "idem/f", "", seg, payload.size(), src2, false),
            Status::Ok);

  Manifest m;
  ASSERT_EQ(repl::fetchManifest(replica, fileId, m), Status::Ok);
  int count = 0;
  for (const auto& s : m.segments)
    if (s.opId == "fixed-op-123") ++count;
  EXPECT_EQ(count, 1);  // applied at most once
}

// merge in a quiescent healthy cluster leaves all designated replicas byte-identical.
TEST(Replication, MergeMakesReplicasIdentical) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string base = makeTempFile("mrgB", 2048, 3);
  std::string ap = makeTempFile("mrgA", 777, 4);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(base, "mrg/f").ok());
  ASSERT_TRUE(c.append(ap, "mrg/f").ok());

  auto m = c.merge("mrg/f");
  ASSERT_EQ(m.status, Status::Ok) << statusName(m.status);
  EXPECT_EQ(m.info.getI64("replicas"), 3);

  // compare all 3 replicas directly
  auto ls = c.ls("mrg/f");
  std::string ref;
  for (int i = 0; i < ls.info.getI64("replicas"); ++i) {
    std::string addr = ls.info.getOr("r" + std::to_string(i) + "_addr");
    std::string out = cl.root() + "/mrg" + std::to_string(i) + ".bin";
    ASSERT_TRUE(c.getFromReplica(addr, "mrg/f", out).ok());
    std::string sha = fileShaHex(out);
    if (ref.empty()) ref = sha;
    EXPECT_EQ(sha, ref);
  }
}

// Appends from several distinct clients all land, and each client's own appends keep their order.
TEST(Replication, MultiClientAppendsAllPresentOrdered) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string base = makeTempFile("mcB", 100, 1);
  std::string ap = makeTempFile("mcA", 50, 2);
  Client creator = cl.client(0, "creator");
  ASSERT_TRUE(creator.create(base, "mc/f").ok());

  const int CLIENTS = 4, PER = 5;
  std::vector<std::thread> ts;
  for (int i = 0; i < CLIENTS; ++i)
    ts.emplace_back([&, i] {
      Client c(cl.addr(i % cl.size()), "mc" + std::to_string(i));
      for (int j = 0; j < PER; ++j) c.append(ap, "mc/f");
    });
  for (auto& t : ts) t.join();

  Manifest m;
  auto ls = creator.ls("mc/f");
  ASSERT_EQ(repl::fetchManifest(ls.info.getOr("r0Addr"), makeFileId("mc/f"), m), Status::Ok);
  std::map<std::string, std::vector<uint64_t>> seqs;
  int appends = 0;
  for (const auto& s : m.segments)
    if (s.kind == SegKind::Append) { ++appends; seqs[s.clientId].push_back(s.clientSeq); }
  EXPECT_EQ(appends, CLIENTS * PER);
  for (auto& [cid, v] : seqs) {
    // ordered() canonical order must present this client's segments by increasing seq
    std::vector<uint64_t> inOrder;
    for (const SegmentRef* s : m.ordered())
      if (s->clientId == cid && s->kind == SegKind::Append) inOrder.push_back(s->clientSeq);
    for (size_t k = 1; k < inOrder.size(); ++k) EXPECT_LT(inOrder[k - 1], inOrder[k]) << cid;
  }
}

// The client watermark advances as appends are acknowledged (enables read-your-writes).
TEST(Replication, WatermarkAdvances) {
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  std::string base = makeTempFile("wmB", 10, 1);
  std::string ap = makeTempFile("wmA", 10, 2);
  Client c = cl.client(0, "w");
  ASSERT_TRUE(c.create(base, "wm/f").ok());
  EXPECT_EQ(c.watermark("wm/f"), 0u);
  ASSERT_TRUE(c.append(ap, "wm/f").ok());
  EXPECT_EQ(c.watermark("wm/f"), 1u);
  ASSERT_TRUE(c.append(ap, "wm/f").ok());
  EXPECT_EQ(c.watermark("wm/f"), 2u);
}
