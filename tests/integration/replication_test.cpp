#include <gtest/gtest.h>

#include "aegisfs/replication.hpp"
#include "support/cluster_fixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

// Read-your-writes: a client's appends are visible on a subsequent get issued through a
// DIFFERENT entry node, because the coordinator only serves a replica meeting the watermark.
TEST(Replication, ReadYourWrites) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string base = make_temp_file("ryw_b", 100, 1);
  std::string ap = make_temp_file("ryw_a", 4096, 2);
  Client c("", "writer");

  c.set_entry(cl.addr(0));
  ASSERT_TRUE(c.create(base, "ryw/f").ok());
  for (int i = 0; i < 3; ++i) {
    // round-robin the entry node across appends to stress coordination
    c.set_entry(cl.addr(i % cl.size()));
    auto r = c.append(ap, "ryw/f");
    ASSERT_TRUE(r.ok() || r.status == Status::Degraded) << status_name(r.status);
  }
  // now read via yet another entry; must include all appended bytes
  c.set_entry(cl.addr(4));
  std::string out = cl.root() + "/ryw_out.bin";
  auto g = c.get("ryw/f", out);
  ASSERT_TRUE(g.ok()) << status_name(g.status);
  EXPECT_EQ(fs::file_size(out), 100u + 3u * 4096u);
}

// Idempotency at the replica level: replaying the same op_id must not duplicate the segment.
TEST(Replication, IdempotentReplay) {
  Cluster cl(3);
  ASSERT_TRUE(cl.await_convergence(3));
  std::string in = make_temp_file("idem", 500);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "idem/f").ok());

  // craft one append segment and push it twice with the SAME op_id directly to a replica
  std::string file_id = make_file_id("idem/f");
  auto ls = c.ls("idem/f");
  ASSERT_TRUE(ls.ok());
  std::string replica = ls.info.get_or("r0_addr");

  std::string payload = "SOME-APPEND-BYTES";
  SegmentRef seg;
  seg.kind = SegKind::Append;
  seg.op_id = "fixed-op-123";
  seg.client_id = "c1";
  seg.client_seq = 1;
  seg.lamport_ts = 50;
  seg.size = payload.size();
  seg.sha = sha256(payload);
  seg.disk_name = "append-fixed.seg";

  auto src = [&](char* b, size_t cap) -> size_t {
    static size_t off = 0;
    size_t n = std::min(cap, payload.size() - off);
    std::memcpy(b, payload.data() + off, n);
    off += n;
    return n;
  };
  ASSERT_EQ(repl::replicate_segment(replica, file_id, "idem/f", "", seg, payload.size(), src, false),
            Status::Ok);
  // second identical push (fresh source) — must be a no-op on the manifest
  size_t off2 = 0;
  auto src2 = [&](char* b, size_t cap) -> size_t {
    size_t n = std::min(cap, payload.size() - off2);
    std::memcpy(b, payload.data() + off2, n);
    off2 += n;
    return n;
  };
  ASSERT_EQ(repl::replicate_segment(replica, file_id, "idem/f", "", seg, payload.size(), src2, false),
            Status::Ok);

  Manifest m;
  ASSERT_EQ(repl::fetch_manifest(replica, file_id, m), Status::Ok);
  int count = 0;
  for (const auto& s : m.segments)
    if (s.op_id == "fixed-op-123") ++count;
  EXPECT_EQ(count, 1);  // applied at most once
}

// merge in a quiescent healthy cluster leaves all designated replicas byte-identical.
TEST(Replication, MergeMakesReplicasIdentical) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string base = make_temp_file("mrg_b", 2048, 3);
  std::string ap = make_temp_file("mrg_a", 777, 4);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(base, "mrg/f").ok());
  ASSERT_TRUE(c.append(ap, "mrg/f").ok());

  auto m = c.merge("mrg/f");
  ASSERT_EQ(m.status, Status::Ok) << status_name(m.status);
  EXPECT_EQ(m.info.get_i64("replicas"), 3);

  // compare all 3 replicas directly
  auto ls = c.ls("mrg/f");
  std::string ref;
  for (int i = 0; i < ls.info.get_i64("replicas"); ++i) {
    std::string addr = ls.info.get_or("r" + std::to_string(i) + "_addr");
    std::string out = cl.root() + "/mrg" + std::to_string(i) + ".bin";
    ASSERT_TRUE(c.get_from_replica(addr, "mrg/f", out).ok());
    std::string sha = file_sha_hex(out);
    if (ref.empty()) ref = sha;
    EXPECT_EQ(sha, ref);
  }
}
