#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "support/cluster_fixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

TEST(Integration, ThreeNodeStartupConverges) {
  Cluster cl(3);
  EXPECT_TRUE(cl.await_convergence(3));
}

TEST(Integration, FiveNodeStartupConverges) {
  Cluster cl(5);
  EXPECT_TRUE(cl.await_convergence(5));
}

TEST(Integration, TenNodeStartupConverges) {
  Cluster cl(10);
  EXPECT_TRUE(cl.await_convergence(10, 10000));
}

TEST(Integration, CreateGetByteIdentical) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("cg", 250000, 7);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "x/data.bin").ok());
  std::string out = cl.root() + "/out_cg.bin";
  // read from a different entry node
  Client c2 = cl.client(3, "c1");
  auto r = c2.get("x/data.bin", out);
  ASSERT_TRUE(r.ok());
  EXPECT_EQ(file_sha_hex(in), file_sha_hex(out));
}

TEST(Integration, EmptyFile) {
  Cluster cl(3);
  ASSERT_TRUE(cl.await_convergence(3));
  std::string in = make_temp_file("empty", 0);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "e/empty").ok());
  std::string out = cl.root() + "/out_empty.bin";
  ASSERT_TRUE(c.get("e/empty", out).ok());
  EXPECT_EQ(file_sha_hex(in), file_sha_hex(out));
}

TEST(Integration, LargeFileStreaming) {
  // 64 MiB exercises the 1 MiB chunked streaming path (no full-file buffering in storage layer).
  Cluster cl(4);
  ASSERT_TRUE(cl.await_convergence(4));
  std::string in = make_temp_file("big", 64u * 1024 * 1024, 99);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "big/blob").ok());
  std::string out = cl.root() + "/out_big.bin";
  ASSERT_TRUE(c.get("big/blob", out).ok());
  EXPECT_EQ(file_sha_hex(in), file_sha_hex(out));
}

TEST(Integration, FirstCreateOnly) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("fc", 1000);
  Client a = cl.client(0, "ca");
  Client b = cl.client(2, "cb");
  ASSERT_TRUE(a.create(in, "only/one").ok());
  auto dup = b.create(in, "only/one");
  EXPECT_EQ(dup.status, Status::AlreadyExists);
}

TEST(Integration, AppendMissingFileErrors) {
  Cluster cl(3);
  ASSERT_TRUE(cl.await_convergence(3));
  std::string in = make_temp_file("am", 100);
  Client c = cl.client(0, "c1");
  auto r = c.append(in, "no/such/file");
  EXPECT_EQ(r.status, Status::NotFound);
}

TEST(Integration, RepeatedAppendAccumulates) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string base = make_temp_file("ra_base", 500, 1);
  std::string ap = make_temp_file("ra_ap", 300, 2);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(base, "acc/f").ok());
  for (int i = 0; i < 4; ++i) {
    auto r = c.append(ap, "acc/f");
    ASSERT_TRUE(r.ok() || r.status == Status::Degraded) << status_name(r.status);
  }
  std::string out = cl.root() + "/out_acc.bin";
  ASSERT_TRUE(c.get("acc/f", out).ok());
  EXPECT_EQ(fs::file_size(out), 500u + 4u * 300u);
}

TEST(Integration, GetFromReplicaPurityAndAgreement) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("gfr", 20000);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "pure/f").ok());
  auto ls = c.ls("pure/f");
  ASSERT_TRUE(ls.ok());
  // every designated replica returns byte-identical content with no repair side effects
  std::string ref_sha;
  for (int i = 0; i < ls.info.get_i64("replicas"); ++i) {
    std::string addr = ls.info.get_or("r" + std::to_string(i) + "_addr");
    std::string out = cl.root() + "/gfr" + std::to_string(i) + ".bin";
    auto r = c.get_from_replica(addr, "pure/f", out);
    ASSERT_TRUE(r.ok()) << addr;
    std::string sha = file_sha_hex(out);
    if (ref_sha.empty()) ref_sha = sha;
    EXPECT_EQ(sha, ref_sha);
  }
  EXPECT_EQ(ref_sha, file_sha_hex(in));
}

TEST(Integration, MembershipPropagatesToAllNodes) {
  Cluster cl(6);
  ASSERT_TRUE(cl.await_convergence(6, 8000));
  // every node independently sees all 6 members alive
  for (size_t i = 0; i < cl.size(); ++i)
    EXPECT_EQ(cl.node(i)->membership()->alive_count(), 6u) << "node " << i;
}

TEST(Integration, LsPresenceReflectsFailure) {
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("lsp", 4000);
  Client c = cl.client(0, "c1");
  ASSERT_TRUE(c.create(in, "lsp/f").ok());
  auto ls = c.ls("lsp/f");
  ASSERT_TRUE(ls.ok());
  EXPECT_EQ(ls.info.get_i64("present_count"), 3);

  // Kill one replica. After failure detection + re-replication, ls should again report 3 present
  // replicas (all on live nodes) — proving both the presence probe and healing work end to end.
  std::string addr = ls.info.get_or("r0_addr");
  int idx = cl.index_of_addr(addr);
  ASSERT_GE(idx, 0);
  cl.stop_node(idx);
  int entry = (idx + 1) % static_cast<int>(cl.size());
  Client c2(cl.addr(entry), "c2");
  bool healed = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  while (std::chrono::steady_clock::now() < deadline) {
    auto l = c2.ls("lsp/f");
    if (l.ok() && l.info.get_i64("present_count") == 3) {
      // and none of the present replicas is the dead node
      bool dead_listed = false;
      for (int i = 0; i < 3; ++i)
        if (cl.index_of_addr(l.info.get_or("r" + std::to_string(i) + "_addr")) == idx)
          dead_listed = true;
      if (!dead_listed) { healed = true; break; }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  EXPECT_TRUE(healed) << "present_count should return to 3 on live replicas after healing";
}

TEST(Integration, GetNonexistentReturnsNotFound) {
  Cluster cl(3);
  ASSERT_TRUE(cl.await_convergence(3));
  Client c = cl.client(0, "c1");
  std::string out = cl.root() + "/nope.bin";
  auto r = c.get("does/not/exist", out);
  EXPECT_EQ(r.status, Status::NotFound);
}

TEST(Integration, InvalidFilenameRejected) {
  Cluster cl(3);
  ASSERT_TRUE(cl.await_convergence(3));
  std::string in = make_temp_file("bad", 10);
  Client c = cl.client(0, "c1");
  EXPECT_EQ(c.create(in, "../escape").status, Status::InvalidArgument);
  EXPECT_EQ(c.create(in, "/abs/path").status, Status::InvalidArgument);
}
