#include <gtest/gtest.h>

#include <fstream>
#include <set>

#include "support/clusterFixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

namespace {
std::string slurp(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
}
void spit(const std::string& path, const std::string& data) {
  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  f << data;
}
// How many nodes hold at least one stripe chunk of `name`.
size_t nodesHoldingChunks(Cluster& cl, const std::string& name) {
  size_t holders = 0;
  for (size_t i = 0; i < cl.addrs().size(); ++i) {
    Client c = cl.client(i, "probe");
    OpResult r = c.listStore();
    int count = static_cast<int>(r.info.getI64("count"));
    for (int k = 0; k < count; ++k) {
      if (r.info.getOr("f" + std::to_string(k) + "_name").rfind(name + ".stripe.", 0) == 0) {
        ++holders;
        break;
      }
    }
  }
  return holders;
}
}  // namespace

TEST(Striping, LargeFileIsSplitSpreadAndReadBackIdentical) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("stripe", 800000, 7);
  Client writer = cl.client(0, "w");
  writer.setStripeSize(100000);
  OpResult created = writer.create(in, "big/file");
  ASSERT_TRUE(created.ok()) << statusName(created.status);
  EXPECT_EQ(created.info.getU64("stripes"), 8u);

  // Eight chunks are placed independently, so together they use more than one replica set.
  EXPECT_GT(nodesHoldingChunks(cl, "big/file"), 3u);

  // Any client on any entry node reassembles the exact bytes, whatever its own stripe setting.
  Client reader = cl.client(3, "r");
  std::string out = in + ".out";
  OpResult got = reader.get("big/file", out);
  ASSERT_TRUE(got.ok()) << statusName(got.status);
  EXPECT_EQ(fileShaHex(in), fileShaHex(out));
  EXPECT_EQ(got.info.getOr("sha256"), fileShaHex(in));
}

TEST(Striping, SmallFilesAreStoredWholeAndUnchanged) {
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  std::string in = makeTempFile("small", 5000, 3);
  Client c = cl.client(0, "c");
  c.setStripeSize(100000);
  ASSERT_TRUE(c.create(in, "plain/file").ok());
  EXPECT_EQ(nodesHoldingChunks(cl, "plain/file"), 0u);
  std::string out = in + ".out";
  ASSERT_TRUE(c.get("plain/file", out).ok());
  EXPECT_EQ(slurp(in), slurp(out));
}

TEST(Striping, AppendsToAStripedFileKeepTheirOrder) {
  Cluster cl(5);
  ASSERT_TRUE(cl.awaitConvergence(5));
  std::string in = makeTempFile("sa", 250000, 11);
  Client c = cl.client(1, "c");
  c.setStripeSize(100000);
  ASSERT_TRUE(c.create(in, "log/striped").ok());

  std::string small = in + ".small";
  std::string large = in + ".large";
  spit(small, "tail-one");                     // smaller than a stripe, still becomes a chunk
  spit(large, std::string(180000, 'L'));       // larger than a stripe, becomes two
  ASSERT_TRUE(c.append(small, "log/striped").ok());
  ASSERT_TRUE(c.append(large, "log/striped").ok());

  std::string out = in + ".out";
  ASSERT_TRUE(c.get("log/striped", out).ok());
  EXPECT_EQ(slurp(out), slurp(in) + "tail-one" + std::string(180000, 'L'));

  // A different client, with striping off, still gets the whole file back.
  Client other = cl.client(4, "o");
  other.setStripeSize(0);
  std::string out2 = in + ".out2";
  ASSERT_TRUE(other.get("log/striped", out2).ok());
  EXPECT_EQ(slurp(out), slurp(out2));
}

TEST(Striping, OnlyTheFirstCreateOfAStripedNameWins) {
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  std::string in = makeTempFile("dup", 300000, 5);
  Client a = cl.client(0, "a");
  Client b = cl.client(1, "b");
  a.setStripeSize(100000);
  b.setStripeSize(100000);
  ASSERT_TRUE(a.create(in, "dup/name").ok());
  EXPECT_EQ(b.create(in, "dup/name").status, Status::AlreadyExists);
}

TEST(Striping, AppendingToAPlainFileIsUnaffected) {
  Cluster cl(3);
  ASSERT_TRUE(cl.awaitConvergence(3));
  std::string in = makeTempFile("pa", 4000, 2);
  Client c = cl.client(0, "c");
  c.setStripeSize(100000);
  ASSERT_TRUE(c.create(in, "plain/log").ok());
  std::string more = in + ".more";
  spit(more, "more");
  ASSERT_TRUE(c.append(more, "plain/log").ok());
  std::string out = in + ".out";
  ASSERT_TRUE(c.get("plain/log", out).ok());
  EXPECT_EQ(slurp(out), slurp(in) + "more");
}
