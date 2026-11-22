#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <thread>

#include "aegisfs/replication.hpp"
#include "support/cluster_fixture.hpp"

using namespace aegisfs;
using namespace aegisfs::test;

// Many clients append concurrently to one file. Each client serializes its own appends.
// Expect: exactly N*M append segments, unique op ids, and each client's seqs are 1..M.
TEST(Concurrency, ConcurrentAppendsNoDuplicateOrTornManifest) {
  const int N = 6;   // clients
  const int M = 8;   // appends per client
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string base = make_temp_file("cc_base", 64, 1);
  std::string ap = make_temp_file("cc_ap", 128, 2);
  Client creator = cl.client(0, "creator");
  ASSERT_TRUE(creator.create(base, "cc/f").ok());

  std::atomic<int> failures{0};
  std::vector<std::thread> ts;
  for (int i = 0; i < N; ++i) {
    ts.emplace_back([&, i] {
      Client c(cl.addr(i % cl.size()), "client" + std::to_string(i));
      for (int j = 0; j < M; ++j) {
        auto r = c.append(ap, "cc/f");
        if (!r.ok() && r.status != Status::Degraded) ++failures;
      }
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(failures.load(), 0);

  // inspect the primary replica's manifest
  std::string file_id = make_file_id("cc/f");
  auto ls = creator.ls("cc/f");
  ASSERT_TRUE(ls.ok());
  Manifest m;
  ASSERT_EQ(repl::fetch_manifest(ls.info.get_or("r0_addr"), file_id, m), Status::Ok);

  std::set<std::string> op_ids;
  std::map<std::string, std::vector<uint64_t>> per_client_seqs;
  int appends = 0;
  for (const auto& s : m.segments) {
    EXPECT_TRUE(op_ids.insert(s.op_id).second) << "duplicate op " << s.op_id;
    if (s.kind == SegKind::Append) {
      ++appends;
      per_client_seqs[s.client_id].push_back(s.client_seq);
    }
  }
  EXPECT_EQ(appends, N * M);
  // per-client sequence numbers must be exactly 1..M (ordered after sort), no gaps/dups
  for (auto& [cid, seqs] : per_client_seqs) {
    std::sort(seqs.begin(), seqs.end());
    ASSERT_EQ(seqs.size(), static_cast<size_t>(M)) << cid;
    for (int j = 0; j < M; ++j) EXPECT_EQ(seqs[j], static_cast<uint64_t>(j + 1)) << cid;
  }
}

// N clients create the SAME name concurrently via different entry nodes: exactly one wins.
TEST(Concurrency, SimultaneousCreateExactlyOneWins) {
  const int N = 8;
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("sc", 1000);

  std::atomic<int> ok{0}, already{0}, other{0};
  std::vector<std::thread> ts;
  for (int i = 0; i < N; ++i) {
    ts.emplace_back([&, i] {
      Client c(cl.addr(i % cl.size()), "c" + std::to_string(i));
      auto r = c.create(in, "race/one");
      if (r.status == Status::Ok) ++ok;
      else if (r.status == Status::AlreadyExists) ++already;
      else ++other;
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(ok.load(), 1) << "exactly one create should win";
  EXPECT_EQ(ok.load() + already.load() + other.load(), N);
  EXPECT_EQ(other.load(), 0);
}

TEST(Concurrency, ConcurrentDifferentFilesAllSucceed) {
  const int N = 10;
  Cluster cl(5);
  ASSERT_TRUE(cl.await_convergence(5));
  std::string in = make_temp_file("cdf", 5000);
  std::atomic<int> ok{0};
  std::vector<std::thread> ts;
  for (int i = 0; i < N; ++i) {
    ts.emplace_back([&, i] {
      Client c(cl.addr(i % cl.size()), "c" + std::to_string(i));
      if (c.create(in, "multi/f" + std::to_string(i)).ok()) ++ok;
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(ok.load(), N);
}
