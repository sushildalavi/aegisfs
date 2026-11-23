#include <gtest/gtest.h>

#include <filesystem>
#include <atomic>
#include <fstream>
#include <thread>
#include <vector>

#include "aegisfs/partition.hpp"
#include "aegisfs/storage.hpp"

using namespace aegisfs;
namespace fs = std::filesystem;

class StorageFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    directory = (fs::temp_directory_path() /
            ("aegisfs_store_" + std::to_string(::getpid()) + "_" +
             std::to_string(reinterpret_cast<uintptr_t>(this))))
               .string();
    fs::remove_all(directory);
  }
  void TearDown() override { fs::remove_all(directory); }
  std::string directory;
};

TEST_F(StorageFixture, PathValidation) {
  EXPECT_TRUE(StorageEngine::validFilename("a/b/c.txt"));
  EXPECT_TRUE(StorageEngine::validFilename("file"));
  EXPECT_FALSE(StorageEngine::validFilename(""));
  EXPECT_FALSE(StorageEngine::validFilename("/etc/passwd"));
  EXPECT_FALSE(StorageEngine::validFilename("../secret"));
  EXPECT_FALSE(StorageEngine::validFilename("a/../../b"));
  EXPECT_FALSE(StorageEngine::validFilename(std::string("bad\0name", 8)));
}

TEST_F(StorageFixture, RejectsUnsafeSegmentComponents) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  Sha256Digest sha = sha256("payload");
  EXPECT_EQ(s.putSegment("../escape", "base.seg", "payload", sha), Status::InvalidArgument);
  EXPECT_EQ(s.putSegment(makeFileId("safe"), "../escape", "payload", sha),
            Status::InvalidArgument);
  Manifest unsafe;
  unsafe.fileId = makeFileId("safe");
  SegmentRef seg;
  seg.opId = "op";
  seg.diskName = "../escape";
  unsafe.segments.push_back(seg);
  EXPECT_EQ(s.commitManifest(unsafe), Status::InvalidArgument);
}

TEST_F(StorageFixture, RejectsOversizedSourceChunk) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  Sha256Digest out;
  std::string payload = "payload";
  EXPECT_EQ(s.putSegmentStream(makeFileId("safe"), "base.seg", 1,
                                  [&](char* b, size_t cap) {
                                    (void)b;
                                    (void)payload;
                                    return cap + 1;
                                  },
                                  out),
            Status::IoError);
}

TEST_F(StorageFixture, ChecksumRoundTrip) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string fileId = makeFileId("data.bin");
  std::string content(100000, '\0');
  for (size_t i = 0; i < content.size(); ++i) content[i] = static_cast<char>(i * 7 + 3);
  Sha256Digest sha = sha256(content);
  ASSERT_EQ(s.putSegment(fileId, "base.seg", content, sha), Status::Ok);
  std::string back;
  ASSERT_EQ(s.readSegment(fileId, "base.seg", back), Status::Ok);
  EXPECT_EQ(back, content);
}

TEST_F(StorageFixture, RejectsWrongChecksum) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string fileId = makeFileId("x");
  Sha256Digest wrong = sha256("not the content");
  EXPECT_EQ(s.putSegment(fileId, "base.seg", "hello", wrong), Status::ChecksumMismatch);
}

TEST_F(StorageFixture, ManifestCommitAndReconstruct) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string fileId = makeFileId("doc");
  Manifest m;
  m.filename = "doc";
  m.fileId = fileId;
  m.createOpId = "c0";

  std::string base = "BASE-";
  std::string ap1 = "APPEND1-";
  SegmentRef sb; sb.kind = SegKind::Base; sb.opId = "c0"; sb.clientId = "c"; sb.clientSeq = 0;
  sb.lamportTs = 1; sb.size = base.size(); sb.sha = sha256(base); sb.diskName = "base.seg";
  SegmentRef sa; sa.kind = SegKind::Append; sa.opId = "c1"; sa.clientId = "c"; sa.clientSeq = 1;
  sa.lamportTs = 2; sa.size = ap1.size(); sa.sha = sha256(ap1); sa.diskName = "a1.seg";

  ASSERT_EQ(s.putSegment(fileId, "base.seg", base, sb.sha), Status::Ok);
  ASSERT_EQ(s.putSegment(fileId, "a1.seg", ap1, sa.sha), Status::Ok);
  m.addSegment(sb);
  m.addSegment(sa);
  ASSERT_EQ(s.commitManifest(m), Status::Ok);

  std::string out;
  Sha256Digest fsha;
  ASSERT_EQ(s.reconstruct(m, [&](const char* b, size_t n) { out.append(b, n); return true; }, fsha),
            Status::Ok);
  EXPECT_EQ(out, base + ap1);
  EXPECT_EQ(fsha, sha256(base + ap1));
}

TEST_F(StorageFixture, ConcurrentPutsDifferentFiles) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  std::vector<std::thread> ts;
  std::atomic<int> ok{0};
  for (int i = 0; i < 16; ++i) {
    ts.emplace_back([&, i] {
      std::string name = "file" + std::to_string(i);
      std::string fid = makeFileId(name);
      std::string content(2000 + i * 37, static_cast<char>('A' + (i % 26)));
      if (isOk(s.putSegment(fid, "base.seg", content, sha256(content)))) ok.fetch_add(1);
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(ok.load(), 16);
  // read each back correctly
  for (int i = 0; i < 16; ++i) {
    std::string fid = makeFileId("file" + std::to_string(i));
    std::string content(2000 + i * 37, static_cast<char>('A' + (i % 26)));
    std::string back;
    ASSERT_EQ(s.readSegment(fid, "base.seg", back), Status::Ok);
    EXPECT_EQ(back, content);
  }
}

TEST_F(StorageFixture, NestedFilenameRoundTrip) {
  StorageEngine s(directory);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string name = "a/deep/nested/path.log";
  ASSERT_TRUE(StorageEngine::validFilename(name));
  std::string fid = makeFileId(name);
  std::string content = "nested-content";
  ASSERT_EQ(s.putSegment(fid, "base.seg", content, sha256(content)), Status::Ok);
  Manifest m; m.filename = name; m.fileId = fid; m.createOpId = "c0";
  SegmentRef sb; sb.kind = SegKind::Base; sb.opId = "c0"; sb.size = content.size();
  sb.sha = sha256(content); sb.diskName = "base.seg";
  m.addSegment(sb);
  ASSERT_EQ(s.commitManifest(m), Status::Ok);
  Manifest loaded;
  ASSERT_TRUE(s.loadManifest(fid, loaded));
  EXPECT_EQ(loaded.filename, name);
}

TEST_F(StorageFixture, CrashRecoveryDropsOrphanSegment) {
  std::string fileId = makeFileId("f");
  {
    StorageEngine s(directory);
    ASSERT_EQ(s.init(), Status::Ok);
    // commit a base
    std::string base = "BASE";
    SegmentRef sb; sb.kind = SegKind::Base; sb.opId = "c0"; sb.size = base.size();
    sb.sha = sha256(base); sb.diskName = "base.seg";
    ASSERT_EQ(s.putSegment(fileId, "base.seg", base, sb.sha), Status::Ok);
    Manifest m; m.filename = "f"; m.fileId = fileId; m.createOpId = "c0"; m.addSegment(sb);
    ASSERT_EQ(s.commitManifest(m), Status::Ok);
    // simulate crash after writing an append segment but BEFORE manifest commit:
    std::string ap = "ORPHAN";
    ASSERT_EQ(s.putSegment(fileId, "orphan.seg", ap, sha256(ap)), Status::Ok);
    // no manifest commit -> "crash"
  }
  // reopen -> recovery should drop the orphan segment
  StorageEngine s2(directory);
  ASSERT_EQ(s2.init(), Status::Ok);
  Manifest m;
  ASSERT_TRUE(s2.loadManifest(fileId, m));
  EXPECT_EQ(m.segments.size(), 1u);  // only the committed base
  EXPECT_FALSE(fs::exists(directory + "/files/" + fileId + "/orphan.seg"));
}

// Crash injection: stop the write path at each durable step, reopen the store the way a restart
// would, and check that the file is either exactly what was acknowledged before or exactly the new
// version, never something in between.
namespace {
struct CrashScenario {
  const char* point;
  bool newVersionVisible;  // true if a crash at this point still leaves the new manifest committed
};

SegmentRef makeSeg(SegKind kind, const std::string& op, uint64_t seq, uint64_t ts,
                   const std::string& data, const std::string& disk) {
  SegmentRef r;
  r.kind = kind; r.opId = op; r.clientId = "c"; r.clientSeq = seq; r.lamportTs = ts;
  r.size = data.size(); r.sha = sha256(data); r.diskName = disk;
  return r;
}
}  // namespace

class CrashPoints : public StorageFixture, public ::testing::WithParamInterface<CrashScenario> {
 protected:
  void TearDown() override {
    fault::setCrashHook(nullptr);
    StorageFixture::TearDown();
  }
};

TEST_P(CrashPoints, AppendIsAllOrNothingAfterRestart) {
  const std::string base = "BASE-DATA";
  const std::string extra = "APPENDED-DATA";
  std::string fileId = makeFileId("doc");
  Manifest v1;
  v1.filename = "doc"; v1.fileId = fileId; v1.createOpId = "c0";
  SegmentRef sb = makeSeg(SegKind::Base, "c0", 0, 1, base, "base.seg");
  SegmentRef sa = makeSeg(SegKind::Append, "c1", 1, 2, extra, "a1.seg");
  {
    StorageEngine s(directory);
    ASSERT_EQ(s.init(), Status::Ok);
    ASSERT_EQ(s.putSegment(fileId, "base.seg", base, sb.sha), Status::Ok);
    v1.addSegment(sb);
    ASSERT_EQ(s.commitManifest(v1), Status::Ok);

    const std::string target = GetParam().point;
    fault::setCrashHook([&](const char* p) { return target == p; });
    Status ps = s.putSegment(fileId, "a1.seg", extra, sa.sha);
    if (ps == Status::Ok) {
      Manifest v2 = v1;
      v2.addSegment(sa);
      EXPECT_NE(s.commitManifest(v2), Status::Ok) << "crash at " << target << " must fail the commit";
    } else {
      EXPECT_NE(ps, Status::Ok);
    }
    fault::setCrashHook(nullptr);
  }  // the engine goes away without any cleanup, like a killed process

  StorageEngine restarted(directory);
  ASSERT_EQ(restarted.init(), Status::Ok);
  Manifest m;
  ASSERT_TRUE(restarted.loadManifest(fileId, m));
  std::string out;
  Sha256Digest fsha;
  ASSERT_EQ(restarted.reconstruct(m, [&](const char* b, size_t n) { out.append(b, n); return true; }, fsha),
            Status::Ok);
  EXPECT_EQ(out, GetParam().newVersionVisible ? base + extra : base);
  EXPECT_EQ(fsha, sha256(out));
  // Nothing unreferenced or half-written survives recovery.
  size_t entries = 0;
  for (auto& e : fs::directory_iterator(fs::path(directory) / "files" / fileId)) { (void)e; ++entries; }
  EXPECT_EQ(entries, m.segments.size() + 1);  // segments plus the manifest
  EXPECT_TRUE(fs::is_empty(fs::path(directory) / "tmp"));
}

INSTANTIATE_TEST_SUITE_P(
    EveryStep, CrashPoints,
    ::testing::Values(CrashScenario{"segment.afterWrite", false},
                      CrashScenario{"segment.afterFsync", false},
                      CrashScenario{"segment.afterRename", false},
                      CrashScenario{"manifest.afterWrite", false},
                      CrashScenario{"manifest.afterFsync", false},
                      CrashScenario{"manifest.afterRename", true}),
    [](const ::testing::TestParamInfo<CrashScenario>& i) {
      std::string n = i.param.point;
      for (auto& ch : n) if (ch == '.') ch = '_';
      return n;
    });

TEST_F(StorageFixture, CrashDuringFirstCreateLeavesNoFile) {
  std::string fileId = makeFileId("fresh");
  const std::string data = "NEW";
  for (const char* point : {"segment.afterWrite", "segment.afterFsync", "segment.afterRename",
                            "manifest.afterWrite", "manifest.afterFsync"}) {
    fs::remove_all(directory);
    {
      StorageEngine s(directory);
      ASSERT_EQ(s.init(), Status::Ok);
      std::string target = point;
      fault::setCrashHook([&](const char* p) { return target == p; });
      Sha256Digest sha = sha256(data);
      Status ps = s.putSegment(fileId, "base.seg", data, sha);
      if (ps == Status::Ok) {
        Manifest m;
        m.filename = "fresh"; m.fileId = fileId; m.createOpId = "c0";
        m.addSegment(makeSeg(SegKind::Base, "c0", 0, 1, data, "base.seg"));
        EXPECT_NE(s.commitManifest(m), Status::Ok);
      }
      fault::setCrashHook(nullptr);
    }
    StorageEngine restarted(directory);
    ASSERT_EQ(restarted.init(), Status::Ok);
    EXPECT_FALSE(restarted.exists(fileId)) << "unacknowledged create survived a crash at " << point;
    EXPECT_FALSE(fs::exists(fs::path(directory) / "files" / fileId)) << point;
  }
}
