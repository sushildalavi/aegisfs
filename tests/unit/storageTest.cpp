#include <gtest/gtest.h>

#include <filesystem>
#include <atomic>
#include <fstream>
#include <thread>
#include <vector>

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
