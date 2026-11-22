#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "aegisfs/storage.hpp"

using namespace aegisfs;
namespace fs = std::filesystem;

class StorageFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = (fs::temp_directory_path() /
            ("aegisfs_store_" + std::to_string(::getpid()) + "_" +
             std::to_string(reinterpret_cast<uintptr_t>(this))))
               .string();
    fs::remove_all(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }
  std::string dir_;
};

TEST_F(StorageFixture, PathValidation) {
  EXPECT_TRUE(StorageEngine::valid_filename("a/b/c.txt"));
  EXPECT_TRUE(StorageEngine::valid_filename("file"));
  EXPECT_FALSE(StorageEngine::valid_filename(""));
  EXPECT_FALSE(StorageEngine::valid_filename("/etc/passwd"));
  EXPECT_FALSE(StorageEngine::valid_filename("../secret"));
  EXPECT_FALSE(StorageEngine::valid_filename("a/../../b"));
  EXPECT_FALSE(StorageEngine::valid_filename(std::string("bad\0name", 8)));
}

TEST_F(StorageFixture, ChecksumRoundTrip) {
  StorageEngine s(dir_);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string file_id = make_file_id("data.bin");
  std::string content(100000, '\0');
  for (size_t i = 0; i < content.size(); ++i) content[i] = static_cast<char>(i * 7 + 3);
  Sha256Digest sha = sha256(content);
  ASSERT_EQ(s.put_segment(file_id, "base.seg", content, sha), Status::Ok);
  std::string back;
  ASSERT_EQ(s.read_segment(file_id, "base.seg", back), Status::Ok);
  EXPECT_EQ(back, content);
}

TEST_F(StorageFixture, RejectsWrongChecksum) {
  StorageEngine s(dir_);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string file_id = make_file_id("x");
  Sha256Digest wrong = sha256("not the content");
  EXPECT_EQ(s.put_segment(file_id, "base.seg", "hello", wrong), Status::ChecksumMismatch);
}

TEST_F(StorageFixture, ManifestCommitAndReconstruct) {
  StorageEngine s(dir_);
  ASSERT_EQ(s.init(), Status::Ok);
  std::string file_id = make_file_id("doc");
  Manifest m;
  m.filename = "doc";
  m.file_id = file_id;
  m.create_op_id = "c0";

  std::string base = "BASE-";
  std::string ap1 = "APPEND1-";
  SegmentRef sb; sb.kind = SegKind::Base; sb.op_id = "c0"; sb.client_id = "c"; sb.client_seq = 0;
  sb.lamport_ts = 1; sb.size = base.size(); sb.sha = sha256(base); sb.disk_name = "base.seg";
  SegmentRef sa; sa.kind = SegKind::Append; sa.op_id = "c1"; sa.client_id = "c"; sa.client_seq = 1;
  sa.lamport_ts = 2; sa.size = ap1.size(); sa.sha = sha256(ap1); sa.disk_name = "a1.seg";

  ASSERT_EQ(s.put_segment(file_id, "base.seg", base, sb.sha), Status::Ok);
  ASSERT_EQ(s.put_segment(file_id, "a1.seg", ap1, sa.sha), Status::Ok);
  m.add_segment(sb);
  m.add_segment(sa);
  ASSERT_EQ(s.commit_manifest(m), Status::Ok);

  std::string out;
  Sha256Digest fsha;
  ASSERT_EQ(s.reconstruct(m, [&](const char* b, size_t n) { out.append(b, n); return true; }, fsha),
            Status::Ok);
  EXPECT_EQ(out, base + ap1);
  EXPECT_EQ(fsha, sha256(base + ap1));
}

TEST_F(StorageFixture, CrashRecoveryDropsOrphanSegment) {
  std::string file_id = make_file_id("f");
  {
    StorageEngine s(dir_);
    ASSERT_EQ(s.init(), Status::Ok);
    // commit a base
    std::string base = "BASE";
    SegmentRef sb; sb.kind = SegKind::Base; sb.op_id = "c0"; sb.size = base.size();
    sb.sha = sha256(base); sb.disk_name = "base.seg";
    ASSERT_EQ(s.put_segment(file_id, "base.seg", base, sb.sha), Status::Ok);
    Manifest m; m.filename = "f"; m.file_id = file_id; m.create_op_id = "c0"; m.add_segment(sb);
    ASSERT_EQ(s.commit_manifest(m), Status::Ok);
    // simulate crash after writing an append segment but BEFORE manifest commit:
    std::string ap = "ORPHAN";
    ASSERT_EQ(s.put_segment(file_id, "orphan.seg", ap, sha256(ap)), Status::Ok);
    // no manifest commit -> "crash"
  }
  // reopen -> recovery should drop the orphan segment
  StorageEngine s2(dir_);
  ASSERT_EQ(s2.init(), Status::Ok);
  Manifest m;
  ASSERT_TRUE(s2.load_manifest(file_id, m));
  EXPECT_EQ(m.segments.size(), 1u);  // only the committed base
  EXPECT_FALSE(fs::exists(dir_ + "/files/" + file_id + "/orphan.seg"));
}
