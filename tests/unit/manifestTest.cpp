#include <gtest/gtest.h>

#include <cstring>

#include "aegisfs/manifest.hpp"

using namespace aegisfs;

static SegmentRef mkseg(SegKind k, const std::string& op, const std::string& client, uint64_t seq,
                        uint64_t lamport) {
  SegmentRef s;
  s.kind = k;
  s.opId = op;
  s.clientId = client;
  s.clientSeq = seq;
  s.lamportTs = lamport;
  s.size = 10;
  s.diskName = op + ".seg";
  s.sha = sha256(op);
  return s;
}

TEST(ManifestTest, SerializeRoundTrip) {
  Manifest m;
  m.filename = "logs/app.log";
  m.fileId = makeFileId(m.filename);
  m.createOpId = "c#1";
  m.addSegment(mkseg(SegKind::Base, "c#1", "c", 0, 1));
  m.addSegment(mkseg(SegKind::Append, "c#2", "c", 1, 2));

  std::string buf;
  m.serialize(buf);
  auto parsed = Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->filename, m.filename);
  EXPECT_EQ(parsed->segments.size(), 2u);
  EXPECT_EQ(parsed->segments[1].opId, "c#2");
}

TEST(ManifestTest, DetectsCorruption) {
  Manifest m;
  m.filename = "f";
  m.fileId = makeFileId("f");
  m.addSegment(mkseg(SegKind::Base, "b", "c", 0, 1));
  std::string buf;
  m.serialize(buf);
  buf[buf.size() / 2] ^= 0xFF;  // flip a byte
  EXPECT_FALSE(Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size()).has_value());
}

TEST(ManifestTest, Idempotent) {
  Manifest m;
  EXPECT_TRUE(m.addSegment(mkseg(SegKind::Append, "op1", "c", 1, 1)));
  EXPECT_FALSE(m.addSegment(mkseg(SegKind::Append, "op1", "c", 1, 1)));  // duplicate op
  EXPECT_EQ(m.segments.size(), 1u);
  EXPECT_TRUE(m.hasOp("op1"));
}

TEST(ManifestTest, PerClientOrder) {
  Manifest m;
  // Insert out of order; canonical order must sort base first, then by (lamport, client, seq).
  m.addSegment(mkseg(SegKind::Append, "c1#2", "c1", 2, 5));
  m.addSegment(mkseg(SegKind::Base, "c1#0", "c1", 0, 1));
  m.addSegment(mkseg(SegKind::Append, "c1#1", "c1", 1, 3));
  auto ord = m.ordered();
  ASSERT_EQ(ord.size(), 3u);
  EXPECT_EQ(ord[0]->kind, SegKind::Base);
  EXPECT_EQ(ord[1]->opId, "c1#1");
  EXPECT_EQ(ord[2]->opId, "c1#2");
}

TEST(ManifestTest, CrossClientDeterministicTieBreak) {
  Manifest m;
  m.addSegment(mkseg(SegKind::Append, "cB", "clientB", 1, 10));
  m.addSegment(mkseg(SegKind::Append, "cA", "clientA", 1, 10));  // same lamport
  auto ord = m.ordered();
  // tie broken by clientId: clientA before clientB
  EXPECT_EQ(ord[0]->clientId, "clientA");
  EXPECT_EQ(ord[1]->clientId, "clientB");
}

TEST(ManifestTest, EqualOrderingTupleUsesOperationIdTieBreak) {
  Manifest m;
  m.addSegment(mkseg(SegKind::Append, "op-b", "client", 1, 10));
  m.addSegment(mkseg(SegKind::Append, "op-a", "client", 1, 10));
  auto ord = m.ordered();
  ASSERT_EQ(ord.size(), 2u);
  EXPECT_EQ(ord[0]->opId, "op-a");
  EXPECT_EQ(ord[1]->opId, "op-b");
}

TEST(ManifestTest, RejectsTrailingBytesAndInvalidKind) {
  Manifest m;
  m.addSegment(mkseg(SegKind::Base, "base", "client", 0, 1));
  std::string buf;
  m.serialize(buf);
  buf.push_back('x');
  EXPECT_FALSE(Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size()));

  m.serialize(buf);
  // The kind byte is after the three strings, version, and segment count.
  size_t pos = 8;
  auto skip = [&](size_t n) { pos += 4 + n; };
  skip(m.filename.size());
  skip(m.fileId.size());
  skip(m.createOpId.size());
  pos += 8 + 4;
  buf[pos] = static_cast<char>(99);
  auto bodyLen = static_cast<uint32_t>(buf.size() - 8 - 32);
  auto checksum = sha256(buf.data() + 8, bodyLen);
  std::memcpy(buf.data() + 8 + bodyLen, checksum.data(), checksum.size());
  // The checksum is repaired so this specifically exercises kind validation.
  EXPECT_FALSE(Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size()));
}

TEST(ManifestTest, FileIdIs16Hex) {
  std::string id = makeFileId("some/file.txt");
  EXPECT_EQ(id.size(), 16u);
  for (char c : id) EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  EXPECT_EQ(makeFileId("x"), makeFileId("x"));            // deterministic
  EXPECT_NE(makeFileId("x"), makeFileId("y"));
}

TEST(ManifestTest, TotalSizeSumsSegments) {
  Manifest m;
  auto s1 = mkseg(SegKind::Base, "b", "c", 0, 1); s1.size = 100;
  auto s2 = mkseg(SegKind::Append, "a", "c", 1, 2); s2.size = 55;
  m.addSegment(s1);
  m.addSegment(s2);
  EXPECT_EQ(m.totalSize(), 155u);
}

TEST(ManifestTest, OrderStableAcrossSerialize) {
  Manifest m;
  m.addSegment(mkseg(SegKind::Append, "c1#2", "c1", 2, 5));
  m.addSegment(mkseg(SegKind::Base, "c1#0", "c1", 0, 1));
  m.addSegment(mkseg(SegKind::Append, "c1#1", "c1", 1, 3));
  std::string buf; m.serialize(buf);
  auto p = Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
  ASSERT_TRUE(p.has_value());
  auto a = m.ordered(), b = p->ordered();
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) EXPECT_EQ(a[i]->opId, b[i]->opId);
}
