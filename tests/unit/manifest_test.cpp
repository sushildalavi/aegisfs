#include <gtest/gtest.h>

#include "aegisfs/manifest.hpp"

using namespace aegisfs;

static SegmentRef mkseg(SegKind k, const std::string& op, const std::string& client, uint64_t seq,
                        uint64_t lamport) {
  SegmentRef s;
  s.kind = k;
  s.op_id = op;
  s.client_id = client;
  s.client_seq = seq;
  s.lamport_ts = lamport;
  s.size = 10;
  s.disk_name = op + ".seg";
  s.sha = sha256(op);
  return s;
}

TEST(ManifestTest, SerializeRoundTrip) {
  Manifest m;
  m.filename = "logs/app.log";
  m.file_id = make_file_id(m.filename);
  m.create_op_id = "c#1";
  m.add_segment(mkseg(SegKind::Base, "c#1", "c", 0, 1));
  m.add_segment(mkseg(SegKind::Append, "c#2", "c", 1, 2));

  std::string buf;
  m.serialize(buf);
  auto parsed = Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size());
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->filename, m.filename);
  EXPECT_EQ(parsed->segments.size(), 2u);
  EXPECT_EQ(parsed->segments[1].op_id, "c#2");
}

TEST(ManifestTest, DetectsCorruption) {
  Manifest m;
  m.filename = "f";
  m.file_id = make_file_id("f");
  m.add_segment(mkseg(SegKind::Base, "b", "c", 0, 1));
  std::string buf;
  m.serialize(buf);
  buf[buf.size() / 2] ^= 0xFF;  // flip a byte
  EXPECT_FALSE(Manifest::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size()).has_value());
}

TEST(ManifestTest, Idempotent) {
  Manifest m;
  EXPECT_TRUE(m.add_segment(mkseg(SegKind::Append, "op1", "c", 1, 1)));
  EXPECT_FALSE(m.add_segment(mkseg(SegKind::Append, "op1", "c", 1, 1)));  // duplicate op
  EXPECT_EQ(m.segments.size(), 1u);
  EXPECT_TRUE(m.has_op("op1"));
}

TEST(ManifestTest, PerClientOrder) {
  Manifest m;
  // Insert out of order; canonical order must sort base first, then by (lamport, client, seq).
  m.add_segment(mkseg(SegKind::Append, "c1#2", "c1", 2, 5));
  m.add_segment(mkseg(SegKind::Base, "c1#0", "c1", 0, 1));
  m.add_segment(mkseg(SegKind::Append, "c1#1", "c1", 1, 3));
  auto ord = m.ordered();
  ASSERT_EQ(ord.size(), 3u);
  EXPECT_EQ(ord[0]->kind, SegKind::Base);
  EXPECT_EQ(ord[1]->op_id, "c1#1");
  EXPECT_EQ(ord[2]->op_id, "c1#2");
}

TEST(ManifestTest, CrossClientDeterministicTieBreak) {
  Manifest m;
  m.add_segment(mkseg(SegKind::Append, "cB", "clientB", 1, 10));
  m.add_segment(mkseg(SegKind::Append, "cA", "clientA", 1, 10));  // same lamport
  auto ord = m.ordered();
  // tie broken by client_id: clientA before clientB
  EXPECT_EQ(ord[0]->client_id, "clientA");
  EXPECT_EQ(ord[1]->client_id, "clientB");
}
