#include <gtest/gtest.h>

#include "aegisfs/protocol.hpp"

using namespace aegisfs;

TEST(ProtocolTest, FieldsRoundTrip) {
  Fields f;
  f.set("file", "a/b/c.txt");
  f.setI64("status", 7);
  f.setU64("size", 123456789ULL);
  f.set("sha", "deadbeef");

  std::string buf;
  f.serialize(buf);

  Fields g;
  ASSERT_TRUE(Fields::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size(), g));
  EXPECT_EQ(g.getOr("file"), "a/b/c.txt");
  EXPECT_EQ(g.getI64("status"), 7);
  EXPECT_EQ(g.getU64("size"), 123456789ULL);
  EXPECT_EQ(g.getOr("sha"), "deadbeef");
  EXPECT_EQ(g.getOr("missing", "def"), "def");
}

TEST(ProtocolTest, BinarySafeValues) {
  Fields f;
  std::string bin;
  for (int i = 0; i < 256; ++i) bin.push_back(static_cast<char>(i));
  f.set("blob", bin);
  std::string buf;
  f.serialize(buf);
  Fields g;
  ASSERT_TRUE(Fields::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size(), g));
  EXPECT_EQ(g.getOr("blob"), bin);
}

TEST(ProtocolTest, RejectsTruncated) {
  Fields f;
  f.set("k", "value");
  std::string buf;
  f.serialize(buf);
  Fields g;
  // Truncate: parser must fail cleanly, not crash or over-read.
  EXPECT_FALSE(Fields::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size() - 2, g));
}

TEST(ProtocolTest, EndianEncoders) {
  std::string b;
  putU16(b, 0x0102);
  putU32(b, 0x01020304u);
  putU64(b, 0x0102030405060708ULL);
  const uint8_t* p = reinterpret_cast<const uint8_t*>(b.data());
  EXPECT_EQ(getU16(p), 0x0102);
  EXPECT_EQ(getU32(p + 2), 0x01020304u);
  EXPECT_EQ(getU64(p + 6), 0x0102030405060708ULL);
}

TEST(ProtocolTest, FieldsOverwriteKeepsLast) {
  Fields f;
  f.set("k", "first");
  f.set("k", "second");
  std::string buf; f.serialize(buf);
  Fields g;
  ASSERT_TRUE(Fields::parse(reinterpret_cast<const uint8_t*>(buf.data()), buf.size(), g));
  EXPECT_EQ(g.getOr("k"), "second");
}
