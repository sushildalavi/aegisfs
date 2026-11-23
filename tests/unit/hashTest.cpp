#include <gtest/gtest.h>

#include "aegisfs/hash.hpp"

using namespace aegisfs;

TEST(HashTest, Sha256KnownVector) {
  // SHA-256("abc")
  auto d = sha256("abc");
  EXPECT_EQ(toHex(d),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

TEST(HashTest, EmptyInput) {
  auto d = sha256("");
  EXPECT_EQ(toHex(d),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
}

TEST(HashTest, HexRoundTrip) {
  auto d = sha256("the quick brown fox");
  Sha256Digest parsed;
  ASSERT_TRUE(fromHex(toHex(d), parsed));
  EXPECT_EQ(d, parsed);
  EXPECT_FALSE(fromHex("zzzz", parsed));
  EXPECT_FALSE(fromHex("abcd", parsed));  // wrong length
}

TEST(HashTest, RingIdDeterministic) {
  // Stable across calls / processes — never std::hash.
  EXPECT_EQ(ringId("node-A"), ringId("node-A"));
  EXPECT_NE(ringId("node-A"), ringId("node-B"));
}

TEST(HashTest, RingIdIsTop64BitsOfSha) {
  auto d = sha256("file.txt");
  uint64_t expect = 0;
  for (int i = 0; i < 8; ++i) expect = (expect << 8) | d[i];
  EXPECT_EQ(ringId("file.txt"), expect);
}

TEST(HashTest, StreamingMatchesOneShot) {
  std::string data(1 << 20, 'x');  // 1 MiB
  for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 31 + 7);
  Sha256Stream s;
  s.update(data.data(), 1000);
  s.update(data.data() + 1000, data.size() - 1000);
  EXPECT_EQ(s.finalize(), sha256(data));
}
