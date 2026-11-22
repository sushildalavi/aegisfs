#include <gtest/gtest.h>

#include "aegisfs/membership.hpp"

using namespace aegisfs;

TEST(MembershipTest, SelfPresentAlive) {
  Membership m("self", "127.0.0.1:1000");
  EXPECT_TRUE(m.is_alive("self"));
  EXPECT_EQ(m.alive_count(), 1u);
}

TEST(MembershipTest, MergeLearnsPeers) {
  Membership m("self", "127.0.0.1:1000");
  std::vector<Member> in = {
      {"n2", "127.0.0.1:1001", 1, MemberState::Alive, 0},
      {"n3", "127.0.0.1:1002", 1, MemberState::Alive, 0},
  };
  EXPECT_TRUE(m.merge(in));
  EXPECT_EQ(m.alive_count(), 3u);
}

TEST(MembershipTest, HigherIncarnationWins) {
  Membership m("self", "a:1");
  m.merge({{"n2", "a:2", 1, MemberState::Alive, 0}});
  m.merge({{"n2", "a:2", 2, MemberState::Failed, 0}});
  Member out;
  ASSERT_TRUE(m.lookup("n2", out));
  EXPECT_EQ(out.state, MemberState::Failed);
  // A stale lower-incarnation Alive must NOT resurrect it.
  m.merge({{"n2", "a:2", 1, MemberState::Alive, 0}});
  ASSERT_TRUE(m.lookup("n2", out));
  EXPECT_EQ(out.state, MemberState::Failed);
}

TEST(MembershipTest, IncarnationRefute) {
  Membership m("self", "a:1");
  uint64_t inc0 = m.self_incarnation();
  // A rumor that self is FAILED at our incarnation must be refuted by bumping our incarnation.
  m.merge({{"self", "a:1", inc0, MemberState::Failed, 0}});
  EXPECT_TRUE(m.is_alive("self"));
  EXPECT_GT(m.self_incarnation(), inc0);
}

TEST(MembershipTest, SuspectEscalatesToFailed) {
  Membership m("self", "a:1");
  m.merge({{"n2", "a:2", 1, MemberState::Alive, 0}});
  m.mark_suspect("n2");
  auto failed = m.tick(/*suspect_ms=*/0, /*reap_ms=*/1000000);
  ASSERT_EQ(failed.size(), 1u);
  EXPECT_EQ(failed[0], "n2");
  EXPECT_FALSE(m.is_alive("n2"));
}

TEST(MembershipTest, SerializeRoundTrip) {
  Membership m("self", "a:1");
  m.merge({{"n2", "b:2", 3, MemberState::Suspect, 0}});
  std::string buf = m.serialize();
  std::vector<Member> out;
  ASSERT_TRUE(Membership::parse(buf, out));
  EXPECT_EQ(out.size(), 2u);
}
