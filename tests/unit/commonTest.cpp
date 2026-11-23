#include <gtest/gtest.h>

#include <string>

#include "aegisfs/common.hpp"

using namespace aegisfs;

TEST(CommonTest, StatusNamesStable) {
  EXPECT_EQ(statusName(Status::Ok), "OK");
  EXPECT_EQ(statusName(Status::AlreadyExists), "ALREADY_EXISTS");
  EXPECT_EQ(statusName(Status::NotFound), "NOT_FOUND");
  EXPECT_EQ(statusName(Status::Degraded), "DEGRADED");
  EXPECT_EQ(statusName(Status::Stale), "STALE");
  EXPECT_EQ(statusName(Status::ChecksumMismatch), "CHECKSUM_MISMATCH");
  EXPECT_EQ(statusName(Status::Conflict), "CONFLICT");
}

TEST(CommonTest, IsOk) {
  EXPECT_TRUE(isOk(Status::Ok));
  EXPECT_FALSE(isOk(Status::NotFound));
}

TEST(CommonTest, ResultHelpers) {
  auto good = Result<int>::success(42);
  EXPECT_TRUE(good.ok());
  EXPECT_EQ(good.value, 42);
  auto bad = Result<int>::fail(Status::Timeout);
  EXPECT_FALSE(bad.ok());
  EXPECT_EQ(bad.status, Status::Timeout);
}

TEST(CommonTest, StatusCodesAreStableWireValues) {
  // These are serialized across RPCs; regressions would break compatibility.
  EXPECT_EQ(static_cast<int>(Status::Ok), 0);
  EXPECT_EQ(static_cast<int>(Status::AlreadyExists), 1);
  EXPECT_EQ(static_cast<int>(Status::NotFound), 2);
  EXPECT_EQ(static_cast<int>(Status::Degraded), 3);
}
