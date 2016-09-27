#include "lattices/max_lattice.h"

#include <string>

#include "gtest/gtest.h"

namespace lf = latticeflow;

using MaxIntLattice = lf::MaxLattice<int>;
using MaxStringLattice = lf::MaxLattice<std::string>;

TEST(MaxIntLattice, Basics) {
  MaxIntLattice x(1);
  MaxIntLattice y(42);
  EXPECT_EQ(1, x.get());
  EXPECT_EQ(42, y.get());

  y.join(x);
  x.join(y);
  EXPECT_EQ(42, x.get());
  EXPECT_EQ(42, y.get());
}

TEST(MaxIntLattice, Comparison) {
  MaxIntLattice x(1);
  MaxIntLattice y(42);
  EXPECT_TRUE(x < y);
  EXPECT_TRUE(x <= y);
  EXPECT_FALSE(x > y);
  EXPECT_FALSE(x >= y);
  EXPECT_FALSE(x == y);
  EXPECT_TRUE(x != y);
}

TEST(MaxStringLattice, Basics) {
  MaxStringLattice x("aaa");
  MaxStringLattice y("aaz");
  EXPECT_EQ("aaa", x.get());
  EXPECT_EQ("aaz", y.get());

  y.join(x);
  x.join(y);
  EXPECT_EQ("aaz", x.get());
  EXPECT_EQ("aaz", y.get());
}

TEST(MaxStringLattice, Comparison) {
  MaxStringLattice x("aaa");
  MaxStringLattice y("aaz");
  EXPECT_TRUE(x < y);
  EXPECT_TRUE(x <= y);
  EXPECT_FALSE(x > y);
  EXPECT_FALSE(x >= y);
  EXPECT_FALSE(x == y);
  EXPECT_TRUE(x != y);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
