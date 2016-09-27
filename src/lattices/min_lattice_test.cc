#include "lattices/min_lattice.h"

#include <string>

#include "gtest/gtest.h"

namespace lf = latticeflow;

using MinIntLattice = lf::MinLattice<int>;
using MinStringLattice = lf::MinLattice<std::string>;

TEST(MinIntLattice, Basics) {
  MinIntLattice x(1);
  MinIntLattice y(42);
  EXPECT_EQ(1, x.get());
  EXPECT_EQ(42, y.get());

  x.join(y);
  y.join(x);
  EXPECT_EQ(1, x.get());
  EXPECT_EQ(1, y.get());
}

TEST(MinIntLattice, Comparison) {
  MinIntLattice x(1);
  MinIntLattice y(42);
  EXPECT_TRUE(y < x);
  EXPECT_TRUE(y <= x);
  EXPECT_FALSE(y > x);
  EXPECT_FALSE(y >= x);
  EXPECT_FALSE(y == x);
  EXPECT_TRUE(y != x);
}

TEST(MinStringLattice, Basics) {
  MinStringLattice x("aaa");
  MinStringLattice y("aaz");
  EXPECT_EQ("aaa", x.get());
  EXPECT_EQ("aaz", y.get());

  x.join(y);
  y.join(x);
  EXPECT_EQ("aaa", x.get());
  EXPECT_EQ("aaa", y.get());
}

TEST(MinStringLattice, Comparison) {
  MinStringLattice x("aaa");
  MinStringLattice y("aaz");
  EXPECT_TRUE(y < x);
  EXPECT_TRUE(y <= x);
  EXPECT_FALSE(y > x);
  EXPECT_FALSE(y >= x);
  EXPECT_FALSE(y == x);
  EXPECT_TRUE(y != x);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
