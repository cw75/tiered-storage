#include "lattices/bool_or_lattice.h"

#include "gtest/gtest.h"

namespace lf = latticeflow;

TEST(BoolOrLattice, Basics) {
  lf::BoolOrLattice x(true);
  lf::BoolOrLattice y(false);
  EXPECT_EQ(true, x.get());
  EXPECT_EQ(false, y.get());

  x.join(y);
  y.join(x);
  EXPECT_EQ(true, x.get());
  EXPECT_EQ(true, y.get());
}

TEST(BoolOrLattice, Comparison) {
  lf::BoolOrLattice tru(true);
  lf::BoolOrLattice fls(false);

  EXPECT_TRUE(fls < tru);
  EXPECT_TRUE(fls <= tru);
  EXPECT_FALSE(fls > tru);
  EXPECT_FALSE(fls >= tru);
  EXPECT_FALSE(fls == tru);
  EXPECT_TRUE(fls != tru);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
