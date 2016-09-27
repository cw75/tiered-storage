#include "lattices/bool_and_lattice.h"

#include "gtest/gtest.h"

namespace lf = latticeflow;

TEST(BoolAndLattice, Basics) {
  lf::BoolAndLattice x(true);
  lf::BoolAndLattice y(false);
  EXPECT_EQ(true, x.get());
  EXPECT_EQ(false, y.get());

  y.join(x);
  x.join(y);
  EXPECT_EQ(false, x.get());
  EXPECT_EQ(false, y.get());
}

TEST(BoolAndLattice, Comparison) {
  lf::BoolAndLattice tru(true);
  lf::BoolAndLattice fls(false);

  EXPECT_TRUE(tru < fls);
  EXPECT_TRUE(tru <= fls);
  EXPECT_FALSE(tru > fls);
  EXPECT_FALSE(tru >= fls);
  EXPECT_FALSE(tru == fls);
  EXPECT_TRUE(tru != fls);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
