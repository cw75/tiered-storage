#include "lattices/array_lattice.h"

#include <array>

#include "gtest/gtest.h"

#include "lattices/bool_or_lattice.h"

namespace lf = latticeflow;

using BoolArrayLattice = lf::ArrayLattice<lf::BoolOrLattice, 3>;

TEST(BoolArrayLattice, Basics) {
  lf::BoolOrLattice tru(true);
  lf::BoolOrLattice fls(false);
  BoolArrayLattice x(std::array<lf::BoolOrLattice, 3>({tru, tru, fls}));
  BoolArrayLattice y(std::array<lf::BoolOrLattice, 3>({tru, fls, fls}));

  EXPECT_EQ(tru, std::get<0>(x.get()));
  EXPECT_EQ(tru, std::get<1>(x.get()));
  EXPECT_EQ(fls, std::get<2>(x.get()));
  EXPECT_EQ(tru, std::get<0>(y.get()));
  EXPECT_EQ(fls, std::get<1>(y.get()));
  EXPECT_EQ(fls, std::get<2>(y.get()));

  y.join(x);
  EXPECT_EQ(tru, std::get<0>(y.get()));
  EXPECT_EQ(tru, std::get<1>(y.get()));
  EXPECT_EQ(fls, std::get<2>(y.get()));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
