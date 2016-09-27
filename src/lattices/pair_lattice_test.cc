#include "lattices/pair_lattice.h"

#include <utility>

#include "gtest/gtest.h"

#include "lattices/bool_and_lattice.h"
#include "lattices/bool_or_lattice.h"

namespace lf = latticeflow;

using BoolBoolLattice = lf::PairLattice<lf::BoolOrLattice, lf::BoolAndLattice>;

TEST(BoolBoolLattice, Basics) {
  lf::BoolOrLattice x1(true);
  lf::BoolAndLattice y1(false);
  lf::BoolOrLattice x2(false);
  lf::BoolAndLattice y2(true);
  BoolBoolLattice p1(x1, y1);
  BoolBoolLattice p2(x2, y2);

  EXPECT_EQ(true, std::get<0>(p1.get()).get());
  EXPECT_EQ(false, std::get<1>(p1.get()).get());
  EXPECT_EQ(false, std::get<0>(p2.get()).get());
  EXPECT_EQ(true, std::get<1>(p2.get()).get());

  p2.join(p1);
  EXPECT_EQ(true, std::get<0>(p2.get()).get());
  EXPECT_EQ(false, std::get<1>(p2.get()).get());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
