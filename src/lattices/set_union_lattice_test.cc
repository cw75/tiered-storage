#include "lattices/set_union_lattice.h"

#include <unordered_set>

#include "gtest/gtest.h"

namespace lf = latticeflow;

using IntSetLattice = lf::SetUnionLattice<int>;

TEST(IntSetLattice, Basics) {
  IntSetLattice x(std::unordered_set<int>({1, 2, 3}));

  IntSetLattice y;
  y.insert(3);
  y.insert(4);
  y.insert(5);

  EXPECT_EQ(std::unordered_set<int>({1, 2, 3}), x.get());
  EXPECT_EQ(std::unordered_set<int>({3, 4, 5}), y.get());

  x.join(y);
  EXPECT_EQ(std::unordered_set<int>({1, 2, 3, 4, 5}), x.get());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
