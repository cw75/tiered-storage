#include "lattices/set_intersect_lattice.h"

#include <unordered_set>

#include "gtest/gtest.h"

namespace lf = latticeflow;

using IntSetLattice = lf::SetIntersectLattice<int>;

TEST(IntSetLattice, Basics) {
  IntSetLattice x(std::unordered_set<int>({1, 2, 3}));
  IntSetLattice y(std::unordered_set<int>({3, 4, 5}));
  EXPECT_EQ(std::unordered_set<int>({1, 2, 3}), x.get());
  EXPECT_EQ(std::unordered_set<int>({3, 4, 5}), y.get());

  x.join(y);
  EXPECT_EQ(std::unordered_set<int>({3}), x.get());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
