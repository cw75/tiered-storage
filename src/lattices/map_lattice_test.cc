#include "lattices/map_lattice.h"

#include <string>

#include "gtest/gtest.h"

#include "lattices/max_lattice.h"

namespace lf = latticeflow;

using MaxIntLattice = lf::MaxLattice<int>;
using StringIntMapLattice = lf::MapLattice<std::string, MaxIntLattice>;

TEST(StringIntMapLattice, Basics) {
  StringIntMapLattice x;
  x.put("a", MaxIntLattice(1));
  x.put("b", MaxIntLattice(2));
  x.put("c", MaxIntLattice(3));

  StringIntMapLattice y;
  y.put("c", MaxIntLattice(30));
  y.put("d", MaxIntLattice(40));
  y.put("e", MaxIntLattice(50));

  EXPECT_EQ(1, x.get("a").get());
  EXPECT_EQ(2, x.get("b").get());
  EXPECT_EQ(3, x.get("c").get());

  EXPECT_EQ(30, y.get("c").get());
  EXPECT_EQ(40, y.get("d").get());
  EXPECT_EQ(50, y.get("e").get());

  x.join(y);
  EXPECT_EQ(1, x.get("a").get());
  EXPECT_EQ(2, x.get("b").get());
  EXPECT_EQ(30, x.get("c").get());
  EXPECT_EQ(40, x.get("d").get());
  EXPECT_EQ(50, x.get("e").get());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
