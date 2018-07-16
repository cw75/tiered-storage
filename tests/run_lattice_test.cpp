#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "test_bool_lattice.hpp"
#include "test_map_lattice.hpp"
#include "test_max_lattice.hpp"
#include "test_set_lattice.hpp"

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
