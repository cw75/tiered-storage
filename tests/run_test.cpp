#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "test_AtomicMaxLattice.h"
#include "test_BoolLattice.h"
#include "test_MapLattice.h"
#include "test_MaxLattice.h"
#include "test_MinLattice.h"
#include "test_SetLattice.h"
#include "test_TombstoneLattice.h"

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
