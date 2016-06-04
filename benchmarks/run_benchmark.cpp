#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h> 
#include "benchmark_BoolLattice.h"
#include "benchmark_MaxLattice.h"
#include "benchmark_MinLattice.h"
#include "benchmark_SetLattice.h"
#include "benchmark_AtomicMaxLattice.h"
#include "benchmark_AtomicSetLattice.h"

BENCHMARK(BM_BoolMerge)->Arg(pow(10, 6));
BENCHMARK(BM_MaxIntIncrementMerge)->Arg(pow(10, 6));
BENCHMARK(BM_MaxIntDecrementMerge)->Arg(pow(10, 6));
BENCHMARK(BM_MinIntIncrementMerge)->Arg(pow(10, 6));
BENCHMARK(BM_MinIntDecrementMerge)->Arg(pow(10, 6));
BENCHMARK(BM_SetInsertionNoDuplicate)->Arg(pow(10, 5));
BENCHMARK(BM_SetInsertionAllDuplicate)->Arg(pow(10, 5));
BENCHMARK(BM_SetInsertionRandom)->Arg(pow(10, 5));
BENCHMARK(BM_AtomicMaxMerge)->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_AtomicMaxMergeComparison)->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_AtomicSetInsert)->ThreadRange(1, 8)->UseRealTime();
BENCHMARK(BM_AtomicSetInsertComparison)->ThreadRange(1, 8)->UseRealTime();

BENCHMARK_MAIN();