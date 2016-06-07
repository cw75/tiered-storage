#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h> 
#include "benchmark_KVS.h"

BENCHMARK(BM_KVSGET)->Arg(pow(10, 5));
BENCHMARK(BM_CKVSGET)->Arg(pow(10, 5));
BENCHMARK(BM_KVSGETComparison)->Arg(pow(10, 5));
BENCHMARK(BM_KVSPUT)->Arg(pow(10, 5));
BENCHMARK(BM_CKVSPUT)->Arg(pow(10, 5));
BENCHMARK(BM_KVSPUTComparison)->Arg(pow(10, 5));

BENCHMARK_MAIN();