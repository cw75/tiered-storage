#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

static void BM_BoolMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		BoolLattice bl;
		for (int i = 0; i < state.range_x(); i++) {
			bl.merge(true);
		}
	}
}