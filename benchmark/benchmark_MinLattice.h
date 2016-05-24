#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

static void BM_MinIntIncrementMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		MinLattice<int> ml;
		for (int i = 0; i < state.range_x(); i++) {
			ml.merge(i);
		}
	}
}

static void BM_MinIntDecrementMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		MinLattice<int> ml;
		for (int i = state.range_x(); i > 0; i--) {
			ml.merge(i);
		}
	}
}