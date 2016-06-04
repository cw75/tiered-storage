#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

static void BM_MaxIntIncrementMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		MaxLattice<int> ml;
		for (int i = 0; i < state.range_x(); i++) {
			ml.merge(i);
		}
	}
}

static void BM_MaxIntDecrementMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		MaxLattice<int> ml;
		for (int i = state.range_x(); i > 0; i--) {
			ml.merge(i);
		}
	}
}