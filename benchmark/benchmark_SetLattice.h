#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

static void BM_SetInsertionNoDuplicate(benchmark::State& state) {
	while (state.KeepRunning()) {
		SetLattice<int> sl;
		for (int i = 0; i < state.range_x(); i++) {
			sl.insert(i);
		}
	}
}

static void BM_SetInsertionAllDuplicate(benchmark::State& state) {
	while (state.KeepRunning()) {
		SetLattice<int> sl;
		for (int i = 0; i < state.range_x(); i++) {
			sl.insert(0);
		}
	}
}

static void BM_SetInsertionRandom(benchmark::State& state) {
	int num[state.range_x()];
	for (int i = 0; i < state.range_x(); i++) {
		num[i] = rand() % state.range_x();
	}
	while (state.KeepRunning()) {
		SetLattice<int> sl;
		for (int i = 0; i < state.range_x(); i++) {
			sl.insert(num[i]);
		}
	}
}