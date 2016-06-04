#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

AtomicMaxLattice<int> aml;

static void BM_AtomicMaxMerge(benchmark::State& state) {
	while (state.KeepRunning()) {
		for (int i = state.thread_index; i < 1000000; i += state.threads) {
			aml.merge(i);
		}
	}
	if (state.thread_index == 0) aml.assign(0);
}

static void BM_AtomicMaxMergeComparison(benchmark::State& state) {
	MaxLattice<int> ml;
	while (state.KeepRunning()) {
		for (int i = state.thread_index; i < 1000000; i += state.threads) {
			ml.merge(i);
		}
	}
	ml.assign(0);
}