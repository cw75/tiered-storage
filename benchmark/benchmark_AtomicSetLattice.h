#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "core_lattices.h"
#include "benchmark/benchmark.h"

AtomicSetLattice<int> asl;

static void BM_AtomicSetInsert(benchmark::State& state) {
	while (state.KeepRunning()) {
		for (int i = state.thread_index; i < 100000; i += state.threads) {
			asl.insert(i);
		}
	}
	if (state.thread_index == 0) asl.assign(asl.bot());
}

static void BM_AtomicSetInsertComparison(benchmark::State& state) {
	SetLattice<int> sl;
	while (state.KeepRunning()) {
		for (int i = state.thread_index; i < 100000; i += state.threads) {
			sl.insert(i);
		}
	}
	sl.assign(sl.bot());
}