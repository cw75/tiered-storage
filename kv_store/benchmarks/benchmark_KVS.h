#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "kv_store.h"
#include "benchmark/benchmark.h"

KV_Store kvs;

static void BM_KVSGET(benchmark::State& state) {
	version_value_pair p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	kvs.put('A', p);
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.get('A').value;
		}
	}
}

static void BM_KVSGETComparison(benchmark::State& state) {
	version_value_pair p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			p.value;
		}
	}
}

static void BM_KVSPUT(benchmark::State& state) {
	version_value_pair p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.put('A', p);
		}
	}
}

static void BM_KVSPUTComparison(benchmark::State& state) {
	version_value_pair p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	version_value_pair q;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			q = p;
		}
	}
}