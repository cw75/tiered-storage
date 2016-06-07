#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "kv_store.h"
#include "benchmark/benchmark.h"

KV_Store kvs;
Concurrent_KV_Store ckvs;

static void BM_KVSGET(benchmark::State& state) {
	// version_value_pair p;
	// p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	// p.value = 10;
	// kvs.put(1, p);
	for (int i = 0; i < state.range_x(); i++) {
			kvs.get(i).value;
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.get(i).value;
		}
	}
	kvs = KV_Store();
}

static void BM_CKVSGET(benchmark::State& state) {
	// version_value_pair p;
	// p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	// p.value = 10;
	// ckvs.put(1, p);
	for (int i = 0; i < state.range_x(); i++) {
			ckvs.get(i).value;
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			ckvs.get(i).value;
		}
	}
	ckvs = Concurrent_KV_Store();
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
	for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
		}
	}
	kvs = KV_Store();
}

static void BM_CKVSPUT(benchmark::State& state) {
	version_value_pair p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i++) {
			ckvs.put(i, p);
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			ckvs.put(i, p);
		}
	}
	ckvs = Concurrent_KV_Store();
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