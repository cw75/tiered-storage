#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "versioned_kv_store.h"
#include "benchmark/benchmark.h"

KV_Store<int, KVS_PairLattice<MaxLattice<int>>> kvs;
Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>> ckvs;

static void BM_KVSGET(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, p);
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			KVS_PairLattice<MaxLattice<int>> pl = kvs.get(i);
		}
	}
	kvs = KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSGET(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(i, p);
	}
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			KVS_PairLattice<MaxLattice<int>> pl = ckvs.get(i);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_KVSGETComparison(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> q = KVS_PairLattice<MaxLattice<int>>(p);
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			q;
		}
	}
}

static void BM_KVSPUT(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> pl = KVS_PairLattice<MaxLattice<int>>(p);

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			kvs.put(i, pl);
		}
	}
	kvs = KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSPUT_LOWCONTENTION(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> pl = KVS_PairLattice<MaxLattice<int>>(p);

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(i, pl);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_CKVSPUT_HIGHCONTENTION(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> pl = KVS_PairLattice<MaxLattice<int>>(p);

	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i+=state.threads) {
			ckvs.put(0, pl);
		}
	}
	if (state.thread_index == 0) ckvs = Concurrent_KV_Store<int, KVS_PairLattice<MaxLattice<int>>>();
}

static void BM_KVSPUTComparison(benchmark::State& state) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	KVS_PairLattice<MaxLattice<int>> pl = KVS_PairLattice<MaxLattice<int>>(p);
	KVS_PairLattice<MaxLattice<int>> res;
	while (state.KeepRunning()) {
		for (int i = 0; i < state.range_x(); i++) {
			res = pl;
		}
	}
}