#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <memory>
#include "core_lattices.h"

using namespace std;

struct version_value_pair {
	MapLattice<int, MaxLattice<int>> v_map;
	MaxLattice<int> value;

	version_value_pair() {
		v_map = MapLattice<int, MaxLattice<int>>();
		value = MaxLattice<int>();
	}
	// need this because of static cast
	version_value_pair(int a) {
		v_map = MapLattice<int, MaxLattice<int>>();
		value = MaxLattice<int>();
	}
	version_value_pair(MapLattice<int, MaxLattice<int>> m, MaxLattice<int> v) {
		v_map = m;
		value = v;
	}
};

class KVS_PairLattice : public Lattice<version_value_pair> {
protected:
    void do_merge(const version_value_pair &p) {
    	bool smaller = dominated(element.v_map, p.v_map);
    	bool bigger = dominated(p.v_map, element.v_map);
    	if (bigger);
    	else if (smaller) {
    		element.v_map.merge(p.v_map);
    		element.value.assign(p.value);
    	}
    	else {
    		element.v_map.merge(p.v_map);
    		element.value.merge(p.value);
    	}
    }
public:
    KVS_PairLattice() : Lattice<version_value_pair>() {}
    KVS_PairLattice(const version_value_pair &p)  : Lattice<version_value_pair>(p) {}
};

class KV_Store{
protected:
	MapLattice<char, KVS_PairLattice> db;
public:
	KV_Store() {}
	KV_Store(MapLattice<char, KVS_PairLattice> other) {
		db = other;
	}
	const version_value_pair &get(char k) {
		return db.at(k).reveal();
	}
	void put(char k, const version_value_pair &p) {
		db.at(k).merge(p);
	}
};

// class Concurrent_KV_Store{
// protected:
// 	AtomicMapLattice<char, KVS_PairLattice> db;
// 	tbb::concurrent_unordered_map<char, unique_ptr<mutex>> lock_table;
// public:
// 	Concurrent_KV_Store() {}
// 	Concurrent_KV_Store(AtomicMapLattice<char, KVS_PairLattice> other) {
// 		db = other;
// 	}
// 	version_value_pair get(char k) {
// 		auto it = lock_table.find(k);
// 		if (it == lock_table.end()) {
// 			it = lock_table.insert({k, unique_ptr<mutex>(new mutex)}).first;
// 		}
// 		lock_guard<mutex> lg(*(it->second));
// 		version_value_pair p = db.at(k).reveal();
// 		//lock_table.at(k).unlock();
// 		return p;
// 	}
// 	void put(const char &k, const version_value_pair &p) {
// 		//lock_table.emplace(k, mutex);
// 		db.at(k).merge(p);
// 		//lock_table[k];
// 	}
// };


