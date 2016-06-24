#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <memory>
#include "core_lattices.h"

using namespace std;

template <typename K, typename V>
class KV_Store{
protected:
	MapLattice<K, V> db;
public:
	KV_Store<K, V>() {}
	KV_Store<K, V>(MapLattice<K, V> &other) {
		db = other;
	}
	const V &get(K k) {
		return db.at(k);
	}
	void put(K k, const V &v) {
		db.at(k).merge(v);
	}
};

// Lock-free implementation of concurrent kvs
template <typename K, typename V>
class Concurrent_KV_Store{
protected:
	AtomicMapLattice<K, V> db;
	tbb::concurrent_unordered_map<int, unique_ptr<atomic<int>>> lock_table;
public:
	Concurrent_KV_Store<K, V>() {}
	Concurrent_KV_Store<K, V>(AtomicMapLattice<K, V> &other) {
		db = other;
	}
	V get(int k) {
		auto it = lock_table.find(k);
		if (it == lock_table.end()) {
			it = lock_table.insert({k, unique_ptr<atomic<int>>(new atomic<int>(0))}).first;
		}
		int expected = 0;
		while(!it->second->compare_exchange_strong(expected, expected - 1)) {
			if (expected > 0) expected = 0;
		}
		V result = db.at(k);
		it->second->fetch_add(1);
		return result;
	}
	void put(const K &k, const V &v) {
		auto it = lock_table.find(k);
		if (it == lock_table.end()) {
			it = lock_table.insert({k, unique_ptr<atomic<int>>(new atomic<int>(0))}).first;
		}
		int expected = 0;
		while(!it->second->compare_exchange_strong(expected, expected + 1)) {
			expected = 0;
		}
		db.at(k).merge(v);
		it->second->fetch_sub(1);
	}
};

// Concurrent kvs implementation using software lock (mutex). Not as efficient as the previous lock-free implementation.

// class Concurrent_KV_Store{
// protected:
// 	AtomicMapLattice<int, KVS_PairLattice> db;
// 	tbb::concurrent_unordered_map<int, unique_ptr<mutex>> lock_table;
// public:
// 	Concurrent_KV_Store() {}
// 	Concurrent_KV_Store(AtomicMapLattice<int, KVS_PairLattice> other) {
// 		db = other;
// 	}
// 	version_value_pair get(int k) {
// 		auto it = lock_table.find(k);
// 		if (it == lock_table.end()) {
// 			it = lock_table.insert({k, unique_ptr<mutex>(new mutex)}).first;
// 		}
// 		lock_guard<mutex> lg(*(it->second));
// 		return db.at(k).reveal();
// 	}
// 	void put(const int &k, const version_value_pair &p) {
// 		auto it = lock_table.find(k);
// 		if (it == lock_table.end()) {
// 			it = lock_table.insert({k, unique_ptr<mutex>(new mutex)}).first;
// 		}
// 		lock_guard<mutex> lg(*(it->second));
// 		db.at(k).merge(p);
// 	}
// };
