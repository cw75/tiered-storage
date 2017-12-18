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
  V get(const K& k, bool& succeed) {
    if (db.contain(k).reveal()) {
      succeed = true;
    } else {
      succeed = false;
    }
    return db.at(k);
  }
  bool put(const K& k, const V &v, bool& succeed) {
    bool replaced = db.at(k).Merge(v);
    succeed = true;
    return replaced;
  }
  void remove(const K& k) {
    db.remove(k);
  }
};

// Lock-free implementation of concurrent kvs
template <typename K, typename V>
class Concurrent_KV_Store{
protected:
  AtomicMapLattice<K, V> db;
  tbb::concurrent_unordered_map<K, atomic<int>*> lock_table;
public:
  Concurrent_KV_Store<K, V>() {}
  Concurrent_KV_Store<K, V>(AtomicMapLattice<K, V> &other) {
    db = other;
  }
  V get(const K& k, bool& succeed) {
    auto it = lock_table.find(k);
    if (it == lock_table.end()) {
      it = lock_table.insert({k, new atomic<int>(0)}).first;
    }
    int expected = 0;
    while(!it->second->compare_exchange_strong(expected, expected - 1)) {
      if (expected > 0) expected = 0;
    }
    if (db.contain(k).reveal()) {
      succeed = true;
    } else {
      succeed = false;
    }
    V result = db.at(k);
    it->second->fetch_add(1);
    return result;
  }
  bool put(const K& k, const V& v, bool& succeed) {
    auto it = lock_table.find(k);
    if (it == lock_table.end()) {
      it = lock_table.insert({k, new atomic<int>(0)}).first;
    }
    int expected = 0;
    while(!it->second->compare_exchange_strong(expected, expected + 1)) {
      expected = 0;
    }
    bool replaced = db.at(k).Merge(v);
    succeed = true;
    it->second->fetch_sub(1);
    return replaced;
  }
};

// Concurrent kvs implementation using software lock (mutex). Not as efficient as the previous lock-free implementation.

// class Concurrent_KV_Store{
// protected:
//  AtomicMapLattice<int, KVS_PairLattice> db;
//  tbb::concurrent_unordered_map<int, unique_ptr<mutex>> lock_table;
// public:
//  Concurrent_KV_Store() {}
//  Concurrent_KV_Store(AtomicMapLattice<int, KVS_PairLattice> other) {
//    db = other;
//  }
//  version_value_pair get(int k) {
//    auto it = lock_table.find(k);
//    if (it == lock_table.end()) {
//      it = lock_table.insert({k, unique_ptr<mutex>(new mutex)}).first;
//    }
//    lock_guard<mutex> lg(*(it->second));
//    return db.at(k).reveal();
//  }
//  void put(const int &k, const version_value_pair &p) {
//    auto it = lock_table.find(k);
//    if (it == lock_table.end()) {
//      it = lock_table.insert({k, unique_ptr<mutex>(new mutex)}).first;
//    }
//    lock_guard<mutex> lg(*(it->second));
//    db.at(k).merge(p);
//  }
// };
