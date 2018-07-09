#ifndef __BASE_KV_STORE_H__
#define __BASE_KV_STORE_H__

#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <mutex>

#include "../lattices/core_lattices.hpp"

using namespace std;

template <typename K, typename V>
class KVStore {
 protected:
  MapLattice<K, V> db;

 public:
  KVStore<K, V>() {}

  KVStore<K, V>(MapLattice<K, V>& other) { db = other; }

  V get(const K& k, unsigned& err_number) {
    if (!db.contain(k).reveal()) {
      err_number = 1;
    }
    return db.at(k);
  }

  bool put(const K& k, const V& v) { return db.at(k).merge(v); }

  void remove(const K& k) { db.remove(k); }
};

#endif
