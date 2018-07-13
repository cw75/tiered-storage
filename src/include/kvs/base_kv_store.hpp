#ifndef SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_
#define SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_

#include "../lattices/core_lattices.hpp"

template <typename K, typename V>
class KVStore {
 protected:
  MapLattice<K, V> db;

 public:
  KVStore<K, V>() {}

  KVStore<K, V>(MapLattice<K, V>& other) { db = other; }

  V get(const K& k, unsigned& err_number) {
    if (!db.contains(k).reveal()) {
      err_number = 1;
    }
    return db.at(k);
  }

  bool put(const K& k, const V& v) { return db.at(k).merge(v); }

  void remove(const K& k) { db.remove(k); }
};

#endif  // SRC_INCLUDE_KVS_BASE_KV_STORE_HPP_
