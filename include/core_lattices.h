#ifndef INCLUDE_CORE_LATTICES_H
#define INCLUDE_CORE_LATTICES_H

#include <stdio.h>
#include <stdlib.h>

#include <cassert>
#include <iostream>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"

#include "include/base_lattices.h"

// NOTE(mwhittaker): Some general notes:
//
// - I think there is a lot of code duplication between the atomic and
//   non-atomic types. It would be neat if we could figure out a way to avoid
//   that.
// - Maybe each lattice should be put in its own file instead of having
//   everything in one big file?
// - Maybe this directory should be called lattices or something similar
//   instead of include?

class BoolLattice : public Lattice<bool> {
 public:
  BoolLattice() : Lattice() {}

  // NOTE(mwhittaker): I think primitives like bool should be passed by value.
  BoolLattice(const bool &e) : Lattice(e) {}

  // NOTE(mwhittaker): What purpose does this function serve? Why is the return
  // type an int?
  // this should probably be defined by the application
  const int when_true(const int (*f)()) const {
    if (element) {
      return (*f)();
    } else
      return 0;
  }

 protected:
  void do_merge(const bool &e) { element |= e; }
};

template <typename T>
class MaxLattice : public Lattice<T> {
 public:
  MaxLattice() : Lattice<T>() {}
  MaxLattice(const T &e) : Lattice<T>(e) {}
  BoolLattice gt(T n) const {
    if (this->element > n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  BoolLattice gt_eq(T n) const {
    if (this->element >= n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  // NOTE(mwhittaker): It seems like this lattice operates over any T that has
  // a total order. Should these add and substract operations be in the
  // interface? They don't seem related to the total order.
  MaxLattice<T> add(T n) const { return MaxLattice<T>(this->element + n); }
  MaxLattice<T> subtract(T n) const { return MaxLattice<T>(this->element - n); }

 protected:
  void do_merge(const T &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    int current = this->element;
    if (current < e) {
      this->element = e;
    }
  }
};

template <typename T>
class MinLattice : public Lattice<T> {
 public:
  MinLattice() {
    // NOTE(mwhittaker): Are MinLattice and MaxLattice only designed to work
    // for numbers?
    // this->assign(numeric_limits<T>::max());
    this->assign(static_cast<T>(1000000));
  }
  MinLattice(const T &e) : Lattice<T>(e) {}
  const T bot() const {
    // return numeric_limits<T>::max();
    return static_cast<T>(1000000);
  }
  BoolLattice lt(T n) const {
    if (this->element < n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  BoolLattice lt_eq(T n) const {
    if (this->element <= n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  MinLattice<T> add(T n) const { return MinLattice<T>(this->element + n); }
  MinLattice<T> subtract(T n) const { return MinLattice<T>(this->element - n); }

 protected:
  void do_merge(const T &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    int current = this->element;
    if (current > e) {
      this->element = e;
    }
  }
};

template <typename T>
class SetLattice : public Lattice<std::unordered_set<T>> {
 public:
  // NOTE(mwhittaker): I think the parent constructor will be called for us.
  SetLattice() : Lattice<std::unordered_set<T>>() {}
  SetLattice(const std::unordered_set<T> &e)
      : Lattice<std::unordered_set<T>>(e) {}
  MaxLattice<int> size() const { return MaxLattice<int>(this->element.size()); }
  void insert(const T &e) { this->element.insert(e); }
  // NOTE(mwhittaker): Argument should be taken by reference to const.
  SetLattice<T> intersect(std::unordered_set<T> s) const {
    std::unordered_set<T> res;
    for (auto iter_i = s.begin(); iter_i != s.end(); ++iter_i) {
      for (auto iter_j = this->element.begin(); iter_j != this->element.end();
           ++iter_j) {
        if (*iter_i == *iter_j) res.insert(*iter_i);
      }
    }
    return SetLattice<T>(res);
  }
  // NOTE(mwhittaker): I'm not familiar with the best way to take function
  // arguments, but I know there are some other options like taking in an
  // std::function or templating the argument.
  SetLattice<T> project(bool (*f)(T)) const {
    std::unordered_set<T> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (f(*it)) res.insert(*it);
    }
    return SetLattice<T>(res);
  }
  // NOTE(mwhittaker): Argument should be taken by reference to const.
  BoolLattice contain(T v) const {
    auto it = this->element.find(v);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

 protected:
  void do_merge(const std::unordered_set<T> &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    for (auto it = e.begin(); it != e.end(); ++it) {
      this->element.insert(*it);
    }
  }
};

template <typename K, typename V>
class MapLattice : public Lattice<std::unordered_map<K, V>> {
 public:
  // NOTE(mwhittaker): See above.
  MapLattice() : Lattice<std::unordered_map<K, V>>() {}
  MapLattice(const std::unordered_map<K, V> &m)
      : Lattice<std::unordered_map<K, V>>(m) {}
  MaxLattice<int> size() const { return this->element.size(); }
  MapLattice<K, V> intersect(MapLattice<K, V> other) const {
    MapLattice<K, V> res;
    std::unordered_map<K, V> m = other.reveal();
    for (auto it = m.begin(); it != this->m.end(); ++it) {
      if (this->contain(it->first).reveal()) {
        res.insert_pair(it->first, this->at(it->first));
        res.insert_pair(it->first, it->second);
      }
    }
    return res;
  }
  MapLattice<K, V> project(bool (*f)(V)) const {
    std::unordered_map<K, V> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (f(it->second)) res.emplace(it->first, it->second);
    }
    return MapLattice<K, V>(res);
  }
  SetLattice<K> key_set() const {
    std::unordered_set<K> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      res.insert(it->first);
    }
    return SetLattice<K>(res);
  }
  V &at(K k) { return this->element[k]; }
  // NOTE(mwhittaker): Argument should be taken by reference to const.
  BoolLattice contain(K k) const {
    auto it = this->element.find(k);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

 protected:
  void insert_pair(const K &k, const V &v) {
    auto search = this->element.find(k);
    if (search != this->element.end()) {
      // avoid copying the value out of the pair during casting!  Instead
      // cast the pointer. A bit ugly but seems like it should be safe.
      static_cast<V *>(&(search->second))->merge(v);
    } else {
      // need to copy v since we will be "growing" it within the lattice
      V new_v = v;
      this->element.emplace(k, new_v);
    }
  }
  void do_merge(const std::unordered_map<K, V> &m) {
    for (auto ms = m.begin(); ms != m.end(); ++ms) {
      this->insert_pair(ms->first, ms->second);
    }
  }
};

template <typename V>
class VectorLattice : public Lattice<std::vector<V>> {
 public:
  VectorLattice() : Lattice<std::vector<V>>() {}
  VectorLattice(const std::vector<V> &v) : Lattice<std::vector<V>>(v) {}

 protected:
  void do_merge(const std::vector<V> &v) {
    bool self_done = false;
    for (int i = 0; i < v.size(); i++) {
      if (!self_done && i == this->element.size()) self_done = true;
      if (!self_done) {
        // merge by position
        this->element[i].merge(v[i]);
      } else {
        // append additional input positions
        this->element.push_back(v[i]);
      }
    }
  }
};

// assume that once a value has been deleted, it cannot be re-inserted
template <typename T>
class TombstoneLattice : public MapLattice<T, BoolLattice> {
 public:
  TombstoneLattice() : MapLattice<T, BoolLattice>() {}
  TombstoneLattice(const std::unordered_map<T, BoolLattice> &m)
      : MapLattice<T, BoolLattice>(m) {}
  void insert(const T &e) { this->insert_pair(e, BoolLattice(false)); }
  void remove(const T &e) { this->insert_pair(e, BoolLattice(true)); }
  SetLattice<T> living_elements() const {
    std::unordered_set<T> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (!it->second.reveal()) res.insert(it->first);
    }
    return SetLattice<T>(res);
  }
};

// NOTE(mwhittaker): Same as http://en.cppreference.com/w/cpp/container/array?
template <typename T, size_t S>
struct slotArray {
  T slots[S];
  size_t size;
  slotArray<T, S>() { size = S; }
  // not sure why this constructor is called instead
  slotArray<T, S>(int a) { size = S; }
};

template <typename T, size_t S>
class ArrayLattice : public Lattice<slotArray<T, S>> {
 public:
  ArrayLattice() : Lattice<slotArray<T, S>>() {}
  ArrayLattice(const slotArray<T, S> &sa) : Lattice<slotArray<T, S>>(sa) {}
  int insert(const T &e, int index) {
    if (index >= S)
      return -1;
    else {
      T *ptr = &(this->element.slots[index]);
      ptr->merge(e);
      return 0;
    }
  }

 protected:
  void do_merge(const slotArray<T, S> &e) {
    // assume for now that size(e) is smaller, so no resize
    for (int i = 0; i < e.size; i++) {
      insert(e.slots[i], i);
    }
  }
};

class AtomicBoolLattice : public AtomicLattice<bool> {
 public:
  AtomicBoolLattice() : AtomicLattice<bool>() {}
  AtomicBoolLattice(const bool &e) : AtomicLattice<bool>(e) {}
  const int when_true(const int (*f)()) const {
    if (element) {
      return (*f)();
    } else
      return 0;
  }

 protected:
  void do_merge(const bool &e) {
    if (e == true && element.load() == false) element.store(e);
  }
};

template <typename T>
class AtomicMaxLattice : public AtomicLattice<T> {
 public:
  AtomicMaxLattice() : AtomicLattice<T>() {}
  AtomicMaxLattice(const T &e) : AtomicLattice<T>(e) {}
  BoolLattice gt(T n) const {
    if (this->element > n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  BoolLattice gt_eq(T n) const {
    if (this->element >= n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  MaxLattice<T> add(T n) const { return MaxLattice<T>(this->element + n); }
  MaxLattice<T> subtract(T n) const { return MaxLattice<T>(this->element - n); }

 protected:
  void do_merge(const T &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    T current = this->element.load();
    if (current < e) {
      while (!this->element.compare_exchange_strong(current, e)) {
        if (current >= e) break;
      }
    }
  }
};

template <typename T>
class AtomicMinLattice : public AtomicLattice<T> {
 public:
  AtomicMinLattice() { this->assign(static_cast<T>(1000000)); }
  AtomicMinLattice(const T &e) : AtomicLattice<T>(e) {}
  BoolLattice lt(T n) const {
    if (this->element < n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  BoolLattice lt_eq(T n) const {
    if (this->element <= n)
      return BoolLattice(true);
    else
      return BoolLattice(false);
  }
  MinLattice<T> add(T n) const { return MinLattice<T>(this->element + n); }
  MinLattice<T> subtract(T n) const { return MinLattice<T>(this->element - n); }

 protected:
  void do_merge(const T &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    T current = this->element.load();
    if (current > e) {
      while (!this->element.compare_exchange_strong(current, e)) {
        if (current <= e) break;
      }
    }
  }
};

template <typename T>
class AtomicSetLattice : public Lattice<tbb::concurrent_unordered_set<T>> {
 public:
  AtomicSetLattice() : Lattice<tbb::concurrent_unordered_set<T>>() {}
  AtomicSetLattice(const tbb::concurrent_unordered_set<T> &e)
      : Lattice<tbb::concurrent_unordered_set<T>>(e) {}
  MaxLattice<int> size() const { return MaxLattice<int>(this->element.size()); }
  void insert(const T &e) { this->element.insert(e); }
  SetLattice<T> intersect(std::unordered_set<T> s) const {
    std::unordered_set<T> res;
    for (auto iter_i = s.begin(); iter_i != s.end(); ++iter_i) {
      for (auto iter_j = this->element.begin(); iter_j != this->element.end();
           ++iter_j) {
        if (*iter_i == *iter_j) res.insert(*iter_i);
      }
    }
    return SetLattice<T>(res);
  }
  SetLattice<T> project(bool (*f)(T)) const {
    std::unordered_set<T> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (f(*it)) res.insert(*it);
    }
    return SetLattice<T>(res);
  }
  BoolLattice contain(T v) const {
    auto it = this->element.find(v);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

 protected:
  void do_merge(const tbb::concurrent_unordered_set<T> &e) {
    // we need 'this' because this is a templated class
    // 'this' makes the name dependent so that we can access the base definition
    for (auto it = e.begin(); it != e.end(); ++it) {
      this->element.insert(*it);
    }
  }
};

template <typename K, typename V>
class AtomicMapLattice : public Lattice<tbb::concurrent_unordered_map<K, V>> {
 public:
  AtomicMapLattice() : Lattice<tbb::concurrent_unordered_map<K, V>>() {}
  AtomicMapLattice(const tbb::concurrent_unordered_map<K, V> &m)
      : Lattice<tbb::concurrent_unordered_map<K, V>>(m) {}
  MaxLattice<int> size() const { return this->element.size(); }
  MapLattice<K, V> intersect(MapLattice<K, V> other) const {
    MapLattice<K, V> res;
    std::unordered_map<K, V> m = other.reveal();
    for (auto it = m.begin(); it != this->m.end(); ++it) {
      if (this->contain(it->first).reveal()) {
        res.insert_pair(it->first, this->at(it->first));
        res.insert_pair(it->first, it->second);
      }
    }
    return res;
  }
  MapLattice<K, V> project(bool (*f)(V)) const {
    std::unordered_map<K, V> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (f(it->second)) res.emplace(it->first, it->second);
    }
    return MapLattice<K, V>(res);
  }
  SetLattice<K> key_set() const {
    std::unordered_set<K> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      res.insert(it->first);
    }
    return SetLattice<K>(res);
  }
  V &at(K k) { return this->element[k]; }
  BoolLattice contain(K k) const {
    auto it = this->element.find(k);
    if (it == this->element.end())
      return BoolLattice(false);
    else
      return BoolLattice(true);
  }

 protected:
  void insert_pair(const K &k, const V &v) {
    auto search = this->element.find(k);
    if (search != this->element.end()) {
      // avoid copying the value out of the pair during casting!  Instead
      // cast the pointer. A bit ugly but seems like it should be safe.
      static_cast<V *>(&(search->second))->merge(v);
    } else {
      // need to copy v since we will be "growing" it within the lattice
      V new_v = v;
      // FIXME: it seems that there is a bug in tbb that fails to enable c++11
      // features with Clang on Linux. So we have to use insert instead of
      // emplace for now...
      // refer to
      // https://software.intel.com/en-us/forums/intel-threading-building-blocks/topic/591305
      this->element.insert({k, new_v});
    }
  }
  void do_merge(const tbb::concurrent_unordered_map<K, V> &m) {
    for (auto ms = m.begin(); ms != m.end(); ++ms) {
      this->insert_pair(ms->first, ms->second);
    }
  }
};

// assume that once a value has been deleted, it cannot be re-inserted
template <typename T>
class AtomicTombstoneLattice : public AtomicMapLattice<T, BoolLattice> {
 public:
  AtomicTombstoneLattice() : AtomicMapLattice<T, BoolLattice>() {}
  AtomicTombstoneLattice(const tbb::concurrent_unordered_map<T, BoolLattice> &m)
      : AtomicMapLattice<T, BoolLattice>(m) {}
  void insert(const T &e) { this->insert_pair(e, BoolLattice(false)); }
  void remove(const T &e) { this->insert_pair(e, BoolLattice(true)); }
  SetLattice<T> living_elements() const {
    std::unordered_set<T> res;
    for (auto it = this->element.begin(); it != this->element.end(); ++it) {
      if (!it->second.reveal()) res.insert(it->first);
    }
    return SetLattice<T>(res);
  }
};

#endif  // INCLUDE_CORE_LATTICES_H
