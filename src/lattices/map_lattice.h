#ifndef LATTICES_MAP_LATTICE_H_
#define LATTICES_MAP_LATTICE_H_

#include <unordered_map>
#include <utility>

#include "lattices/bool_or_lattice.h"
#include "lattices/lattice.h"
#include "lattices/max_lattice.h"

namespace latticeflow {

// A MapLattice<K, V> represents a map from an arbitrary type K to a lattice V.
// The value bound to a key in the join of two maps depends on the presence of
// the key in the two maps.
//
//   +----------+----------+--------------------+
//   | k in m_1 | k in m_2 | (m_1 join m_2)[k]  |
//   +----------+----------+--------------------+
//   | n        | n        | undefined          |
//   | n        | y        | m_2[k]             |
//   | y        | n        | m_1[k]             |
//   | y        | y        | m_1[k] join m_2[k] |
//   +----------+----------+--------------------+
//
// For example, let m_1 = {"a": 42, "b": 19} and m_2 = {"b": 1000, "c": 99} be
// two maps from string to MaxLattice<int>. The join of m_1 and m_2 is {"a":
// 42, "b": 9000, "c": 99}.
template <typename K, typename V>
class MapLattice : public Lattice<MapLattice<K, V>, std::unordered_map<K, V>> {
 public:
  MapLattice() = default;
  MapLattice(const MapLattice<K, V>& l) = delete;
  MapLattice& operator=(const MapLattice<K, V>& l) = delete;

  const std::unordered_map<K, V>& get() const override { return kvs_; }

  void join(const MapLattice<K, V>& l) override {
    for (const auto& kv : l.kvs_) {
      const K& key = std::get<0>(kv);
      const V& value = std::get<1>(kv);
      if (kvs_.count(key) == 0) {
        kvs_.insert(kv);
      } else {
        kvs_[key].join(value);
      }
    }
  }

  // See http://en.cppreference.com/w/cpp/container/unordered_map/at.
  const V& get(const K& key) const { return kvs_.at(key); }

  // `kv.put(k, v)` binds `k` to `v` if `kv` doesn't have an existing binding
  // for `k`. Otherwises, it joins `v` into the existing binding. For example,
  // {"a": 1}.put("b", 2) produces {"a": 1, "b": 2} and {"a": 1}.put("a", 2)
  // produces {"a": 1 join 2}.
  void put(const K& key, const V& val) {
    if (kvs_.count(key) == 0) {
      kvs_.insert(std::make_pair(key, val));
    } else {
      kvs_[key].join(val);
    }
  }

  // TODO(mwhittaker): Figure out very carefully which other map operations to
  // support. We should only support the "monotonic" ones?
  // http://en.cppreference.com/w/cpp/container/unordered_map

 private:
  std::unordered_map<K, V> kvs_;
};

}  // namespace latticeflow

#endif  // LATTICES_MAP_LATTICE_H_
