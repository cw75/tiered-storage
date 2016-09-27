#ifndef LATTICES_SET_UNION_LATTICE_H_
#define LATTICES_SET_UNION_LATTICE_H_

#include <iterator>
#include <unordered_set>

#include "lattices/lattice.h"

namespace latticeflow {

// The semilattice of sets ordered by subset where join is union.
template <typename T>
class SetUnionLattice
    : public Lattice<SetUnionLattice<T>, std::unordered_set<T>> {
 public:
  SetUnionLattice() = default;
  explicit SetUnionLattice(const std::unordered_set<T>& xs) : xs_(xs) {}
  SetUnionLattice(const SetUnionLattice<T>& l) = delete;
  SetUnionLattice& operator=(const SetUnionLattice<T>& l) = delete;

  const std::unordered_set<T>& get() const override { return xs_; }

  void join(const SetUnionLattice<T>& l) override {
    xs_.insert(std::begin(l.xs_), std::end(l.xs_));
  }

  // See http://en.cppreference.com/w/cpp/container/set/insert.
  void insert(const T& x) { xs_.insert(x); }

 private:
  std::unordered_set<T> xs_;
};

}  // namespace latticeflow

#endif  // LATTICES_SET_UNION_LATTICE_H_
