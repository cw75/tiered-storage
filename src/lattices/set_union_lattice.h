#ifndef LATTICES_SET_UNION_LATTICE_H_
#define LATTICES_SET_UNION_LATTICE_H_

#include <iterator>
#include <unordered_set>

#include "lattices/lattice.h"

// The semilattice of sets ordered by subset where join is union.
template <typename T>
class SetUnionLattice
    : public Lattice<SetUnionLattice<T>, std::unordered_set<T>> {
 public:
  SetUnionLattice() = default;
  SetUnionLattice(const SetUnionLattice<T>& l) = delete;
  SetUnionLattice& operator=(const SetUnionLattice<T>& l) = delete;

  const std::unordered_set<T>& get() const override { return xs_; }
  void join(const SetUnionLattice<T>& l) override {
    xs_.insert(std::begin(l.xs_), std::end(l.xs_));
  }

 private:
  std::unordered_set<T> xs_;
};

#endif  // LATTICES_SET_UNION_LATTICE_H_
