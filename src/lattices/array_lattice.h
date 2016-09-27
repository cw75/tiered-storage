#ifndef LATTICES_ARRAY_LATTICE_H_
#define LATTICES_ARRAY_LATTICE_H_

#include <array>
#include <cstddef>
#include <initializer_list>

#include "lattices/lattice.h"

namespace latticeflow {

// Consider an arbitrary lattice T = (S_T, join_T). We can form an array
// lattice of size N A = (S_t ^ N, join_A) where
//
//   join_A([x1, ..., xN], [y1, ..., yN]) = [x1 join_T y1, ..., xN join_T yN]
//
// That is, array semillatices are merged pairwise.
template <typename T, std::size_t N>
class ArrayLattice : public Lattice<ArrayLattice<T, N>, std::array<T, N>> {
 public:
  ArrayLattice() = default;
  explicit ArrayLattice(std::array<T, N> xs) : xs_(xs) {}
  ArrayLattice(const ArrayLattice<T, N>& l) = delete;
  ArrayLattice& operator=(const ArrayLattice<T, N>& l) = delete;

  const std::array<T, N>& get() const override { return xs_; }

  void join(const ArrayLattice<T, N>& l) override {
    for (int i = 0; i < N; ++i) {
      xs_[i].join(l.xs_[i]);
    }
  }

 private:
  std::array<T, N> xs_;
};

}  // namespace latticeflow

#endif  // LATTICES_ARRAY_LATTICE_H_
