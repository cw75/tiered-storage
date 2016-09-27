#ifndef LATTICES_MAX_LATTICE_H_
#define LATTICES_MAX_LATTICE_H_

#include "lattices/lattice.h"

#include <algorithm>

namespace latticeflow {

// The semilattice of an arbitrary totally ordered set where join is max. T
// must support all comparison operators.
template <typename T>
class MaxLattice : public Lattice<MaxLattice<T>, T> {
 public:
  MaxLattice() : x_() {}
  explicit MaxLattice(T x) : x_(std::move(x)) {}
  MaxLattice(const MaxLattice<T>& l) = default;
  MaxLattice& operator=(const MaxLattice<T>& l) = default;

  const T& get() const override { return x_; }
  void join(const MaxLattice<T>& l) override { x_ = std::max(x_, l.x_); }

  friend bool operator<(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ < rhs.x_;
  }
  friend bool operator<=(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ <= rhs.x_;
  }
  friend bool operator>(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ > rhs.x_;
  }
  friend bool operator>=(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ >= rhs.x_;
  }
  friend bool operator==(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ == rhs.x_;
  }
  friend bool operator!=(const MaxLattice<T>& lhs, const MaxLattice<T>& rhs) {
    return lhs.x_ != rhs.x_;
  }

 private:
  T x_;
};

}  // namespace latticeflow

#endif  // LATTICES_MAX_LATTICE_H_
