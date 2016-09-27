#ifndef LATTICES_MIN_LATTICE_H_
#define LATTICES_MIN_LATTICE_H_

#include "lattices/lattice.h"

#include <algorithm>

namespace latticeflow {

// The semilattice of an arbitrary totally ordered set where join is min. T
// must support all comparison operators.
template <typename T>
class MinLattice : public Lattice<MinLattice<T>, T> {
 public:
  MinLattice() : x_() {}
  explicit MinLattice(const T& x) : x_(x) {}
  MinLattice(const MinLattice<T>& l) = default;
  MinLattice& operator=(const MinLattice<T>& l) = default;

  const T& get() const override { return x_; }
  void join(const MinLattice<T>& l) override { x_ = std::min(x_, l.x_); }

  friend bool operator<(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ > rhs.x_;
  }
  friend bool operator<=(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ >= rhs.x_;
  }
  friend bool operator>(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ < rhs.x_;
  }
  friend bool operator>=(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ <= rhs.x_;
  }
  friend bool operator==(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ == rhs.x_;
  }
  friend bool operator!=(const MinLattice<T>& lhs, const MinLattice<T>& rhs) {
    return lhs.x_ != rhs.x_;
  }

 private:
  T x_;
};

}  // namespace latticeflow

#endif  // LATTICES_MIN_LATTICE_H_
