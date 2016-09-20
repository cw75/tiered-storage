#ifndef LATTICES_BOOL_AND_LATTICE_H_
#define LATTICES_BOOL_AND_LATTICE_H_

#include "lattices/lattice.h"

namespace latticeflow {

// The semilattice of booleans ordered by reverse implication where join is
// logical and.
class BoolAndLattice : public Lattice<BoolAndLattice, bool> {
 public:
  BoolAndLattice() : BoolAndLattice(true) {}
  explicit BoolAndLattice(bool b) : b_(b) {}
  BoolAndLattice(const BoolAndLattice& l) = default;
  BoolAndLattice& operator=(const BoolAndLattice& l) = default;

  const bool& get() const override { return b_; }
  void join(const BoolAndLattice& l) override { b_ = b_ && l.b_; }

  friend bool operator<(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() < rhs.convert();
  }
  friend bool operator<=(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() <= rhs.convert();
  }
  friend bool operator>(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() > rhs.convert();
  }
  friend bool operator>=(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() >= rhs.convert();
  }
  friend bool operator==(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() == rhs.convert();
  }
  friend bool operator!=(const BoolAndLattice& lhs, const BoolAndLattice& rhs) {
    return lhs.convert() != rhs.convert();
  }

 private:
  // convert(true)  = 0
  // convert(false) = 1
  int convert() const { return b_ ? 0 : 1; }

  bool b_;
};

}  // namespace latticeflow

#endif  // LATTICES_BOOL_AND_LATTICE_H_
