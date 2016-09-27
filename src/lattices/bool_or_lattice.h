#ifndef LATTICES_BOOL_OR_LATTICE_H_
#define LATTICES_BOOL_OR_LATTICE_H_

#include "lattices/lattice.h"

namespace latticeflow {

// The semilattice of booleans ordered by implication where join is logical or.
class BoolOrLattice : public Lattice<BoolOrLattice, bool> {
 public:
  BoolOrLattice() : BoolOrLattice(false) {}
  explicit BoolOrLattice(bool b) : b_(b) {}
  BoolOrLattice(const BoolOrLattice& l) = default;
  BoolOrLattice& operator=(const BoolOrLattice& l) = default;

  const bool& get() const override { return b_; }
  void join(const BoolOrLattice& l) override { b_ = b_ || l.b_; }

  friend bool operator<(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() < rhs.convert();
  }
  friend bool operator<=(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() <= rhs.convert();
  }
  friend bool operator>(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() > rhs.convert();
  }
  friend bool operator>=(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() >= rhs.convert();
  }
  friend bool operator==(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() == rhs.convert();
  }
  friend bool operator!=(const BoolOrLattice& lhs, const BoolOrLattice& rhs) {
    return lhs.convert() != rhs.convert();
  }

 private:
  // convert(true)  = 1
  // convert(false) = 0
  int convert() const { return b_ ? 1 : 0; }

  bool b_;
};

}  // namespace latticeflow

#endif  // LATTICES_BOOL_OR_LATTICE_H_
