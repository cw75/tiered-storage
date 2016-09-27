#ifndef LATTICES_LATTICE_H_
#define LATTICES_LATTICE_H_

namespace latticeflow {

// Consider a partially ordered set (S, <=). An *upper bound* z of two elements
// a and b in S is an element of S such that z >= a and y >= b. An upper bound
// is said to be a *least upper bound* if it is less than or equal to all other
// upper bounds. That is, z is the least upper bound of a and b if
//
//   1. z is an upper bound of a and b, and
//   2. z is less than or equal to all upper bounds of a and b.
//
// Note that by the antisymmetry of <=, least upper bounds are unique.
//
// A *join semilattice* (or upper semillatice) is a partially ordered set (S,
// <=) such that every pair of elements in S has a least upper bound. The least
// upper bound of two elements x and y is known as their join, which we will
// denote by join(x, y). Note that join is associative, commutative, and
// idempotent.
//
//   - associative: for all x, y, z. join(x, join(y, z)) == join(join(x, y), z)
//   - commutative: for all x, y. join(x, y) == join(y, x)
//   - idempotent:  for all x. join(x, x) == x
//
// Dually, any structure (S, join) of a set S and an associative, commutative,
// idempotent operator join induces a partial order on S: x <= y if and only if
// join(x, y) == y. The structure (S, <=) of the set and the induced partial
// order forms a semillatice.
//
// We represent semilattices using CRTP where an instance of type `Lattice<L,
// T>` represents an instance of a semillatice with type `T` implemented by a
// subclass `L`. Refer to this directory for many examples.
//
// Note that semilattices are not required to have a bottom element, but if
// they do, it is recommended that the default constructor of the implementing
// class initialize to the bottom element.
template <typename L, typename T>
class Lattice {
 public:
  // Returns an element of the semilattice.
  virtual const T& get() const = 0;

  // Joins another instance of the semilattice into this one.
  virtual void join(const L& other) = 0;
};

}  // namespace latticeflow

#endif  // LATTICES_LATTICE_H_
