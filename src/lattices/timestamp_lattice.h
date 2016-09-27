#ifndef LATTICES_TIMESTAMP_LATTICE_H_
#define LATTICES_TIMESTAMP_LATTICE_H_

#include <utility>

#include "lattices/lattice.h"

namespace latticeflow {

// Consider a *timestamp* semilattice T and an arbitrary other semilattice L. A
// TimestampLattice<T, L> is pair (t, l) with the following join:
//
//                              { (t1, l1),                 if t1 > t2
//   join((t1, l1), (t2, l2)) = { (t2, l2),                 if t1 < t2
//                              { (t1 join t2, l1 join l2), otherwise
//
// In words, when merging two timestamp lattices, the one with the bigger
// timestamp completely dominates. If they share the same timestamp, then the
// *timestamp and value* are merged. Note that a TimestampLattice can be
// instantiated with a totally ordered timestamp type, like a Lamport clock, or
// a partially ordered timestamp type like a vector clock!
//
// Also, note that the Timestamp template must support the < operator where
//   - if a < b is true, then b is strictly larger than a, and
//   - if a < b is false, then either b is not strictly larger than a or the
//     two elements are not related.
template <typename Timestamp, typename L>
class TimestampLattice
    : public Lattice<TimestampLattice<Timestamp, L>, std::pair<Timestamp, L>> {
 public:
  TimestampLattice() = default;
  TimestampLattice(const Timestamp& t, const L& x) : p_(t, x) {}
  TimestampLattice(const TimestampLattice<Timestamp, L>& l) = default;
  TimestampLattice& operator=(const TimestampLattice<Timestamp, L>& l) =
      default;

  const std::pair<Timestamp, L>& get() const override { return p_; }

  void join(const TimestampLattice<Timestamp, L>& l) override {
    if (timestamp() < l.timestamp()) {
      // The other lattice's timestamp dominates ours, so we adopt its value.
      p_ = l.p_;
    } else if (l.timestamp() < timestamp()) {
      // Our timestamp dominates the other lattice's timestamp, so we do
      // nothing!
    } else {
      // If !(a < b) and !(b < a), then either a == b or the two are unrelated
      // with respect to the the Timestamp lattice's partial order. In either
      // case, we can merge both the timestamp and value.
      std::get<0>(p_).join(std::get<0>(l.p_));
      std::get<1>(p_).join(std::get<1>(l.p_));
    }
  }

  // Return the timestamp.
  const Timestamp& timestamp() const { return std::get<0>(p_); }

  // Return the value.
  const L& value() const { return std::get<1>(p_); }

 private:
  std::pair<Timestamp, L> p_;
};

}  // namespace latticeflow

#endif  // LATTICES_TIMESTAMP_LATTICE_H_
