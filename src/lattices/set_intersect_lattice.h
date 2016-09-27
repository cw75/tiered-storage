#ifndef LATTICES_SET_INTERSECT_LATTICE_H_
#define LATTICES_SET_INTERSECT_LATTICE_H_

#include <iterator>
#include <unordered_set>

#include "lattices/lattice.h"

namespace latticeflow {

// The semilattice of sets ordered by superset where join is intersect.
template <typename T>
class SetIntersectLattice
    : public Lattice<SetIntersectLattice<T>, std::unordered_set<T>> {
 public:
  SetIntersectLattice() = default;
  explicit SetIntersectLattice(const std::unordered_set<T>& xs) : xs_(xs) {}
  SetIntersectLattice(const SetIntersectLattice<T>& l) = delete;
  SetIntersectLattice& operator=(const SetIntersectLattice<T>& l) = delete;

  const std::unordered_set<T>& get() const override { return xs_; }

  void join(const SetIntersectLattice<T>& l) override {
    for (auto it = std::begin(xs_); it != std::end(xs_);) {
      if (l.xs_.count(*it) > 0) {
        ++it;
      } else {
        it = xs_.erase(it);
      }
    }
  }

 private:
  std::unordered_set<T> xs_;
};

}  // namespace latticeflow

#endif  // LATTICES_SET_INTERSECT_LATTICE_H_
