#ifndef INCLUDE_BASE_LATTICES_H
#define INCLUDE_BASE_LATTICES_H

#include <atomic>
#include <thread>

// NOTE(mwhittaker): I think there may be some peculiarities with the
// AtomicLattice and Lattice interfaces. Most notably, the interfaces preclude
// a semilattice's internal structure to differ from the external structure
// produced by `reveal`. For example, consider an increment only counter CRDT
// [1] implemented across n servers. The counter is an n-tuple of integers
// where server i only increments the ith integer. The `merge` method takes a
// pairwise max, and the `reveal` method returns the sum of all the integers.
//
// We could not implement such a CRDT using this interface because the internal
// structure (n-tuple of integers) and external structure (an integer) differ.
// We have to let the type parameter `T` be an int so that `reveal` returns an
// int, but then `do_merge` takes in an int and should be taking another
// increment-only counter.
//
// You could change the `do_merge` type to take in an `AtomicLattice<T>` but
// that leads to some weirdness too. Now, any two lattices that share the same
// external structure can be merged, but surely we only want to merge two types
// if they share the same internal structure!
//
// I think the AtomicLattice class should take a page from Java's Comparable
// interface [2] and use, as it's known in C++, the curiously recurring
// template pattern [3]:
//
//   template <typename L, typename T>
//   class Semilattice {
//     public:
//       virtual const T& get() const = 0;
//       virtual void merge(const L& other) = 0;
//   };
//
//   // Semilattice of booleans ordered by implication. false <= true.
//   class OrLattice : public Semilattice<OrLattice, bool> {
//     public:
//       OrLattice() : b_(false) {};
//       OrLattice(const bool b) : b_(b) {};
//       const bool& get() const override { return b_; }
//       void merge(const OrLattice& other) override { b_ |= other.b_; }
//
//     private:
//       bool b_;
//   };
//
//   // Semilattice of booleans ordered by reverse implication. true <= false.
//   class AndLattice : public Semilattice<AndLattice, bool> {
//     public:
//       AndLattice() : b_(true) {};
//       AndLattice(const bool b) : b_(b) {};
//       const bool& get() const override { return b_; }
//       void merge(const AndLattice& other) override { b_ &= other.b_; }
//     private:
//       bool b_;
//   };
//
// Now, the internal lattice structure (L) and external structure (T) can
// differ. Also, a semilattice can only be merged with semilattices of the same
// type. And finally, calling merge doesn't incur the cost of a virtual
// function call (I think).
//
// [1]: https://scholar.google.com/scholar?cluster=4496511741683930603&hl=en&as_sdt=0,5
// [2]: https://docs.oracle.com/javase/7/docs/api/java/lang/Comparable.html
// [3]: https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
template <typename T>
class AtomicLattice {
 public:
  AtomicLattice<T>() { assign(bot()); }

  // NOTE(mwhittaker): Why require that lattices be copyable? For big expensive
  // lattices like a set, maybe we would want to delete the copy constructor?
  AtomicLattice<T>(const T &e) { assign(e); }

  // NOTE(mwhittaker): Chapter 5 of "C++ Concurrency in Action" [1] explains
  // that atomic types don't have copy constructors (or copy assignment
  // operators) because a copy (or copy assigment) cannot be performed
  // atomically. Are there any issues with performing these operations
  // non-atomically here?
  //
  // [1]: https://www.manning.com/books/c-plus-plus-concurrency-in-action
  AtomicLattice<T>(const AtomicLattice<T> &other) { assign(other.reveal()); }

  virtual ~AtomicLattice<T>() = default;

  // NOTE(mwhittaker): See comment on copy constructor.
  AtomicLattice<T>& operator=(const AtomicLattice<T>& rhs) {
    assign(rhs.reveal());
    return *this;
  }
  bool operator==(const AtomicLattice<T> &rhs) const {
    return this->reveal() == rhs.reveal();
  }

  // NOTE(mwhittaker): Should we return `T` by value? What if copying T is
  // expensive? Maybe returning by constant reference would be better?
  const T reveal() const { return element.load(); }

  // NOTE(mwhittaker): Same as above.
  const T bot() const { return zero.load(); }
  void merge(const T &e) { return do_merge(e); }
  void merge(const AtomicLattice<T> &e) { return do_merge(e.reveal()); }
  void assign(const T e) { element.store(e); }
  void assign(const AtomicLattice<T> &e) { element.store(e.reveal()); }

 protected:
  std::atomic<T> element;
  // NOTE(mwhittaker): Some questions:
  //
  //   1. Why are we requiring that semilattices have a bottom element?
  //   2. Why are we requiring the bottom element be castable from the literal
  //      0? Why not make `bot` (see above) a virtual method or something
  //      similar?
  //   3. Why is zero a non-static member? Shouldn't this be static?
  const std::atomic<T> zero{static_cast<T>(0)};
  virtual void do_merge(const T &e) = 0;
};

// NOTE(mwhittaker): See above.
template <typename T>
class Lattice {
 public:
  Lattice<T>() { assign(bot()); }
  Lattice<T>(const T &e) { assign(e); }
  Lattice<T>(const Lattice<T> &other) { assign(other.reveal()); }
  virtual ~Lattice<T>() = default;
  Lattice<T> &operator=(const Lattice<T> &rhs) {
    assign(rhs.reveal());
    return *this;
  }
  bool operator==(const Lattice<T> &rhs) const {
    return this->reveal() == rhs.reveal();
  }
  const T &reveal() const { return element; }
  const T &bot() const { return zero; }
  void merge(const T &e) { return do_merge(e); }
  void merge(const Lattice<T> &e) { return do_merge(e.reveal()); }
  void assign(const T e) { element = e; }
  void assign(const Lattice<T> &e) { element = e.reveal(); }

 protected:
  T element;
  const T zero{static_cast<T>(0)};
  virtual void do_merge(const T &e) = 0;

};

// NOTE(mwhittaker):
//
// - I don't think l2 needs to be passed by value.
// - Maybe it would be more efficient to have semilattices implement a <=
//   operator so that dominance can be checked without requiring a copy and a
//   merge?
template <typename T>
bool dominated(T l1, T l2) {
  l1.merge(l2);
  return l1 == l2;
}

#endif  // INCLUDE_BASE_LATTICES_H
