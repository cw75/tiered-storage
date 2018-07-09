#ifndef BASE_LATTICES_H
#define BASE_LATTICES_H

#include <atomic>
#include <thread>

template <typename T>
class Lattice {
 protected:
  T element;
  const T zero{static_cast<T>(0)};
  virtual void do_merge(const T &e) = 0;

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
};

#endif
