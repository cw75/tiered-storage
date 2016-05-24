#ifndef BASE_LATTICES_H
#define BASE_LATTICES_H

#include <atomic>
#include <thread>
using namespace std;

template <typename T>
class AtomicLattice{
protected:
	atomic<T> element;
	const atomic<T> zero {static_cast<T> (0)};
	virtual void do_merge(const T &e) = 0;
public:
	AtomicLattice<T>() {
		assign(bot());
	}
	AtomicLattice<T>(const T &e) {
		assign(e);
	}
	AtomicLattice<T>(const AtomicLattice<T> &other) {
		assign(other.reveal());
	}
	virtual ~AtomicLattice<T> () = default;
	AtomicLattice<T>& operator=(const AtomicLattice<T> &rhs) {
      assign(rhs.reveal());
      return *this;
    }
    bool operator==(const AtomicLattice<T>& rhs) {
		return this->reveal() == rhs.reveal();
	}
	const T reveal() const {
		return element.load();
	}
	const T bot() const {
		return zero.load();
	}
	void merge(const T &e) {
		return do_merge(e);
	}
	void merge(const AtomicLattice<T> &e) {
		return do_merge(e.reveal());
	}
	void assign(const T e) {
		element.store(e);
	}
};

template <typename T>
class Lattice{
protected:
	T element;
	const T zero {static_cast<T> (0)};
	virtual void do_merge(const T &e) = 0;
public:
	Lattice<T>() {
		assign(bot());
	}
	Lattice<T>(const T &e) {
		assign(e);
	}
	Lattice<T>(const Lattice<T> &other) {
		assign(other.reveal());
	}
	virtual ~Lattice<T> () = default;
	Lattice<T>& operator=(const Lattice<T> &rhs) {
      assign(rhs.reveal());
      return *this;
    }
    bool operator==(const Lattice<T>& rhs) {
		return this->reveal() == rhs.reveal();
	}
	const T &reveal() const {
		return element;
	}
	const T &bot() const {
		return zero;
	}
	void merge(const T &e) {
		return do_merge(e);
	}
	void merge(const Lattice<T> &e) {
		return do_merge(e.reveal());
	}
	void assign(const T e) {
		element = e;
	}
};

#endif
