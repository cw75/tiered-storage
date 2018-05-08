#ifndef CORE_LATTICES_H
#define CORE_LATTICES_H

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <limits>
#include <cassert>
#include "base_lattices.h"

using namespace std;


class BoolLattice : public Lattice<bool> {
protected:
	void do_merge(const bool &e) {
		element |= e;
	}
public:
	BoolLattice() : Lattice() {}
	BoolLattice(const bool &e) : Lattice(e) {}
	// this should probably be defined by the application
	// just make it an if-top test and put it in the lattice superclass
	const int when_true(const int (*f)()) const{
		if (element) {
			return (*f)();
		}
		else return 0;
	}
};

template <typename T>
class MaxLattice : public Lattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition

		// T::operator< has to be properly overloaded
		int current = this->element;
		if (current < e) {
			this->element = e;
		}
	}
public:
	MaxLattice() : Lattice<T>() {}
	MaxLattice(const T &e) : Lattice<T>(e) {}
	// overload with operator>

	// non-monotone mehtod should appear outside the initial class definition, fix throughout
	// wrap with namespace? c++ custom type modifier
	// for now, all non-merge methods are non-destructive
	MaxLattice<T> add(T n) const{
		return MaxLattice<T>(this->element + n);
	}
	MaxLattice<T> subtract(T n) const{
		return MaxLattice<T>(this->element - n);
	}
};


template <typename T>
class SetLattice : public Lattice<unordered_set<T>> {
protected:
	void do_merge(const unordered_set<T> &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition

		// is there a more efficient bulk merge for c++ set --no
		// maybe worth doing sort-merge std::sort followed by std::set_union (benchmark)
		// for larger set, implement join algorithm
		for ( auto it = e.begin(); it != e.end(); ++it ) {
			this->element.insert(*it);
		}
	}
public:
	SetLattice() : Lattice<unordered_set<T>>() {}
	SetLattice(const unordered_set<T> &e) : Lattice<unordered_set<T>>(e) {}
	MaxLattice<int> size() const{
		return MaxLattice<int>(this->element.size());
	}

	void insert(const T &e) {
		this->element.insert(e);
	}

	SetLattice<T> intersect(unordered_set<T> s) const{
		unordered_set<T> res;
		for ( auto iter_i = s.begin(); iter_i != s.end(); ++iter_i ) {
			for ( auto iter_j = this->element.begin(); iter_j != this->element.end(); ++iter_j ) {
				if (*iter_i == *iter_j) res.insert(*iter_i);
			}
		}
		return SetLattice<T>(res);
	}

	// rename it "map"
	// this should be outside the library
	SetLattice<T> project(bool (*f)(T)) const{
		unordered_set<T> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(*it)) res.insert(*it);
        }
        return SetLattice<T>(res);
	}
};



template <typename K, typename V>
class MapLattice : public Lattice<unordered_map<K, V>> {
protected:

 	void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            // avoid copying the value out of the pair during casting!  Instead
            // cast the pointer. A bit ugly but seems like it should be safe.
            // WHY NOT search->second.merge(v)??
            // or at least
            // (static_cast<V>(search->second)).merge(v);
            static_cast<V *>(&(search->second))->merge(v);
        } else {
            // need to copy v since we will be "growing" it within the lattice
            V new_v = v;
            this->element.emplace(k, new_v);
        }
    }

    void do_merge(const unordered_map<K, V> &m) {
        for (auto ms = m.begin(); ms != m.end(); ++ms) {
            this->insert_pair(ms->first, ms->second);
        }
    }
public:

	MapLattice() : Lattice<unordered_map<K, V>>() {}
	MapLattice(const unordered_map<K, V> &m) : Lattice<unordered_map<K, V>>(m) {}
	MaxLattice<int> size() const{
		return this->element.size();
	}

	// sort and set_intersect
	// how c++ deal with map intersect
	MapLattice<K, V> intersect(MapLattice<K, V> other) const{
		MapLattice<K, V> res;
		unordered_map<K, V> m = other.reveal();
        for (auto it = m.begin(); it != this->m.end(); ++it) {
            if(this->contain(it->first).reveal()) {
            	res.insert_pair(it->first, this->at(it->first));
            	res.insert_pair(it->first, it->second);
            }
        }
        return res;
	}

  // move it out
	MapLattice<K, V> project(bool (*f)(V)) const{
		unordered_map<K, V> res;
		for (auto it = this->element.begin(); it != this->element.end(); ++it) {
            if(f(it->second)) res.emplace(it->first, it->second);
        }
        return MapLattice<K, V>(res);
	}

	BoolLattice contain(K k) const{
		auto it = this->element.find(k);
		if (it == this->element.end()) return BoolLattice(false);
		else return BoolLattice(true);
	}

  // use c++ built-in function
	SetLattice<K> key_set() const{
		unordered_set<K> res;
		for ( auto it = this->element.begin(); it != this->element.end(); ++it) {
			res.insert(it->first);
		}
		return SetLattice<K>(res);
	}

	V &at(K k) {
		return this->element[k];
	}

  void remove(K k) {
		auto it = this->element.find(k);
		if (it != this->element.end()) this->element.erase(k);
	}
};

#endif
