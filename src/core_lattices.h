#ifndef CORE_LATTICES_H
#define CORE_LATTICES_H

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <limits>
#include "base_lattices.h"
#include "tbb/concurrent_unordered_set.h"
#include "tbb/concurrent_unordered_map.h"

using namespace std;

template <typename T>
class SetLattice : public Lattice<unordered_set<T>> {
protected:
	void do_merge(const unordered_set<T> &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		for ( auto it = e.begin(); it != e.end(); ++it ) {
			this->element.insert(*it);
		}
	}
public:
	SetLattice() : Lattice<unordered_set<T>>() {}
	SetLattice(const unordered_set<T> &e) : Lattice<unordered_set<T>>(e) {}
	const typename unordered_set<T>::size_type size() {
		return this->element.size();
	}
	void insert(const T &e) {
		this->element.insert(e);
	}
};

template <typename T>
class MaxLattice : public Lattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		int current = this->element;
		if (current < e) {
			this->element = e;
		}
	}

public:
	MaxLattice() : Lattice<T>() {}
	MaxLattice(const T &e) : Lattice<T>(e) {}
};

template <typename T>
class MinLattice : public Lattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		int current = this->element;
		if (current > e) {
			this->element = e;
		}
	}
public:
	MinLattice() {
		//this->assign(numeric_limits<T>::max());
		this->assign(static_cast<T> (1000000));
	}
	MinLattice(const T &e) : Lattice<T>(e) {}
	const T bot() const {
		//return numeric_limits<T>::max();
		return static_cast<T> (1000000);
	}
};

class BoolLattice : public Lattice<bool> {
protected:
	void do_merge(const bool &e) {
		element |= e;
	}
public:
	BoolLattice() : Lattice() {}
	BoolLattice(const bool &e) : Lattice(e) {}
};

template <typename K, typename V>
class MapLattice : public Lattice<unordered_map<K, V>> {
protected:
 	void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            // avoid copying the value out of the pair during casting!  Instead
            // cast the pointer. A bit ugly but seems like it should be safe.
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
	// MapLattice<K, V>() {}
	// MapLattice<K, V>(const unordered_map<K, V> &m){
	// 	this->element = m;
	// }
	MapLattice() : Lattice<unordered_map<K, V>>() {}
	MapLattice(const unordered_map<K, V> &m) : Lattice<unordered_map<K, V>>(m) {}
	const typename unordered_map<K, V>::size_type size() {
		return this->element.size();
	}
};

template <typename V>
class VectorLattice : public Lattice<vector<V>> {
protected:
    void do_merge(const vector<V> &v) {
        bool self_done = false;
        for (int i = 0; i < v.size(); i++) {
            if (!self_done && i == this->element.size()) self_done = true;
            if (!self_done) {
                // merge by position
                this->element[i].merge(v[i]);
            } else {
                // append additional input positions
                this->element.push_back(v[i]);
            }
        }
    }
public:
    // VectorLattice<V>() {}
    // VectorLattice<V>(const vector<V> &v) {
    //     this->element = v;
    // }
    VectorLattice() : Lattice<vector<V>>() {}
    VectorLattice(const vector<V> &v) : Lattice<vector<V>>(v) {}
};

// assume that once a value has been deleted, it cannot be re-inserted
template <typename T>
class TombstoneLattice : public MapLattice<T, BoolLattice> {
// protected:
//     void do_merge(const unordered_map<T, BoolLattice> &m) {
//     	this->assign(m);
//   //   	unordered_set<T> iset = t.insertion;
//   //   	unordered_set<T> dset = t.deletion;
//   //   	for ( auto it = iset.begin(); it != iset.end(); ++it ) {
// 		// 	this->element.insertion.insert(*it);
// 		// }
// 		// for ( auto it = dset.begin(); it != dset.end(); ++it ) {
// 		// 	this->element.deletion.insert(*it);
// 		// }
//     }
public:
    TombstoneLattice() : MapLattice<T, BoolLattice>() {}
    TombstoneLattice(const unordered_map<T, BoolLattice> &m)  : MapLattice<T, BoolLattice>(m) {}
    void insert(const T &e) {
		this->insert_pair(e, BoolLattice(false));
	}
	void remove(const T &e) {
		this->insert_pair(e, BoolLattice(true));
	}
};

// template <typename T>
// struct tombstoneSet {
// 	unordered_set<T> insertion;
// 	unordered_set<T> deletion;
// 	tombstoneSet<T>() {}
// 	// don't understand why we need this...
// 	tombstoneSet<T>(int a) {}
// 	tombstoneSet<T>(const unordered_set<T> &i, const unordered_set<T> &d) {
// 		this->insertion = i;
// 		this->deletion = d;
// 	}
// 	// tombstoneSet<T>& operator=(const tombstoneSet<T> &rhs) {
//  //      this->insertion = rhs.insertion;
//  //      this->deletion = rhs.deletion;
//  //      return *this;
//  //    }
// };

// template <typename T>
// class TombstoneSetLattice : public Lattice<tombstoneSet<T>> {
// protected:
//     void do_merge(const tombstoneSet<T> &t) {
//     	unordered_set<T> iset = t.insertion;
//     	unordered_set<T> dset = t.deletion;
//     	for ( auto it = iset.begin(); it != iset.end(); ++it ) {
// 			this->element.insertion.insert(*it);
// 		}
// 		for ( auto it = dset.begin(); it != dset.end(); ++it ) {
// 			this->element.deletion.insert(*it);
// 		}
//     }
// public:
//     TombstoneSetLattice() : Lattice<tombstoneSet<T>>() {}
//     TombstoneSetLattice(const tombstoneSet<T> &ts)  : Lattice<tombstoneSet<T>>(ts) {}
//     void insert(const T &e) {
// 		this->element.insertion.insert(e);
// 	}
// 	void remove(const T &e) {
// 		this->element.deletion.insert(e);
// 	}
// };

template <typename T, size_t S>
struct slotArray {
	T slots[S];
	size_t size;
	slotArray<T, S>() {
		size = S;
	}
	// not sure why this constructor is called instead
	slotArray<T, S>(int a) {
		size = S;
	}
};

template <typename T, size_t S>
class ArrayLattice : public Lattice<slotArray<T, S>> {
protected:
    void do_merge(const slotArray<T, S> &e) {
    	// assume for now that size(e) is smaller, so no resize
    	for (int i = 0; i < e.size; i++) {
    		insert(e.slots[i], i);
    	}
    }
public:
	ArrayLattice() : Lattice<slotArray<T, S>>() {}
	ArrayLattice(const slotArray<T, S> &sa) : Lattice<slotArray<T, S>>(sa) {}
	int insert(const T &e, int index) {
		if (index >= S) return -1;
		else {
			T * ptr = &(this->element.slots[index]);
			ptr->merge(e);
			return 0;
		}
	}
};

// template <typename T, size_t S>
// class ArrayLattice : public Lattice<T*> {
// protected:
//     void do_merge(T* const &e) {
//         this->assign(e);
//     }
// public:
// 	ArrayLattice() : Lattice<T*>() {
// 		this->element = (T*) malloc(S*sizeof(T));
// 		// have to explicitly initialize T using its default constructor
// 		for (int i = 0; i < S; i++) new (&(this->element[i])) T;
// 	}
// 	// ArrayLattice<T, S>(const unordered_map<K, V> &m){
// 	// 	this->element = unordered_map<K, V>(m);
// 	// }
// 	int insert(const T &e, int index) {
// 		if (index >= S) return -1;
// 		else {
// 			T * ptr = &(this->element[index]);
// 			ptr->merge(e);
// 			return 0;
// 		}
// 	}
// };

// class AtomicBoolLattice : public AtomicLattice<bool> {
// protected:
// 	virtual void do_merge(const bool &e) {
// 		element &= e;
// 	}
// public:
// 	AtomicBoolLattice() : AtomicLattice() {}
// 	AtomicBoolLattice(const bool &e) : AtomicLattice(e) {}
// };

template <typename T>
class AtomicMaxLattice : public AtomicLattice<T> {
protected:
	void do_merge(const T &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		T current = this->element.load();
		if (current < e) {
			while(!this->element.compare_exchange_strong(current, e)){
				if(current >= e) break;
			}
		}
	}
public:
	AtomicMaxLattice() : AtomicLattice<T>() {}
	AtomicMaxLattice(const T &e) : AtomicLattice<T>(e) {}
};

template <typename T>
class AtomicSetLattice : public Lattice<tbb::concurrent_unordered_set<T>> {
protected:
	void do_merge(const tbb::concurrent_unordered_set<T> &e) {
		// we need 'this' because this is a templated class
		// 'this' makes the name dependent so that we can access the base definition
		for ( auto it = e.begin(); it != e.end(); ++it ) {
			this->element.insert(*it);
		}
	}
public:
	AtomicSetLattice() : Lattice<tbb::concurrent_unordered_set<T>>() {}
	AtomicSetLattice(const tbb::concurrent_unordered_set<T> &e) : Lattice<tbb::concurrent_unordered_set<T>>(e) {}
	const typename tbb::concurrent_unordered_set<T>::size_type size() {
		return this->element.size();
	}
	void insert(const T &e) {
		this->element.insert(e);
	}
};

template <typename K, typename V>
class AtomicMapLattice : public Lattice<tbb::concurrent_unordered_map<K, V>> {
protected:
 	void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            // avoid copying the value out of the pair during casting!  Instead
            // cast the pointer. A bit ugly but seems like it should be safe.
            static_cast<V *>(&(search->second))->merge(v);
        } else {
            // need to copy v since we will be "growing" it within the lattice
            V new_v = v;
            this->element.emplace(k, new_v);
        }
    }
    void do_merge(const tbb::concurrent_unordered_map<K, V> &m) {
        for (auto ms = m.begin(); ms != m.end(); ++ms) {
            this->insert_pair(ms->first, ms->second);
        }
    }
public:
	AtomicMapLattice() : Lattice<tbb::concurrent_unordered_map<K, V>>() {}
	AtomicMapLattice(const tbb::concurrent_unordered_map<K, V> &m) : Lattice<tbb::concurrent_unordered_map<K, V>>(m) {}
	const typename tbb::concurrent_unordered_map<K, V>::size_type size() {
		return this->element.size();
	}
};







#endif