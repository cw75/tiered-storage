#include <stdio.h>
#include <stdlib.h>
#include "base_kv_store.h"

template <typename T>
struct TimestampValuePair {
	// MapLattice<int, MaxLattice<int>> v_map;
	int timestamp {-1};
	T value;

	TimestampValuePair<T>() {
		timestamp = -1;
		value = T();
	}

	// need this because of static cast
	TimestampValuePair<T>(int a) {
		timestamp = -1;
		value = T();
	}

	TimestampValuePair<T>(int ts, T v) {
		timestamp = ts;
		value = v;
	}
};

template <typename T>
class ReadCommittedPairLattice : public Lattice<TimestampValuePair<T>> {
protected:
    void do_merge(const TimestampValuePair<T> &p) {
    	if (p.timestamp >= this -> element.timestamp) {
    		this->element.timestamp = p.timestamp;
    		this->element.value = p.value;
    	}
    }

public:
    ReadCommittedPairLattice() : Lattice<TimestampValuePair<T>>() {}
    ReadCommittedPairLattice(const TimestampValuePair<T> &p)  : Lattice<TimestampValuePair<T>>(p) {}

    bool merge(const TimestampValuePair<T>& p) {
      if (p.timestamp >= this->element.timestamp) {
        this->element.timestamp = p.timestamp;
        this->element.value = p.value;

        return true;
      }

      return false;
    }

    bool merge(const ReadCommittedPairLattice<T>& pl) {
      return merge(pl.reveal());
    }
};
