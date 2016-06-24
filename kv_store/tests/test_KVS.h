#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "versioned_kv_store.h"

typedef unordered_map<int, MaxLattice<int>> intMaxIntMap;

class KVStoreTest : public ::testing::Test {
protected:
	KV_Store<int, KVS_PairLattice<MaxLattice<int>>>* kvs;
	intMaxIntMap map1 = {{1, MaxLattice<int>(1)}};
	intMaxIntMap map2 = {{1, MaxLattice<int>(1)}, {2, MaxLattice<int>(1)}};
	intMaxIntMap map3 = {{1, MaxLattice<int>(2)}, {2, MaxLattice<int>(1)}};
	KVStoreTest() {
		kvs = new KV_Store<int, KVS_PairLattice<MaxLattice<int>>>;
	}
	virtual ~KVStoreTest() {
		delete kvs;
	}
};

TEST_F(KVStoreTest, GETPUT) {
	version_value_pair<MaxLattice<int>> p;
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(1)}}));
	p.value = 10;
	kvs->put(1, p);
	p = kvs->get(1).reveal();
	EXPECT_EQ(10, p.value.reveal());
	EXPECT_EQ(map1, p.v_map.reveal());
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{2, MaxLattice<int>(1)}}));
	p.value = 20;
	kvs->put(1, p);
	p = kvs->get(1).reveal();
	EXPECT_EQ(20, p.value.reveal());
	EXPECT_EQ(map2, p.v_map.reveal());
	p.v_map = MapLattice<int, MaxLattice<int>>(unordered_map<int, MaxLattice<int>>({{1, MaxLattice<int>(2)}, {2, MaxLattice<int>(1)}}));
	p.value = 5;
	kvs->put(1, p);
	p = kvs->get(1).reveal();
	EXPECT_EQ(5, p.value.reveal());
	EXPECT_EQ(map3, p.v_map.reveal());
}






