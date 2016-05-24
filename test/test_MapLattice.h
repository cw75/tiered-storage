#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

typedef unordered_map<char, MaxLattice<int>> charMaxIntMap;

class MapLatticeTest : public ::testing::Test {
protected:
	MapLattice<char, MaxLattice<int>>* mapl;
	charMaxIntMap map1 = {{'a', MaxLattice<int>(10)}, {'b', MaxLattice<int>(20)}};
	charMaxIntMap map2 = {{'b', MaxLattice<int>(30)}, {'c', MaxLattice<int>(40)}};
	charMaxIntMap map3 = {{'a', MaxLattice<int>(10)}, {'b', MaxLattice<int>(30)}, {'c', MaxLattice<int>(40)}};
	MapLatticeTest() {
		mapl = new MapLattice<char, MaxLattice<int>>;
	}
	virtual ~MapLatticeTest() = default;
	void check_equality(charMaxIntMap m) {
		EXPECT_EQ(m.size(), mapl->size());
		charMaxIntMap result = mapl->reveal();
		for ( auto it = result.begin(); it != result.end(); ++it ) {
			ASSERT_FALSE(m.find(it->first) == m.end());
			ASSERT_TRUE(m.find(it->first)->second == it->second);
		}
	}
};

TEST_F(MapLatticeTest, Assign) {
	EXPECT_EQ(0, mapl->size());
	mapl->assign(map1);
	check_equality(map1);
}

TEST_F(MapLatticeTest, MergeByValue) {
	EXPECT_EQ(0, mapl->size());
	mapl->merge(map1);
	check_equality(map1);
	mapl->merge(map2);
	check_equality(map3);
}

TEST_F(MapLatticeTest, MergeByLattice) {
	EXPECT_EQ(0, mapl->size());
	mapl->merge(MapLattice<char, MaxLattice<int>>(map1));
	check_equality(map1);
	mapl->merge(MapLattice<char, MaxLattice<int>>(map2));
	check_equality(map3);
}






