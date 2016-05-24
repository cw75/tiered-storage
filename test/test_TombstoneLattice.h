#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

typedef unordered_map<char, BoolLattice> charBoolMap;

class TombstoneLatticeTest : public ::testing::Test {
protected:
	TombstoneLattice<char>* tl;
	charBoolMap map1 = {{'a', BoolLattice(false)}, {'b', BoolLattice(false)}};
	charBoolMap map2 = {{'b', BoolLattice(true)}, {'c', BoolLattice(false)}};
	charBoolMap map3 = {{'a', BoolLattice(false)}, {'b', BoolLattice(true)}, {'c', BoolLattice(false)}};
	TombstoneLatticeTest() {
		tl = new TombstoneLattice<char>;
	}
	virtual ~TombstoneLatticeTest() = default;
	void check_equality(charBoolMap m) {
		EXPECT_EQ(m.size(), tl->size());
		charBoolMap result = tl->reveal();
		for ( auto it = result.begin(); it != result.end(); ++it ) {
			ASSERT_FALSE(m.find(it->first) == m.end());
			ASSERT_TRUE(m.find(it->first)->second == it->second);
		}
	}
};

TEST_F(TombstoneLatticeTest, Assign) {
	EXPECT_EQ(0, tl->size());
	tl->assign(map1);
	check_equality(map1);
}

TEST_F(TombstoneLatticeTest, MergeByValue) {
	EXPECT_EQ(0, tl->size());
	tl->merge(map1);
	EXPECT_EQ(map1.size(), tl->size());
	check_equality(map1);
	tl->merge(map2);
	check_equality(map3);
}

TEST_F(TombstoneLatticeTest, MergeByLattice) {
	EXPECT_EQ(0, tl->size());
	tl->merge(TombstoneLattice<char>(map1));
	check_equality(map1);
	tl->merge(TombstoneLattice<char>(map2));
	check_equality(map3);
}

TEST_F(TombstoneLatticeTest, Insertion) {
	tl->insert('a');
	tl->insert('b');
	check_equality(map1);
}

TEST_F(TombstoneLatticeTest, Deletion) {
	tl->insert('a');
	tl->insert('b');
	tl->insert('c');
	tl->remove('b');
	check_equality(map3);
}










