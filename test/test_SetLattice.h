#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

class SetLatticeTest : public ::testing::Test {
protected:
	SetLattice<char>* sl;
	unordered_set<char> set1 = {'a', 'b', 'c'};
	unordered_set<char> set2 = {'c', 'd', 'e'};
	unordered_set<char> set3 = {'a', 'd', 'e', 'b', 'c'};
	SetLatticeTest() {
		sl = new SetLattice<char>;
	}
	virtual ~SetLatticeTest() = default;
};

TEST_F(SetLatticeTest, Assign) {
	EXPECT_EQ(0, sl->size());
	sl->assign(set1);
	EXPECT_EQ(3, sl->size());
	EXPECT_EQ(set1, sl->reveal());
}

TEST_F(SetLatticeTest, MergeByValue) {
	EXPECT_EQ(0, sl->size());
	sl->merge(set1);
	EXPECT_EQ(3, sl->size());
	EXPECT_EQ(set1, sl->reveal());
	sl->merge(set2);
	EXPECT_EQ(5, sl->size());
	EXPECT_EQ(set3, sl->reveal());
}

TEST_F(SetLatticeTest, MergeByLattice) {
	EXPECT_EQ(0, sl->size());
	sl->merge(SetLattice<char>(set1));
	EXPECT_EQ(3, sl->size());
	EXPECT_EQ(set1, sl->reveal());
	sl->merge(SetLattice<char>(set2));
	EXPECT_EQ(5, sl->size());
	EXPECT_EQ(set3, sl->reveal());
}

