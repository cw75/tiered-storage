#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

template <typename T>
class MaxLatticeTest : public ::testing::Test {
protected:
	MaxLattice<T>* ml;
	MaxLatticeTest() {
		ml = new MaxLattice<T>;
	}
	virtual ~MaxLatticeTest() = default;
};

typedef ::testing::Types<int, float, double> MaxTypes;
TYPED_TEST_CASE(MaxLatticeTest, MaxTypes);

TYPED_TEST(MaxLatticeTest, Assign) {
	EXPECT_EQ(0, this->ml->reveal());
	this->ml->assign(10);
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->assign(5);
	EXPECT_EQ(5, this->ml->reveal());
}

TYPED_TEST(MaxLatticeTest, MergeByValue) {
	EXPECT_EQ(0, this->ml->reveal());
	this->ml->merge(10);
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->merge(5);
	EXPECT_EQ(10, this->ml->reveal());
}

TYPED_TEST(MaxLatticeTest, MergeByLattice) {
	EXPECT_EQ(0, this->ml->reveal());
	this->ml->merge(MaxLattice<TypeParam>(10));
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->merge(MaxLattice<TypeParam>(5));
	EXPECT_EQ(10, this->ml->reveal());
}








