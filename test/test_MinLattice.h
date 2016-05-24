#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

template <typename T>
class MinLatticeTest : public ::testing::Test {
protected:
	MinLattice<T>* ml;
	MinLatticeTest() {
		ml = new MinLattice<T>;
	}
	virtual ~MinLatticeTest() = default;
};

typedef ::testing::Types<int, float, double> MinTypes;
TYPED_TEST_CASE(MinLatticeTest, MinTypes);

TYPED_TEST(MinLatticeTest, Assign) {
	EXPECT_EQ(static_cast<TypeParam> (1000000), this->ml->reveal());
	this->ml->assign(10);
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->assign(5);
	EXPECT_EQ(5, this->ml->reveal());
}

TYPED_TEST(MinLatticeTest, MergeByValue) {
	EXPECT_EQ(static_cast<TypeParam> (1000000), this->ml->reveal());
	this->ml->merge(10);
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->merge(20);
	EXPECT_EQ(10, this->ml->reveal());
}

TYPED_TEST(MinLatticeTest, MergeByLattice) {
	EXPECT_EQ(static_cast<TypeParam> (1000000), this->ml->reveal());
	this->ml->merge(MinLattice<TypeParam>(10));
	EXPECT_EQ(10, this->ml->reveal());
	this->ml->merge(MinLattice<TypeParam>(20));
	EXPECT_EQ(10, this->ml->reveal());
}