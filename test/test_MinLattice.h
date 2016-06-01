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
	virtual ~MinLatticeTest() {
		delete ml;
	}
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

TYPED_TEST(MinLatticeTest, Lt) {
	BoolLattice bl = this->ml->lt(0);
	EXPECT_EQ(false, bl.reveal());
	this->ml->merge(0);
	bl = this->ml->lt(0);
	EXPECT_EQ(false, bl.reveal());
	this->ml->merge(-1);
	bl = this->ml->lt(0);
	EXPECT_EQ(true, bl.reveal());
}

TYPED_TEST(MinLatticeTest, LtEq) {
	this->ml->merge(0);
	BoolLattice bl = this->ml->lt_eq(0);
	EXPECT_EQ(true, bl.reveal());
}

TYPED_TEST(MinLatticeTest, Add) {
	this->ml->merge(0);
	MinLattice<TypeParam> res = this->ml->add(5);
	EXPECT_EQ(5, res.reveal());
}

TYPED_TEST(MinLatticeTest, Subtract) {
	this->ml->merge(0);
	MinLattice<TypeParam> res = this->ml->subtract(5);
	EXPECT_EQ(-5, res.reveal());
}