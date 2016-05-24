#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "core_lattices.h"

class BoolLatticeTest : public ::testing::Test {
protected:
	BoolLattice* bl;
	BoolLatticeTest() {
		bl = new BoolLattice;
	}
	virtual ~BoolLatticeTest() = default;
};

TEST_F(BoolLatticeTest, Assign) {
	EXPECT_EQ(false, bl->reveal());
	bl->assign(true);
	EXPECT_EQ(true, bl->reveal());
	bl->assign(false);
	EXPECT_EQ(false, bl->reveal());
}

TEST_F(BoolLatticeTest, MergeByValue) {
	EXPECT_EQ(false, bl->reveal());
	bl->merge(true);
	EXPECT_EQ(true, bl->reveal());
	bl->merge(false);
	EXPECT_EQ(true, bl->reveal());
}

TEST_F(BoolLatticeTest, MergeByLattice) {
	EXPECT_EQ(false, bl->reveal());
	bl->merge(BoolLattice(true));
	EXPECT_EQ(true, bl->reveal());
	bl->merge(BoolLattice(false));
	EXPECT_EQ(true, bl->reveal());
}


