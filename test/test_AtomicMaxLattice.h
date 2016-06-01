#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include "gtest/gtest.h"
#include "core_lattices.h"

template <typename T>
class AtomicMaxLatticeTest : public ::testing::Test {
protected:
	AtomicMaxLattice<T>* aml;
	AtomicMaxLatticeTest() {
		aml = new AtomicMaxLattice<T>;
	}
	virtual ~AtomicMaxLatticeTest() {
		delete aml;
	}
};

typedef ::testing::Types<int, float, double> AtomicMaxTypes;
TYPED_TEST_CASE(AtomicMaxLatticeTest, AtomicMaxTypes);

template <typename T>
void AtomicMaxMerge(AtomicMaxLattice<T>* p, atomic<bool> &signal, int index, int num_thread) {
	while(!signal){
		std::this_thread::yield();
	}
	for (int i = index; i < 1000000; i += num_thread) {
		p->merge(i);
	}
}

TYPED_TEST(AtomicMaxLatticeTest, ThreadSafe) {
	EXPECT_EQ(0, this->aml->reveal());
	vector<thread> threads;
	atomic<bool> go(false);
	for (int i = 0; i < 10; i += 1) {
		threads.push_back(thread(AtomicMaxMerge<TypeParam>, this->aml, ref(go), i, 10));
	}
	go.store(true);
	for (auto& th : threads) th.join();
	EXPECT_EQ(999999, this->aml->reveal());
}


