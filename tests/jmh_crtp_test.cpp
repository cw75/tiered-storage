#include <iostream>
#include "crtp_core_lattices.h"

int main() 
{
	MaxLattice<int> m1, m2, m3;
	MinLattice<int64_t> n1, n2;
	// SetLattice<int> *s1, *s2;
	// unordered_set<int> set1 = {1,2,3};
	BoolLattice b1;

	m1.assign(2);
	m2.assign(3);
	m3 = m1;
	m1.merge(m2);

	n1.assign(2);
	n2.assign(3);

	// s1 = new SetLattice<int>;
	// s1->insert(2);
	// // s2->insert(3);

	// m1.merge(4);
	printf("max is %d\n", m1.reveal());
	printf("m3 is %d\n", m3.reveal());
	printf("max.gt(m3) is %d\n", m1.gt(m3.reveal()).reveal());
	printf("min is %lld\n", n1.reveal());
	printf("n2 is %lld\n", n2.reveal());
	printf("min.lt(n2) is %d\n", n1.lt(n2.reveal()).reveal());
}