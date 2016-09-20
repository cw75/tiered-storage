// This file instantiates each type of lattice to sanity check that everything
// builds properly. It is a short term hack and should be replaced with unit
// tests

#include <string>

#include "lattices/lattices.h"

template <typename T>
void ignore(const T& x) {
  (void)x;
}

template <typename T>
void check_comparisons(const T& x) {
  ignore(x < x);
  ignore(x <= x);
  ignore(x > x);
  ignore(x >= x);
  ignore(x == x);
  ignore(x != x);
}

int main() {
  BoolOrLattice bool_or_lattice;
  BoolAndLattice bool_and_lattice;
  MaxLattice<int> max_int_lattice;
  MaxLattice<float> max_float_lattice;
  MaxLattice<std::string> max_string_lattice;
  MinLattice<int> min_int_lattice;
  MinLattice<float> min_float_lattice;
  MinLattice<std::string> min_string_lattice;
  SetUnionLattice<int> set_union_int_lattice;
  SetUnionLattice<float> set_union_float_lattice;
  SetUnionLattice<std::string> set_union_string_lattice;
  SetIntersectLattice<int> set_intersect_int_lattice;
  SetIntersectLattice<float> set_intersect_float_lattice;
  SetIntersectLattice<std::string> set_intersect_string_lattice;
  PairLattice<BoolOrLattice, BoolAndLattice> pair_lattice;
  VectorLattice<BoolOrLattice> vector_lattice;
  TimestampLattice<BoolOrLattice, BoolAndLattice> timestamp_lattice;
  MapLattice<int, BoolOrLattice> map_lattice;

  check_comparisons(bool_or_lattice);
  check_comparisons(bool_and_lattice);
  check_comparisons(max_int_lattice);
  check_comparisons(max_float_lattice);
  check_comparisons(max_string_lattice);
  check_comparisons(min_int_lattice);
  check_comparisons(min_float_lattice);
  check_comparisons(min_string_lattice);
}
