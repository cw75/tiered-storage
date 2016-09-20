// This file instantiates each type of lattice to sanity check that everything
// builds properly. It is a short term hack and should be replaced with unit
// tests

#include <string>

#include "lattices/lattices.h"

namespace lf = latticeflow;

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
  lf::BoolOrLattice bool_or_lattice;
  lf::BoolAndLattice bool_and_lattice;
  lf::MaxLattice<int> max_int_lattice;
  lf::MaxLattice<float> max_float_lattice;
  lf::MaxLattice<std::string> max_string_lattice;
  lf::MinLattice<int> min_int_lattice;
  lf::MinLattice<float> min_float_lattice;
  lf::MinLattice<std::string> min_string_lattice;
  lf::SetUnionLattice<int> set_union_int_lattice;
  lf::SetUnionLattice<float> set_union_float_lattice;
  lf::SetUnionLattice<std::string> set_union_string_lattice;
  lf::SetIntersectLattice<int> set_intersect_int_lattice;
  lf::SetIntersectLattice<float> set_intersect_float_lattice;
  lf::SetIntersectLattice<std::string> set_intersect_string_lattice;
  lf::PairLattice<lf::BoolOrLattice, lf::BoolAndLattice> pair_lattice;
  lf::VectorLattice<lf::BoolOrLattice> vector_lattice;
  lf::TimestampLattice<lf::BoolOrLattice, lf::BoolAndLattice> timestamp_lattice;
  lf::MapLattice<int, lf::BoolOrLattice> map_lattice;

  check_comparisons(bool_or_lattice);
  check_comparisons(bool_and_lattice);
  check_comparisons(max_int_lattice);
  check_comparisons(max_float_lattice);
  check_comparisons(max_string_lattice);
  check_comparisons(min_int_lattice);
  check_comparisons(min_float_lattice);
  check_comparisons(min_string_lattice);
}
