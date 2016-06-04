cmake_minimum_required (VERSION 2.6)
project (High-Performance-Lattice)

SET (CMAKE_CXX_COMPILER /usr/bin/clang++)
SET (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -O0 --coverage -lpthread -ltbb -std=c++0x")
SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -lpthread -ltbb -std=c++0x")

include_directories (
	vendor/gbenchmark/include
	vendor/gtest/include
	vendor/tbb/include
	include
)

add_subdirectory(tests)
add_subdirectory(benchmarks)
add_subdirectory(kv_store)

