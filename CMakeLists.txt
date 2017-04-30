cmake_minimum_required (VERSION 2.6)
project (High-Performance-Lattice)

SET (CMAKE_CXX_COMPILER /usr/bin/clang++)
SET (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -O0 --coverage -lpthread -ltbb -lzmq -lprotobuf -std=c++0x -stdlib=libc++")
SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -lpthread -ltbb -lzmq -lprotobuf -std=c++0x -stdlib=libc++")

find_package(Boost 1.59 REQUIRED)

INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

include_directories (
	/usr/local/lib
	/usr/local/include
	vendor/gbenchmark/include
	vendor/gtest/include
	vendor/tbb/include
	include
	kv_store/include
)

#add_subdirectory(tests)
#add_subdirectory(benchmarks)
add_subdirectory(kv_store)

