cmake_minimum_required (VERSION 2.6)
project (Lattice)

SET (PROJECT_SOURCE_DIR src)

include_directories (
	/usr/local/include
	${PROJECT_SOURCE_DIR}
	test
	benchmark
)

add_executable(run_test test/run_test.cpp)
add_executable(run_benchmark benchmark/run_benchmark.cpp)
target_link_libraries (run_test /usr/local/lib/libgtest.a)
target_link_libraries (run_benchmark /usr/local/lib/libbenchmark.a)

# note: it seems that Google's leak, thread and memory sanitizers are not supported on Mac
#       only the address sanitizer seems to be functioning

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "-fsanitize=address -o2 -std=c++0x")
endif()

#message(">    ${PROJECT_SOURCE_DIR}")
#message(">    ${CMAKE_CXX_COMPILER}")