cmake_minimum_required (VERSION 2.6)
project (Lattice)

SET (PROJECT_SOURCE_DIR src)
SET (CMAKE_CXX_COMPILER /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/clang++)
SET (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -O0 --coverage -ltbb -std=c++0x")
SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ltbb -std=c++0x")
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

#if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  #message(${CMAKE_CXX_COMPILER_ID})
  #SET(CMAKE_CXX_FLAGS "-g -fsanitize=address -o0 -ltbb -std=c++0x --coverage")
  #SET(CMAKE_EXE_LINKER_FLAGS "-fprofile-arcs -ftest-coverage")
#endif()

message("flags are ${CMAKE_CXX_FLAGS_RELEASE}")
message("flags are ${CMAKE_CXX_FLAGS_DEBUG}")
#message(">    ${PROJECT_SOURCE_DIR}")
#message(">    ${CMAKE_CXX_COMPILER}")