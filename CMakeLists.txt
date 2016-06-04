cmake_minimum_required (VERSION 2.6)
project (High-Performance-Lattice)

SET (PROJECT_SOURCE_DIR src)
SET (CMAKE_CXX_COMPILER /usr/bin/clang++)
SET (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -O0 --coverage -lpthread -ltbb -std=c++0x")
SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -lpthread -ltbb -std=c++0x")

include_directories (
	/usr/local/include
	${PROJECT_SOURCE_DIR}
	test
	benchmark
)

add_executable(run_kvs kv_store/kv_store.cpp)
add_executable(run_test test/run_test.cpp)
add_executable(run_benchmark benchmark/run_benchmark.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
   # Mac OS X specific code
   target_link_libraries (run_test /usr/local/lib/libgtest.dylib)
   target_link_libraries (run_benchmark /usr/local/lib/libbenchmark.dylib)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # Linux specific code
   target_link_libraries (run_test /usr/local/lib/libgtest.so)
   target_link_libraries (run_benchmark /usr/local/lib/libbenchmark.so)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")



