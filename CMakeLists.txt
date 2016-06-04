cmake_minimum_required (VERSION 2.6)
project (High-Performance-Lattice)

SET (PROJECT_SOURCE_DIR include)
SET (CMAKE_CXX_COMPILER /usr/bin/clang++)
SET (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -O0 --coverage -lpthread -ltbb -std=c++0x")
SET (CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -lpthread -ltbb -std=c++0x")

include_directories (
	/usr/local/include
	${PROJECT_SOURCE_DIR}
)

add_subdirectory(test)
add_subdirectory(benchmark)
add_subdirectory(kv_store)



