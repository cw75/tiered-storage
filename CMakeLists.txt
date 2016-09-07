CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(High-Performance-Lattice)

SET(CMAKE_CXX_COMPILER /usr/bin/clang++)
SET(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} \
    -fsanitize=address \
    -O0 \
    --coverage \
    -std=c++0x \
    -stdlib=libc++")
SET(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} \
    -std=c++0x \
    -stdlib=libc++")
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR} .)

# ADD_SUBDIRECTORY(benchmarks)
# ADD_SUBDIRECTORY(tests)
ADD_SUBDIRECTORY(kv_store)
