# Resources:
#   - http://www.kaizou.org/2014/11/gtest-cmake/
#   - http://bit.ly/2cx70Pk
#   - https://github.com/snikulov/google-test-examples

CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

include(ExternalProject)

set(GOOGLETEST_CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++")

ExternalProject_Add(googletest
    GIT_REPOSITORY "https://github.com/google/googletest"
    GIT_TAG "release-1.8.0"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=clang++
    		   -DCMAKE_CXX_FLAGS=${GOOGLETEST_CMAKE_CXX_FLAGS}
    		   -DBUILD_SHARED_LIBS=1
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(googletest SOURCE_DIR)
set(GTEST_INCLUDE_DIRS ${SOURCE_DIR}/googletest/include PARENT_SCOPE)

ExternalProject_Get_Property(googletest BINARY_DIR)
set(GTEST_LIBS_DIR ${BINARY_DIR}/googlemock/gtest PARENT_SCOPE)
