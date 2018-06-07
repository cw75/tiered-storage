CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(yamlcpp
    URL "https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.2.tar.gz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    INSTALL_DIR "${CMAKE_BINARY_DIR}/"
    CMAKE_ARGS -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_COMPILER='/usr/bin/clang++' -DCMAKE_CXX_FLAGS='-stdlib=libc++'
    BUILD_COMMAND make
    INSTALL_COMMAND ""
)

set(YAMLCPP_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp/include PARENT_SCOPE)
set(YAMLCPP_LINK_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp PARENT_SCOPE)