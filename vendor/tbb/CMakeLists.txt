CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(tbb
    URL "https://github.com/01org/tbb/archive/2018_U1.tar.gz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND make compiler=clang stdlib=libc++ std=c++11
    INSTALL_COMMAND ""
)

set(TBB_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/include PARENT_SCOPE)
set(TBB_LINK_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/*_release PARENT_SCOPE)
set(TBB_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/build/*_release/libtbb.so PARENT_SCOPE)