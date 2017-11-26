CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

include(ExternalProject)

ExternalProject_Add(tbb
    URL "https://github.com/01org/tbb/releases/download/2018_U1/tbb2018_20170919oss_lin.tgz"
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}
    BUILD_IN_SOURCE 1
    UPDATE_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

set(TBB_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/include PARENT_SCOPE)
set(TBB_LINK_DIRS ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/lib/intel64/gcc4.7 PARENT_SCOPE)
set(TBB_LIBRARIES ${CMAKE_CURRENT_BINARY_DIR}/src/tbb/lib/intel64/gcc4.7/libtbb.so PARENT_SCOPE)