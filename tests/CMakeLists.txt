cmake_minimum_required (VERSION 2.6)

add_executable(run_lattice_test run_test.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
   # Mac OS X specific code
   target_link_libraries (run_lattice_test ${PROJECT_SOURCE_DIR}/vendor/gtest/build/libgtest.dylib)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # Linux specific code
   target_link_libraries (run_lattice_test ${PROJECT_SOURCE_DIR}/vendor/gtest/build/libgtest.so)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

