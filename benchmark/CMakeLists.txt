cmake_minimum_required (VERSION 2.6)

add_executable(run_benchmark run_benchmark.cpp)

IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
   # Mac OS X specific code
   target_link_libraries (run_benchmark /usr/local/lib/libbenchmark.dylib)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # Linux specific code
   target_link_libraries (run_benchmark /usr/local/lib/libbenchmark.so)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")