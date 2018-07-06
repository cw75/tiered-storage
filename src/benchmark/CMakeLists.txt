CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

ADD_EXECUTABLE(benchmark benchmark.cpp)
TARGET_LINK_LIBRARIES(benchmark flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(benchmark zeromq zeromqcpp)

ADD_EXECUTABLE(benchmark_trigger benchmark_trigger.cpp)
TARGET_LINK_LIBRARIES(benchmark_trigger flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(benchmark_trigger flring zeromq zeromqcpp)
