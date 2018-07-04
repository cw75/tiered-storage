cmake_minimum_required (VERSION 2.6)

# Generate protocol buffers.
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ../include/communication.proto)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

# The library dependencies of the key value store clients and servers.
SET(KV_LIBRARY_DEPENDENCIES
    protobuf
    pthread
    zmq
    yaml-cpp
)

# The source dependencies of the key value store clients and servers.
SET(KV_SRC_DEPENDENCIES
    ${PROTO_SRC}
    ${PROTO_HEADER}
)
SET(ZMQ_DEPENDENCIES
  ../include/zmq/zmq_util.cc
  ../include/zmq/zmq_util.h
  ../include/zmq/socket_cache.cc
  ../include/zmq/socket_cache.h
)

ADD_EXECUTABLE(benchmark benchmark.cpp ${KV_SRC_DEPENDENCIES} ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(benchmark ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(benchmark zeromq zeromqcpp)

ADD_EXECUTABLE(benchmark_trigger benchmark_trigger.cpp ${KV_SRC_DEPENDENCIES} ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(benchmark_trigger ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(benchmark_trigger zeromq zeromqcpp)
