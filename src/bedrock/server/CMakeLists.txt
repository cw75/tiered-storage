cmake_minimum_required (VERSION 2.6)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

SET(ZMQ_DEPENDENCIES
  ../../include/zmq/zmq_util.cc
  ../../include/zmq/zmq_util.h
  ../../include/zmq/socket_cache.cc
  ../../include/zmq/socket_cache.h
)

ADD_EXECUTABLE(server server.cpp join_handler.cpp ${KV_SRC_DEPENDENCIES} ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(server ${KV_LIBRARY_DEPENDENCIES})
