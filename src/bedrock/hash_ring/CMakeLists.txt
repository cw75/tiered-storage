CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

SET(ZMQ_DEPENDENCIES
  ../../include/zmq/zmq_util.cpp
  ../../include/zmq/zmq_util.hpp
  ../../include/zmq/socket_cache.cpp
  ../../include/zmq/socket_cache.hpp
)

ADD_LIBRARY(flring STATIC hash_ring.cpp ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(flring flproto ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flring zeromq zeromqcpp)
