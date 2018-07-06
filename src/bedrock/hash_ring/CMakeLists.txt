SET(ZMQ_DEPENDENCIES
  ../../include/zmq/zmq_util.cpp
  ../../include/zmq/zmq_util.hpp
  ../../include/zmq/socket_cache.cpp
  ../../include/zmq/socket_cache.hpp
)

# avoid cmake error by letting it know the protobuf files will
# be generated at make time
# SET_SOURCE_FILES_PROPERTIES(${KV_SRC_DEPENDENCIES} PROPERTIES
                            # GENERATED TRUE)
ADD_LIBRARY(flring STATIC hash_ring.cpp ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(flring flproto ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flring zeromq zeromqcpp)
