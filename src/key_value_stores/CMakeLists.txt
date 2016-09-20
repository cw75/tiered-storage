CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

# Generate protocol buffers.
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)

# The library dependencies of the key value store clients and servers.
SET(KV_LIBRARY_DEPENDENCIES
    protobuf
    pthread
    tbb
    zmq
)

# The source dependencies of the key value store clients and servers.
SET(KV_SRC_DEPENDENCIES
    ${PROTO_SRC}
    barrier.cc
    util.cc
    zmq_util.cc
)

# The message broker that forwards traffic between clients and servers.
ADD_EXECUTABLE(msgqueue msgqueue.cc)
TARGET_LINK_LIBRARIES(msgqueue ${KV_LIBRARY_DEPENDENCIES})

# Read uncommitted client.
ADD_EXECUTABLE(ruc_client ruc_client.cc ${KV_SRC_DEPENDENCIES})
TARGET_LINK_LIBRARIES(ruc_client ${KV_LIBRARY_DEPENDENCIES})

# Read uncommitted server.
ADD_EXECUTABLE(ruc_server ruc_server.cc ${KV_SRC_DEPENDENCIES})
TARGET_LINK_LIBRARIES(ruc_server ${KV_LIBRARY_DEPENDENCIES})
