cmake_minimum_required (VERSION 2.6)

# Generate protocol buffers.
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)

FIND_PACKAGE(TBB)
include_directories(${TBB_INCLUDE_DIRS})
add_definitions(${TBB_DEFINITIONS})

# The library dependencies of the key value store clients and servers.
SET(KV_LIBRARY_DEPENDENCIES
    protobuf
    pthread
    zmq
)

# The source dependencies of the key value store clients and servers.
SET(KV_SRC_DEPENDENCIES
    ${PROTO_SRC}
)

ADD_EXECUTABLE(kvs_client kvs_client.cpp ${KV_SRC_DEPENDENCIES} ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_client ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES})

ADD_EXECUTABLE(kvs_server kvs_server.cpp ${KV_SRC_DEPENDENCIES} ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_server ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES})

CONFIGURE_FILE(server_address.txt . COPYONLY)
CONFIGURE_FILE(seed_address.txt . COPYONLY)
CONFIGURE_FILE(client_address.txt . COPYONLY)
