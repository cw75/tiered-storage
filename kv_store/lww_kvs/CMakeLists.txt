cmake_minimum_required (VERSION 2.6)

# Generate protocol buffers.
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)

FIND_PACKAGE(TBB)
INCLUDE_DIRECTORIES(${TBB_INCLUDE_DIRS})
ADD_DEFINITIONS(${TBB_DEFINITIONS})

FIND_PACKAGE(CURL)
INCLUDE_DIRECTORIES(${CURL_INCLUDE_DIRS})


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

ADD_EXECUTABLE(kvs_user kvs_user.cpp ../include/zmq_util.cc ../include/zmq_util.h)
TARGET_LINK_LIBRARIES(kvs_user ${KV_LIBRARY_DEPENDENCIES})

ADD_EXECUTABLE(kvs_client kvs_client.cpp ${KV_SRC_DEPENDENCIES} ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_client ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES} ${CURL_LIBRARIES})

ADD_EXECUTABLE(kvs_server kvs_server.cpp ${KV_SRC_DEPENDENCIES} ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_server ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES} ${CURL_LIBRARIES})

ADD_EXECUTABLE(kvs_memory_server kvs_memory_server.cpp ${KV_SRC_DEPENDENCIES} ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
TARGET_LINK_LIBRARIES(kvs_memory_server ${KV_LIBRARY_DEPENDENCIES} ${TBB_LIBRARIES} ${CURL_LIBRARIES})
