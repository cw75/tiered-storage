CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)

add_executable(versioned_kvs_client kvs_client.cpp ${PROTO_SRC})
target_link_libraries(versioned_kvs_client ${KV_LIBRARY_DEPENDENCIES})

add_executable(versioned_kvs_server_distributed kvs_server_distributed.cpp ${PROTO_SRC})
target_link_libraries(versioned_kvs_server_distributed ${KV_LIBRARY_DEPENDENCIES})
