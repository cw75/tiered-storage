CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER message.proto)

add_executable(msgqueue msgqueue.cpp)
target_link_libraries(msgqueue ${kv_library_dependencies})

add_executable(kvs_client kvs_client.cpp ${PROTO_SRC})
target_link_libraries(kvs_client ${kv_library_dependencies})

add_executable(kvs_server_distributed kvs_server_distributed.cpp ${PROTO_SRC})
target_link_libraries(kvs_server_distributed ${kv_library_dependencies})
