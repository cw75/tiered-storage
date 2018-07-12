CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

INCLUDE_DIRECTORIES(include)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ./include/proto/misc.proto
  ./include/proto/replication.proto
  ./include/proto/requests.proto)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

# need to build a target at this level or subdirs won't have the 
# protobuf files generated.
ADD_LIBRARY(flproto ${PROTO_HEADER} ${PROTO_SRC})

# The source dependencies of the key value store clients and servers.
SET(KV_SRC_DEPENDENCIES
    ${PROTO_SRC}
    ${PROTO_HEADER}
)

# The library dependencies of the key value store clients and servers.
SET(KV_LIBRARY_DEPENDENCIES
    protobuf
    flproto
    pthread
    zmq
    yaml-cpp
)

ADD_SUBDIRECTORY(bedrock)
ADD_SUBDIRECTORY(cli)
ADD_SUBDIRECTORY(benchmark)
