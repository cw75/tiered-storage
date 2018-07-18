#  Copyright 2018 U.C. Berkeley RISE Lab
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

CMAKE_MINIMUM_REQUIRED(VERSION 3.6)
PROJECT(Bedrock)

SET(BEDROCK_VERSION_MAJOR 0)
SET(BEDROCK_VERSION_MINOR 1)
SET(BEDROCK_VERSION_PATCH 0)

SET(CMAKE_CXX_COMPILER /usr/bin/clang++)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED on)

SET(VENDOR_DIR vendor)

SET(CMAKE_CXX_FLAGS_COMMON
    "-std=c++11 \
    -stdlib=libc++")

SET(CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} \
    ${CMAKE_CXX_FLAGS_COMMON}")

SET(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE} \
    ${CMAKE_CXX_FLAGS_COMMON} \
    -O3")

ADD_SUBDIRECTORY(${VENDOR_DIR}/spdlog)
ADD_SUBDIRECTORY(${VENDOR_DIR}/yamlcpp)
ADD_SUBDIRECTORY(${VENDOR_DIR}/googletest)
ADD_SUBDIRECTORY(${VENDOR_DIR}/zeromq)
ADD_SUBDIRECTORY(${VENDOR_DIR}/zeromqcpp)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(.)
INCLUDE_DIRECTORIES(${GTEST_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${ZEROMQCPP_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${ZEROMQ_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${YAMLCPP_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${SPDLOG_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(src/include)
INCLUDE_DIRECTORIES(tests)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ./src/include/proto/misc.proto
  ./src/include/proto/replication.proto
  ./src/include/proto/requests.proto)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

# need to build a target at this level or subdirs won't have the 
# protobuf files generated.
ADD_LIBRARY(flproto ${PROTO_HEADER} ${PROTO_SRC})

INCLUDE(cmake/clang-format.cmake)

LINK_DIRECTORIES(${ZEROMQ_LINK_DIRS})
LINK_DIRECTORIES(${YAMLCPP_LINK_DIRS})

ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(tests)
