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

FILE(GLOB_RECURSE ALL_SOURCES RELATIVE ${CMAKE_SOURCE_DIR}/src "../src/*.cpp")
FILE(GLOB MOCK_SOURCES "${CMAKE_SOURCE_DIR}/tests/mock/*.cpp")
FILE(GLOB_RECURSE HANDLER_SOURCES RELATIVE "${CMAKE_SOURCE_DIR}/tests" "../src/*handler.cpp")
FILE(GLOB_RECURSE UTIL_SOURCES RELATIVE "${CMAKE_SOURCE_DIR}/tests"
  "../src/*utils.cpp" "../src/util.cpp")

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src/include)

ADD_EXECUTABLE(test_all
  test_all.cpp
  ${MOCK_SOURCES}
  ${HANDLER_SOURCES}
  ${UTIL_SOURCES}
)

TARGET_LINK_LIBRARIES(test_all gtest gmock pthread flring zmq)
ADD_DEPENDENCIES(test_all gtest)

ADD_SUBDIRECTORY(include)
ADD_SUBDIRECTORY(bedrock)
