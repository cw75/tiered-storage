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

ADD_EXECUTABLE(run_server_handler_test
			   run_server_handler_test.cpp
			   ${CMAKE_SOURCE_DIR}/tests/mocked.cpp
			   ${CMAKE_SOURCE_DIR}/src/bedrock/kvs/node_join_handler.cpp
			   ${CMAKE_SOURCE_DIR}/src/bedrock/kvs/node_depart_handler.cpp
			   ${CMAKE_SOURCE_DIR}/src/bedrock/kvs/self_depart_handler.cpp
			   ${CMAKE_SOURCE_DIR}/src/bedrock/kvs/utils.cpp)
TARGET_LINK_LIBRARIES (run_server_handler_test ${GTEST_LIBS_DIR}/libgtest.so pthread flring zmq)
ADD_DEPENDENCIES(run_server_handler_test googletest)