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

SET(ZMQ_DEPENDENCIES
  ../../include/zmq/zmq_util.cpp
  ../../include/zmq/zmq_util.hpp
  ../../include/zmq/socket_cache.cpp
  ../../include/zmq/socket_cache.hpp
)

ADD_LIBRARY(flring STATIC hash_ring.cpp ${ZMQ_DEPENDENCIES})
TARGET_LINK_LIBRARIES(flring flproto ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flring zeromq zeromqcpp)
