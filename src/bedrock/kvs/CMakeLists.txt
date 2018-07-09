CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

set(KVS_SOURCE 
	server.cpp
	node_join_handler.cpp
  node_depart_handler.cpp
  self_depart_handler.cpp
  user_request_handler.cpp
  gossip_handler.cpp
  rep_factor_response_handler.cpp
  rep_factor_change_handler.cpp
  utils.cpp)

ADD_EXECUTABLE(flkvs ${KVS_SOURCE})
TARGET_LINK_LIBRARIES(flkvs flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flkvs zeromq zeromqcpp)
