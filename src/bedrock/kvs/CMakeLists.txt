CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

ADD_EXECUTABLE(flkvs server.cpp)
set(KVS_SOURCE 
	server.cpp
	join_handler.cpp)

ADD_EXECUTABLE(flkvs ${KVS_SOURCE})
TARGET_LINK_LIBRARIES(flkvs flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flkvs zeromq zeromqcpp)
