CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

set(ROUTING_SOURCE 
		routing.cpp
		seed_handler.cpp
		membership_handler.cpp
		replication_response_handler.cpp
		replication_change_handler.cpp
		address_handler.cpp)

ADD_EXECUTABLE(flroute ${ROUTING_SOURCE})
TARGET_LINK_LIBRARIES(flroute flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flroute flring zeromq zeromqcpp)
