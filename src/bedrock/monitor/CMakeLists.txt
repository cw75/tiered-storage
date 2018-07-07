CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

set(MONITORING_SOURCE 
		monitoring.cpp
		membership_handler.cpp
		depart_done_handler.cpp
		feedback_handler.cpp
		monitoring_utils.cpp
		policy_algorithm.cpp)

ADD_EXECUTABLE(flmon ${MONITORING_SOURCE})
TARGET_LINK_LIBRARIES(flmon flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flmon zeromq zeromqcpp)
