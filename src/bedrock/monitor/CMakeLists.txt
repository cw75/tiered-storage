CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

set(MONITORING_SOURCE 
		monitoring.cpp
		membership_handler.cpp
		depart_done_handler.cpp
		feedback_handler.cpp
		metadata_helpers.cpp
		stats_helpers.cpp
		replication_helpers.cpp
		elasticity.cpp
		storage_policy.cpp
		movement_policy.cpp
		slo_policy.cpp)

ADD_EXECUTABLE(flmon ${MONITORING_SOURCE})
TARGET_LINK_LIBRARIES(flmon flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flmon zeromq zeromqcpp)
