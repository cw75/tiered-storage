ADD_EXECUTABLE(flmon monitoring.cpp)
TARGET_LINK_LIBRARIES(flmon flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flmon zeromq zeromqcpp)