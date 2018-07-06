ADD_EXECUTABLE(flroute routing.cpp)
TARGET_LINK_LIBRARIES(flroute flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flroute flring zeromq zeromqcpp)