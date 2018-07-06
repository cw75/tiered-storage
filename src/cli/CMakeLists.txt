cmake_minimum_required (VERSION 2.6)

ADD_EXECUTABLE(flcli user.cpp)
TARGET_LINK_LIBRARIES(flcli flring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(flcli zeromq zeromqcpp)
