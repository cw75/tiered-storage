CMAKE_MINIMUM_REQUIRED(VERSION 3.6)

ADD_EXECUTABLE(run_lattice_test run_lattice_test.cpp)
TARGET_LINK_LIBRARIES (run_lattice_test ${GTEST_LIBS_DIR}/libgtest.so)
ADD_DEPENDENCIES(run_lattice_test googletest)

ADD_EXECUTABLE(run_server_handler_test
			   run_server_handler_test.cpp 
			   ../src/bedrock/kvs/node_join_handler.cpp
			   ../src/bedrock/kvs/node_depart_handler.cpp
			   ../src/bedrock/kvs/self_depart_handler.cpp
			   ../src/bedrock/kvs/utils.cpp)
TARGET_LINK_LIBRARIES (run_server_handler_test ${GTEST_LIBS_DIR}/libgtest.so pthread flring zmq)
ADD_DEPENDENCIES(run_server_handler_test googletest)