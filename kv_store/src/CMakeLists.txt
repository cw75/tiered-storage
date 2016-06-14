cmake_minimum_required (VERSION 2.6)

add_executable(msgqueue msgqueue.cpp)
add_executable(kvs_client kvs_client.cpp request.pb.cc response.pb.cc)
add_executable(kvs_server_distributed kvs_server_distributed.cpp request.pb.cc response.pb.cc)