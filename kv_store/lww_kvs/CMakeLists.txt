cmake_minimum_required (VERSION 2.6)

add_executable(kvs_client kvs_client.cpp message.pb.cc ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)
add_executable(kvs_server kvs_server.cpp message.pb.cc ../include/zmq_util.cc ../include/zmq_util.h ../include/socket_cache.cc ../include/socket_cache.h)