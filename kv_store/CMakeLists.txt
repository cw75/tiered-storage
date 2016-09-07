CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

SET(kv_library_dependencies pthread tbb zmq protobuf)

# ADD_SUBDIRECTORY(tests)
# ADD_SUBDIRECTORY(benchmarks)
# ADD_SUBDIRECTORY(versioned_kvs)
# ADD_SUBDIRECTORY(ruc_kvs)
# ADD_SUBDIRECTORY(rc_kvs)
ADD_SUBDIRECTORY(ici_kvs)
