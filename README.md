# High Performance Lattices

A C++ library that contains high performance lattices that can be composed to build scalable systems. Currently, the library is only tested under Ubuntu/Linux and Mac OS X.

## Build Instructions:

1. Download and install llvm and Clang (http://llvm.org/docs/GettingStarted.html). Clang/Clang++ is assumed to reside in `/usr/bin/`.
2. Download and install zeromq, which is used for message passing. The instruction for installing zmq can be found here (http://zeromq.org/intro:get-the-software). Note that for Mac OS X, after building zmq, you may have to manually rename `libzmq-static.a` in `/path-to-libzmq/path-to-build-directory/lib/` to `libzmq.a` and put it in `/usr/local/lib/`. Otherwise, clang may generate 'library not found' error while compiling.
3. Download and install Google protobuf (https://github.com/google/protobuf).
4. Run `sh ./scripts/build_googletest.sh` to build Google Test.
5. Run `sh ./scripts/build_googlebenchmark.sh` to build Google Benchmark.
6. Download and install Intel TBB. For Mac OS X, run `brew install tbb`. For Ubuntu/Linux, run `apt-get install libtbb-dev`. Note that you may have to run the above commands with `sudo`.

To build the library in debug mode, run `sh ./scripts/build_debug.sh`.<br />
To build the library in release mode, run `sh ./scripts/build_release.sh`.

## Core Lattices:

Core lattices can be composed to build high performance system components. Built-in core lattices can be found in `./include/core_lattices.h`.

To run the core lattice tests, run `./build/tests/run_lattice_test`.<br />
To run the core lattice benchmarks, run `./build/benchmarks/run_lattice_benchmark`.<br />

## Versioned Key-value Store:

This repo currently provide an implementation of single node versioned key-value stored built with lattice composition. To efficiently utilize multicore, the key-value store is build with multiple threads, each of which responsible for a replica. Replica synchronization is done by asynchronously gossiping updates between threads. The source code is located in `./kv_store`. Note that although the current tests, benchmarks, and demo are based on the versioned key-value store, any type of eventually consistent key-value store could easily be implemented using the key-value store template located in `./kv_store/include/base_kv_store.h`.

To run the versioned KVS tests, run `./build/kv_store/tests/run_kvs_test`.<br />
To run the versioned KVS benchmarks, run `./build/kv_store/benchmarks/run_kvs_benchmark`.

We also implemented a demo for the versioned key-value store. To run the demo,

1. Start the server by running `./build/kv_store/versioned_kvs/kvs_server_distributed`.
2. Start the client by running `./build/kv_store/versioned_kvs/kvs_client`.
3. Start the message broker between client and server by running `./build/kv_store/versioned_kvs/msgqueue`.

The order of 1,2 and 3 should not matter. Meanwhile, the only accepted input formats for the client side are `GET $key` and `PUT $key $value`. For this demo, both the key type and the value type have to be integer.
