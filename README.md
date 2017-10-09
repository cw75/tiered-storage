# Tiered Storage

An elastic, tiered KVS.

## Build Instructions:

1. Download and install llvm and Clang (http://llvm.org/docs/GettingStarted.html). Clang/Clang++ is assumed to reside in `/usr/bin/`.
2. Download and install zeromq, which is used for message passing. The instruction for installing zmq can be found here (http://zeromq.org/intro:get-the-software). Note that for Mac OS X, after building zmq, you may have to manually rename `libzmq-static.a` in `/path-to-libzmq/path-to-build-directory/lib/` to `libzmq.a` and put it in `/usr/local/lib/`. Otherwise, clang may generate 'library not found' error while compiling.
3. Download and install Google protobuf (https://github.com/google/protobuf).
4. Run `sh ./scripts/build_googletest.sh` to build Google Test.
5. Run `sh ./scripts/build_googlebenchmark.sh` to build Google Benchmark.
6. Download and install Intel TBB. For Mac OS X, run `brew install tbb`. For Ubuntu/Linux, run `apt-get install libtbb-dev`. Note that you may have to run the above commands with `sudo`.

To build the KVS in debug mode, run `sh ./scripts/build_debug.sh`.<br />
To build the KVS in release mode, run `sh ./scripts/build_release.sh`.

To run the KVS,

1. Start the server by running `./build/kv_store/lww_kvs/kvs_server <server_ip_addr> n`.
2. Start the client by running `./build/kv_store/lww_kvs/kvs_client <client_ip_addr>`.

Meanwhile, the accepted input formats are `GET $key` and `PUT $key $value`.
