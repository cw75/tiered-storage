# Tiered Storage

An elastic, tiered KVS.

## Build Instructions:

1. Download and install llvm and [Clang](http://llvm.org/docs/GettingStarted.html). Clang/Clang++ is assumed to reside in `/usr/bin/`.
2. Download and install [zeromq](http://zeromq.org/intro:get-the-software), which is used for message passing. Note that for Mac OS X, after building zmq, you may have to manually rename `libzmq-static.a` in `/path-to-libzmq/path-to-build-directory/lib/` to `libzmq.a` and put it in `/usr/local/lib/`. Otherwise, Clang may generate a 'library not found' error while compiling.
3. Download and install Google's [ProtoBuf](https://github.com/google/protobuf).
4. Install [Boost](http://www.boost.org/doc/libs/1_65_1/more/getting_started/unix-variants.html). If you are on Ubuntu, make sure to remove existing (likely older) versions of Boost by running `sudo apt-get -y --purge remove libboost-all-dev libboost-doc libboost-dev` followed by `sudo apt-get autoremove`.
4. Run `sh ./scripts/build_googletest.sh` to build Google Test.
5. Run `sh ./scripts/build_googlebenchmark.sh` to build Google Benchmark.
6. Download and install Intel TBB. For Mac OS X, run `brew install tbb`. For Ubuntu/Linux, run `apt-get install libtbb-dev`. Note that you may have to run the above commands with `sudo`.

To build the KVS in debug mode, run `sh ./scripts/build_debug.sh`.

To build the KVS in release mode, run `sh ./scripts/build_release.sh`.

To run the KVS,

1. Start the server by running `./build/kv_store/lww_kvs/kvs_server <server_ip_addr> n`.
2. Start the client by running `./build/kv_store/lww_kvs/kvs_client <client_ip_addr>`.

Meanwhile, the accepted input formats are `GET $key` and `PUT $key $value`.
