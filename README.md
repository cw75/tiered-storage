# Tiered Storage

An elastic, tiered KVS.

## Build Instructions:

1. Install Clang and libc++.
On Ubuntu, run:<br />
`sudo apt-get install clang-4.0`.<br />
`sudo apt-get install libc++-dev libc++abi-dev`.<br />
Clang/Clang++ is assumed to reside in `/usr/bin/`. Therefore, run:<br />
`sudo ln -s "$(which clang-4.0)” /usr/bin/clang`.<br />
`sudo ln -s "$(which clang++-4.0)" /usr/bin/clang++`.
2. Download and install Google protobuf (https://github.com/google/protobuf).
3. Install Intel TBB. On Mac OS X, run `brew install tbb`. On Ubuntu/Linux, run `apt-get install libtbb-dev`. Note that you may have to run the above commands with `sudo`.
4. Install Curl. On Ubuntu, run `sudo apt-get install libcurl4-openssl-dev`.

To build the KVS in debug mode, run `bash ./scripts/build_debug.sh`.<br />
To build the KVS in release mode, run `bash ./scripts/build_release.sh`.<br />
To clear the EBS storage, run `bash ./scripts/clear_storage.sh`.<br />

To run the KVS,

1. Start the server by running `./build/kv_store/lww_kvs/kvs_server <server_ip_addr> n`.
2. Start the client by running `./build/kv_store/lww_kvs/kvs_client <client_ip_addr>`.

Meanwhile, the accepted input formats are `GET $key` and `PUT $key $value`.
