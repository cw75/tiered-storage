# Tiered Storage

A cloud-native, elastic, tiered KVS.

##Note: some of the build instruction below are out-dated. We plan to officially open-source the repo in the next two months.

## Build Instructions:

1. Install Clang and libc++.
On Ubuntu, run:<br />
`sudo apt-get install clang-4.0`.<br />
`sudo apt-get install libc++-dev libc++abi-dev`.<br />
Clang/Clang++ is assumed to reside in `/usr/bin/`. Therefore, run:<br />
`sudo ln -s "$(which clang-4.0)” /usr/bin/clang`.<br />
`sudo ln -s "$(which clang++-4.0)" /usr/bin/clang++`.
2. Download and install Google protobuf (https://github.com/google/protobuf).

To build the KVS in debug mode, run `bash ./scripts/build_debug.sh`.<br />
To build the KVS in release mode, run `bash ./scripts/build_release.sh`.<br />

To run the KVS,

1. Start a storage node by running `./build/kv_store/lww_kvs/kvs_server`.
2. Start a proxy node by running `./build/kv_store/lww_kvs/kvs_proxy`.
3. Start the monitoring node by running `./build/kv_store/lww_kvs/kvs_monitoring`.
4. Start a benchmark node by running `./build/kv_store/lww_kvs/kvs_benchmark`.

Meanwhile, the accepted input formats are `GET $key` and `PUT $key $value`.
