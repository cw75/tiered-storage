# High Performance Lattices

A C++ library that contains high performance lattices that can be composed to build scalable systems. Currently, the library is only tested under Ubuntu/Linux and Mac OS X.

Build Instructions:

1. Download and install llvm and Clang. Clang is assumed to reside in `/usr/bin/`.
2. Run `sh ./scripts/build_googletest.sh` to build Google Test.
3. Run `sh ./scripts/build_googlebenchmark.sh` to build Google Benchmark.
3. Download and install Intel TBB. For Mac OS X, run `brew install tbb`. For Ubuntu/Linux, run `apt-get install libtbb-dev`.

To build the library in debug mode, run `sh ./scripts/build_debug.sh`.<br />
To build the library in release mode, run `sh ./scripts/build_release.sh`.

To run the core lattice tests, run `./build/tests/run_lattice_test`.<br />
To run the core lattice benchmarks, run `./build/benchmarks/run_lattice_benchmark`.<br />
To run the versioned KVS tests, run `./build/kv_store/tests/run_kvs_test`.
