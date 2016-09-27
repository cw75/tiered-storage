#! /bin/bash

rm -rf build
export CC="clang"
export CXX="clang++"
cmake -DCMAKE_BUILD_TYPE=Debug -H. -Bbuild
cmake --build build
