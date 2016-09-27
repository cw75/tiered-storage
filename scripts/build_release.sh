#! /bin/bash

rm -rf build
export CC="clang"
export CXX="clang++"
cmake -DCMAKE_BUILD_TYPE=Release -H. -Bbuild
cmake --build build
