#!/bin/bash

rm -rf vendor/gtest/build
mkdir vendor/gtest/build
cd vendor/gtest/build
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_COMPILER="clang++" -DCMAKE_CXX_FLAGS="-std=c++11 -stdlib=libc++" ..
make
