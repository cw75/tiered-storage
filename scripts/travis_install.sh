#!/bin/bash

sudo apt-get update
sudo apt-get install -y build-essential autoconf automake libtool curl make unzip pkg-config wget
sudo apt-get install -y libc++-dev libc++abi-dev awscli jq

sudo ln -s $(which clang) /usr/bin/clang
sudo ln -s $(which clang++) /usr/bin/clang++

wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
unzip protobuf-all-3.5.1.zip > /dev/null 2>&1
rm protobuf-all-3.5.1.zip

cd protobuf-3.5.1
./autogen.sh
./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
make -j4
sudo make install
sudo ldconfig
cd ..
