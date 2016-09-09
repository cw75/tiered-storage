#! /bin/bash

# Some useful links:
# - https://github.com/google/protobuf/blob/master/src/README.md
# - https://github.com/google/protobuf/releases

set -euo pipefail

main() {
    # Here, we use proto2. Alternatively, you can use proto3.
    #   local readonly url='https://github.com/google/protobuf/releases/download/v3.0.2/protobuf-cpp-3.0.2.zip'
    #   local readonly name='protobuf-cpp-3.0.2'
    local readonly url='https://github.com/google/protobuf/releases/download/v2.6.1/protobuf-2.6.1.zip'
    local readonly name='protobuf-2.6.1'

    sudo apt-get install -y autoconf automake libtool curl make g++ unzip
    wget "$url"
    unzip "$name.zip"
    cd "$name"
    CXX="clang++ -stdlib=libc++" ./configure
    make # this takes a while
    make check
    sudo make install
    sudo ldconfig
}

main
