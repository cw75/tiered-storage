#! /bin/bash

# Some useful links:
# - http://zeromq.org/intro:get-the-software

set -euo pipefail

main() {
    sudo apt-get install -y \
        libtool pkg-config build-essential autoconf automake uuid-dev
    wget 'https://github.com/zeromq/zeromq4-1/releases/download/v4.1.5/zeromq-4.1.5.tar.gz'
    tar -xzvf zeromq-4.1.5.tar.gz
    cd zeromq-4.1.5
    CXX="clang++ -stdlib=libc++" ./configure
    make
    sudo make install
    sudo ldconfig
    cd -

    git clone 'https://github.com/zeromq/cppzmq'
    sudo ln -s $HOME/cppzmq/zmq.hpp /usr/local/include
    sudo ln -s $HOME/cppzmq/zmq_addon.hpp /usr/local/include

    # Libraries have been installed in:
    #    /usr/local/lib
    #
    # If you ever happen to want to link against installed libraries
    # in a given directory, LIBDIR, you must either use libtool, and
    # specify the full pathname of the library, or use the `-LLIBDIR'
    # flag during linking and do at least one of the following:
    #    - add LIBDIR to the `LD_LIBRARY_PATH' environment variable
    #      during execution
    #    - add LIBDIR to the `LD_RUN_PATH' environment variable
    #      during linking
    #    - use the `-Wl,-rpath -Wl,LIBDIR' linker flag
    #    - have your system administrator add LIBDIR to `/etc/ld.so.conf'
    #
    # See any operating system documentation about shared libraries for
    # more information, such as the ld(1) and ld.so(8) manual pages.
}

main
