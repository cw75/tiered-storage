#! /bin/bash

# Some useful links:
# - https://cmake.org/download/

set -euo pipefail

main() {
    wget 'https://cmake.org/files/v3.6/cmake-3.6.2-Linux-x86_64.sh'
    yes | sh cmake-3.6.2-Linux-x86_64.sh

    # After you download CMake, you should have the following directory
    # structure:
    #
    #   cmake-3.6.2-Linux-x86_64/
    #   ├── bin
    #   │   ├── ccmake
    #   │   ├── cmake
    #   │   ├── cmake-gui
    #   │   ├── cpack
    #   │   └── ctest
    #   ├── doc
    #   ├── man
    #   └── share
    #
    # In order to run cmake, you'll need to put the bin directory in your path.
    # Here, we assume you source the ~/.bash_path file in your ~/.bashrc.
    echo 'export PATH="$PATH:$HOME/cmake-3.6.2-Linux-x86_64/bin"' >> ~/.bash_path
}

main
