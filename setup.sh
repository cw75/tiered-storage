#!/bin/bash

# TODO: check clang, cmake, wget, etc.
UNAMESTR=`uname`

echo "Installing spdlog..."
if [[ "$UNAMESTR" = "Darwin" ]]; then 
  which brew > /dev/null 2>&1
  
  if [[ $? -ne 0 ]]; then
    echo "Must have Homebrew installed on OS X."
    echo "Please follow the instructions at https://brew.sh."

    exit 1
  fi

  brew install spdlog > /dev/null 2>&1
  brew install yaml-cpp > /dev/null 2>&1
elif [[ "$UNAMESTR" = "Linux" ]]; then
  IS_UBUNTU=`cat /etc/os-release | head -n 1 | grep Ubuntu | wc -l`

  if [[ $IS_UBUNTU -ne 1 ]]; then
    echo "Only Ubuntu is currently supported by this script. Please manually install dependencies"
    exit 1
  fi

  sudo apt-get install -y libspdlog > /dev/null 2>&1
  sudo apt-get install -y libyaml-cpp-dev > /dev/null 2>&1
fi

# Check if protobuf is installed, or install it
which protoc > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
  echo "Installing protobuf..."

  wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip > /dev/null 2>&1
  unzip protobuf-all-3.5.1 > /dev/null 2>&1
  rm protobuf-all-3.5.1.zip
  
  cd protobuf-3.5.1
  ./autogen.sh > /dev/null 2>&1
  ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g' > /dev/null 2>&1
  
  make -j4 > /dev/null 2>&1
  make check -j4 > /dev/null 2>&1
  make install > /dev/null 2>&1
  echo "You will be asked for your password to set ldconfig."
  sudo ldconfig > /dev/null 2>&1
  
  cd .. 
  rm -rf protobuf-3.5.1
fi


