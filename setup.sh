#  Copyright 2018 U.C. Berkeley RISE Lab
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/bash

echo "This assumes that you are starting from scratch on a machine. It will install the following packages."
echo "If you have conflicting versions of any of these packages, this script might fail."
echo -e "\t*build-essential"
echo -e "\t*autoconf"
echo -e "\t*automake"
echo -e "\t*libtool"
echo -e "\t*curl"
echo -e "\t*make"
echo -e "\t*g++"
echo -e "\t*unzip"
echo -e "\tpkg-config"
echo -e "\t*wget"
echo -e "\t*clang-3.9"
echo -e "\t*libc++-dev"
echo -e "\t*libc++abi-dev"
echo -e "\t*cmake-3.9.4"
echo -e "\t*protobuf-3.5.1"
echo -e "\t*git"

UNAMESTR=`uname`

echo "Installing apt packages..."
sudo apt-get update > /dev/null 2>&1
sudo apt-get install -y build-essential autoconf automake libtool curl make g++ unzip pkg-config wget clang-3.9
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 1
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-3.9 1
sudo apt-get install -y libc++-dev libc++abi-dev awscli jq

echo "Installing cmake..."
wget https://cmake.org/files/v3.9/cmake-3.9.4-Linux-x86_64.tar.gz
tar xvzf cmake-3.9.4-Linux-x86_64.tar.gz
sudo mv cmake-3.9.4-Linux-x86_64 /usr/bin/cmake
export PATH=$PATH:/usr/bin/cmake/bin
echo "export PATH=$PATH:/usr/bin/cmake/bin" >> ~/.bashrc
rm cmake-3.9.4-Linux-x86_64.tar.gz

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
  sudo make install > /dev/null 2>&1
  echo "You might be asked for your password to set ldconfig."
  sudo ldconfig > /dev/null 2>&1

  cd ..
  rm -rf protobuf-3.5.1
fi

./scripts/build.sh
