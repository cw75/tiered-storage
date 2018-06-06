FROM ubuntu:14.04

MAINTAINER Vikram Sreekanti <vsreekanti@gmail..com> version: 0.1

USER root

# run updates
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool curl make g++ unzip pkg-config wget clang-3.9
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 1
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-3.9 1
RUN apt-get install -y libc++-dev libc++abi-dev

# install cmake
RUN wget https://cmake.org/files/v3.9/cmake-3.9.4-Linux-x86_64.tar.gz
RUN tar xvzf cmake-3.9.4-Linux-x86_64.tar.gz
RUN mv cmake-3.9.4-Linux-x86_64 /usr/bin/cmake
ENV PATH $PATH:/usr/bin/cmake/bin
RUN rm cmake-3.9.4-Linux-x86_64.tar.gz

# install version control
RUN apt-get install -y git

# install vim
RUN apt-get install -y vim

# install protobuf
RUN wget https://github.com/google/protobuf/releases/download/v3.5.1/protobuf-all-3.5.1.zip
RUN unzip protobuf-all-3.5.1.zip 
WORKDIR /protobuf-3.5.1/
RUN ./autogen.sh
RUN ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
RUN make -j4
RUN make check 
RUN make install
RUN ldconfig
WORKDIR /
RUN rm -rf protobuf

RUN apt-get update
RUN apt-get install -y awscli jq

# build kvs
RUN git clone https://github.com/cw75/tiered-storage
RUN cd tiered-storage && git fetch -a origin
RUN cd tiered-storage && git checkout tiered-benchmark
RUN cd tiered-storage && sh scripts/build_release.sh

RUN cp tiered-storage/k8s/start.sh .

CMD sh start.sh $SERVER_TYPE
