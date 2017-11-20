FROM ubuntu:14.04

MAINTAINER Vikram Sreekanti <vsreekanti@gmail.com> version: 0.1

USER root

# run updates
RUN apt-get update
RUN apt-get install -y build-essential autoconf automake libtool curl make g++ unzip pkg-config wget clang-3.9
RUN update-alternatives --install /usr/bin/clang clang /usr/bin/clang-3.9 1
RUN update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-3.9 1
RUN apt-get install -y libc++-dev libc++abi-dev libtbb-dev

# install cmake
RUN wget https://cmake.org/files/v3.9/cmake-3.9.4-Linux-x86_64.tar.gz
RUN tar xvzf cmake-3.9.4-Linux-x86_64.tar.gz
RUN mv cmake-3.9.4-Linux-x86_64 /usr/bin/cmake
ENV PATH $PATH:/usr/bin/cmake/bin
RUN rm cmake-3.9.4-Linux-x86_64.tar.gz

# install version control
RUN apt-get install -y git

# install protobuf
RUN git clone https://github.com/google/protobuf /protobuf/
WORKDIR /protobuf/
RUN ./autogen.sh
RUN ./configure CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g'
RUN make -j4
RUN make check -j4
RUN make install
RUN ldconfig
WORKDIR /
RUN rm -rf protobuf

RUN apt-get update
RUN apt-get install -y awscli jq

# build kvs
RUN git clone https://github.com/cw75/tiered-storage
RUN cd tiered-storage && sh scripts/build_release.sh

COPY memory_start.sh /
COPY ebs_start.sh /
COPY proxy_start.sh /

RUN mkdir tiered-storage/k8s
COPY ./ /tiered-storage/k8s

CMD sh $RUN_SCRIPT
