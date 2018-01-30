#!/bin/bash

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./add_server.sh <node-type> <uid>\nValid node types are m (memory), e (EBS), and p (proxy); valid validate options are y or n."
  exit 1
fi

if [ "$1" = "m" ]; then
  YML_FILE=yaml/igs/memory-ig.yml
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/igs/ebs-ig.yml
elif [ "$1" = "p" ]; then
  YML_FILE=yaml/igs/proxy-ig.yml
elif [ "$1" = "b" ]; then
  YML_FILE=yaml/igs/benchmark-ig.yml
  $2=""
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), b (benchmark), and p (proxy)."
fi

sed "s|CLUSTER_NAME|$NAME|g" $YML_FILE > tmp.yml
sed -i "s|UNIQUE|$2|g" tmp.yml

echo "Adding an EC2 server to the cluster..."
kops create -f tmp.yml > /dev/null 2>&1
rm tmp.yml
