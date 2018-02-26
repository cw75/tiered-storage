#!/bin/bash

if [ -z "$1" ] && [ -z "$2" ] && [ -z "$3" ]; then
  # TODO: eventually switch the two and set prev to 0 default
  echo "Usage: ./add_servers.sh <node-type> <num-prev-instance> <new-instances>"
  echo"Valid node types are m (memory), e (EBS), b (benchmark), and p (proxy)"
  exit 1
fi

if [ "$1" = "memory" ]; then
  YML_FILE=yaml/igs/memory-ig.yml
elif [ "$1" = "ebs" ]; then
  YML_FILE=yaml/igs/ebs-ig.yml
elif [ "$1" = "proxy" ]; then
  YML_FILE=yaml/igs/proxy-ig.yml
elif [ "$1" = "benchmark" ]; then
  YML_FILE=yaml/igs/benchmark-ig.yml
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), b (benchmark), and p (proxy)."
fi

NUM_INSTANCES=$(($2 + $3))

sed "s|CLUSTER_NAME|$NAME|g" $YML_FILE > tmp.yml
sed -i "s|NUM_DUMMY|$NUM_INSTANCES|g" tmp.yml

kops replace -f tmp.yml --force #> /dev/null 2>&1
rm tmp.yml
