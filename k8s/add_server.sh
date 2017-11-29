#!/bin/bash

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./add_server.sh <node-type> <validate> {<uid>}\nValid node types are m (memory), e (EBS), and p (proxy); valid validate options are y or n."
  exit 1
fi

if [ "$1" = "m" ]; then
  YML_FILE=yaml/igs/memory-ig.yml
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/igs/ebs-ig.yml
elif [ "$1" = "p" ]; then
  sed "s|CLUSTER_NAME|$NAME|g" yaml/igs/proxy-ig.yml > tmp.yml
  kops create -f tmp.yml > /dev/null 2>&1
  rm tmp.yml
  exit 0
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), and p (proxy)."
fi

if [ -z "$3" ]; then
  echo "Must provide a unique ID when creating a storage node."
  exit 1
fi

sed "s|CLUSTER_NAME|$NAME|g" $YML_FILE > tmp.yml
sed -i "s|UNIQUE|$3|g" tmp.yml

echo "Adding an EC2 server to the cluster..."
kops create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

# if "y" is set, that means we want to validate the cluster
if [ "$2" = "y" ]; then
  kops update cluster ${NAME} --yes

  echo "Valdiatng cluster..."
  kops validate cluster > /dev/null 2>&1
  while [ $? -ne 0 ]; do
    kops validate cluster > /dev/null 2>&1
  done
fi 
