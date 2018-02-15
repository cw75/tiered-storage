#!/bin/bash

if [ -z "$1" ] || [ -z "$2" ]; then 
  echo "Usage: ./kops_add_node.sh <node_type> <num-nodes>."
  exit 1
fi

IDS=()
for i in $(seq 1 $2); do
  UUID=`tr -dc 'a-z0-9' < /dev/urandom | head -c 16`
  ./add_server.sh $1 $UUID
  IDS+=( $UUID )
done

kops update cluster --name ${NAME} --yes > /dev/null 2>&1
kops validate cluster > /dev/null 2>&1
while [ $? -ne 0 ]
do
  kops validate cluster > /dev/null 2>&1
done

for ID in ${IDS[@]}; do
  ./add_node.sh $1 $ID
done