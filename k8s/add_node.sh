#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./add_node.sh <node-type>\n\nExpected usage is calling add_node, which in turn adds a server (using add_server.sh)."
  exit 1
fi

if [ "$1" = "m" ]; then
  # first create a new memory server
  MEM_UID=`cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 16 | head -n 1`
  ./add_server.sh m y $MEM_UID

  CLIENT_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`
  SERVERS=`kubectl get pods -l role=memory -o jsonpath='{.items[*].status.podIP}'`

  # split the servers into an array and choose a random one
  IFS=' ' read -ra ARR <<< "$SERVERS"
  if [ ${#ARR[@]} -eq 0 ]; then
    SEED_SERVER=""
  else
    SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
  fi
  
  sed "s|CLIENT_IPS_DUMMY|$CLIENT_IPS|g" yaml/pods/memory-pod.yml > tmp.yml
  sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml
  sed -i "s|UNIQUE|$MEM_UID|g" tmp.yml

  echo "Creating pod on the new instance..."
  kubectl create -f tmp.yml > /dev/null 2>&1
  rm tmp.yml
elif [ "$1" = "p" ]; then
  # for now, we only have one proxy... we will worry about uniqueness for
  # proxies later... see create_cluster for more detail
  kubectl create -f yaml/pods/proxy-pod.yml 
elif [ "$1" = "e" ]; then
  # add an ebs node
  echo "not yet working"
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), and p (proxy)."
fi
