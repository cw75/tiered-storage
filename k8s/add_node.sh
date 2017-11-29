#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./add_node.sh <node-type>\n\nExpected usage is calling add_node, which in turn adds a server (using add_server.sh)."
  exit 1
fi
  
UUID=`cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 16 | head -n 1`

if [ "$1" = "m" ]; then
  YML_FILE=yaml/pods/memory-pod.yml

  # add a new memory server
  ./add_server.sh m y $UUID
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/pods/ebs-pod.yml

  # add a new EBS server
  ./add_server.sh e y $UUID

  # create new EBS volumes; we have 3 per server by default
  EBS_V1=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  EBS_V2=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  EBS_V3=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
elif [ "$1" = "p" ]; then
  # for now, we only have one proxy... we will worry about uniqueness for
  # proxies later... see create_cluster for more detail
  kubectl create -f yaml/pods/proxy-pod.yml > /dev/null 2>&1
  exit 0
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), and p (proxy)."
  exit 1
fi
  
PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`
SERVERS=`kubectl get pods -l role=memory -o jsonpath='{.items[*].status.podIP}'`

# split the servers into an array and choose a random one
IFS=' ' read -ra ARR <<< "$SERVERS"
if [ ${#ARR[@]} -eq 0 ]; then
  SEED_SERVER=""
else
  SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
fi

sed "s|PROXY_IPS_DUMMY|$PROXY_IPS|g" $YML_FILE > tmp.yml
sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml
sed -i "s|UNIQUE|$UUID|g" tmp.yml

if [ "$1" = "e" ]; then
  sed -i "s|VOLUME_DUMMY_1|$EBS_V1|g" tmp.yml
  sed -i "s|VOLUME_DUMMY_2|$EBS_V2|g" tmp.yml
  sed -i "s|VOLUME_DUMMY_3|$EBS_V3|g" tmp.yml
fi

echo "Creating pod on the new instance..."
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml
