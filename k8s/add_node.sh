#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./add_node.sh <node-type>\n\nExpected usage is calling add_node, which in turn adds a server (using add_server.sh)."
  exit 1
fi
  
UUID=`cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 16 | head -n 1`

if [ "$1" = "m" ]; then
  YML_FILE=yaml/pods/memory-pod.yml

  # add a new memory server
  ./add_server.sh m $UUID
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/pods/ebs-pod.yml

  # add a new EBS server
  ./add_server.sh e $UUID
 
  # create new EBS volumes; we have 3 per server by default
  EBS_V1=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  EBS_V2=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  EBS_V3=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
elif [ "$1" = "p" ]; then
  YML_FILE=yaml/pods/proxy-pod.yml

  ./add_server.sh p $UUID
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), and p (proxy)."
  exit 1
fi
 
# get the ips of all the different kinds of nodes in the system
PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
MEM_SERVERS=`kubectl get pods -l role=memory -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
EBS_SERVERS=`kubectl get pods -l role=ebs -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`

# this one should never be empty
MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`

if [ -z "$PROXY_IPS" ]; then
  PROXY_IPS=NONE
fi

if [ -z "$MEM_SERVERS" ]; then
  MEM_SERVERS=NONE
fi

if [ -z "$EBS_SERVERS" ]; then
  EBS_SERVERS=NONE
fi

# set the uuid
sed "s|UNIQUE|$UUID|g" $YML_FILE > tmp.yml

if [ "$1" = "e" ]; then
  # set the EBS volumes
  sed -i "s|VOLUME_DUMMY_1|$EBS_V1|g" tmp.yml
  sed -i "s|VOLUME_DUMMY_2|$EBS_V2|g" tmp.yml
  sed -i "s|VOLUME_DUMMY_3|$EBS_V3|g" tmp.yml
  
  SERVERS=$EBS_SERVERS
else
  SERVERS=$MEM_SERVERS
fi

# split the servers into an array and choose a random one as the seed
IFS=' ' read -ra ARR <<< "$SERVERS"
if [ ${#ARR[@]} -eq 0 ]; then
  SEED_SERVER=""
else
  SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
fi

sed -i "s|PROXY_IPS_DUMMY|$PROXY_IPS|g" tmp.yml
sed -i "s|MON_IP_DUMMY|$MON_IP|g" tmp.yml
sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml
sed -i "s|MEM_SERVERS_DUMMY|$MEM_SERVERS|g" tmp.yml
sed -i "s|EBS_SERVERS_DUMMY|$EBS_SERVERS|g" tmp.yml

echo "Creating pod on the new instance..."
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml
