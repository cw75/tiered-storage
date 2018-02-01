#!/bin/bash

if [[ -z "$1" ]] || [[ "$1" = "m" && -z "$3" ]] || [[ "$1" = "e" && -z "$3" ]]; then
  echo "Usage: ./add_node.sh <node-type> {<uuid>} {<join-node>}"
  echo ""
  echo "Expected usage is calling add_node, which in turn adds a server (using add_server.sh) if a UUID is not specified."
  echo "If passing in a join-node value but not a uuid, pass in NULL for uuid."
  echo "If adding a server node, join node determines whether it is the initial node (n) or a node joining an existing server (y)."
  exit 1
fi

# NOTE: This generates a broken pipe error from tr, which we can ignore because
# we're purposefully terminating the pipe early once we have the characters we
# want.

if [ -z "$2" ] || [ "$2" = "NULL" ]; then
  # if a UUID is not specified, then create ond create a new server
  UUID=`tr -dc 'a-z0-9' < /dev/urandom | head -c 16`

  ./add_server.sh $1 $UUID
else
  UUID=$2
fi

# get the ips of all the different kinds of nodes in the system
PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`
if [ "$1" = "m" ] || [ "$1" = "e" ]; then
  while [ "$PROXY_IPS" = "" ]; do
    PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`
  done
fi

MEM_SERVERS=`kubectl get pods -l role=memory -o jsonpath='{.items[*].status.podIP}'`
if [ "$1" = "m" ] && [ "$3" = "y" ]; then
  while [ "$MEM_SERVERS" = "" ]; do
    MEM_SERVERS=`kubectl get pods -l role=memory -o jsonpath='{.items[*].status.podIP}'`
  done
fi

EBS_SERVERS=`kubectl get pods -l role=ebs -o jsonpath='{.items[*].status.podIP}'`
if [ "$1" = "e" ] && [ "$3" = "y" ]; then
  while [ "$EBS_SERVERS" = "" ]; do
    EBS_SERVERS=`kubectl get pods -l role=ebs -o jsonpath='{.items[*].status.podIP}'`
  done
fi

# this one should never be empty
MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MON_IP" = "" ]; do
  MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done

if [ -z "$PROXY_IPS" ]; then
  PROXY_IPS=NONE
fi

if [ -z "$MEM_SERVERS" ]; then
  MEM_SERVERS=NONE
fi

if [ -z "$EBS_SERVERS" ]; then
  EBS_SERVERS=NONE
fi

if [ "$1" = "m" ]; then
  YML_FILE=yaml/pods/memory-pod.yml
  SERVERS=$MEM_SERVERS
elif [ "$1" = "b" ]; then
  YML_FILE=yaml/pods/benchmark-pod.yml
  SERVERS=""
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/pods/ebs-pod.yml
  SERVERS=$EBS_SERVERS

  # create new EBS volumes; we have 3 per server by default
  EBS_V1=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  aws ec2 create-tags --resources $EBS_V1 --tags Key=KubernetesCluster,Value=$NAME
  EBS_V2=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  aws ec2 create-tags --resources $EBS_V2 --tags Key=KubernetesCluster,Value=$NAME
  EBS_V3=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  aws ec2 create-tags --resources $EBS_V3 --tags Key=KubernetesCluster,Value=$NAME
elif [ "$1" = "p" ]; then
  YML_FILE=yaml/pods/proxy-pod.yml
  SERVERS=""
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), b (benchmark), and p (proxy)."
  exit 1
fi
 

# set the uuid
sed "s|UNIQUE|$UUID|g" $YML_FILE > tmp.yml

# split the servers into an array and choose a random one as the seed
IFS=' ' read -ra ARR <<< "$SERVERS"
if [ ${#ARR[@]} -eq 0 ]; then
  SEED_SERVER=""
else
  SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
fi

# set EBS volume IDs
sed -i "s|VOLUME_DUMMY_1|$EBS_V1|g" tmp.yml
sed -i "s|VOLUME_DUMMY_2|$EBS_V2|g" tmp.yml
sed -i "s|VOLUME_DUMMY_3|$EBS_V3|g" tmp.yml
 
# set the IPs of other system components
sed -i "s|PROXY_IPS_DUMMY|\"$PROXY_IPS\"|g" tmp.yml
sed -i "s|MON_IP_DUMMY|$MON_IP|g" tmp.yml
sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml
sed -i "s|MEM_SERVERS_DUMMY|$MEM_SERVERS|g" tmp.yml
sed -i "s|EBS_SERVERS_DUMMY|$EBS_SERVERS|g" tmp.yml

# set whether this is a join node or not
sed -i "s|NEW_DUMMY|\"$3\"|g" tmp.yml

echo "Creating pod on the new instance..."
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml
