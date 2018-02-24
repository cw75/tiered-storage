#!/bin/bash

if [[ -z "$1" ]]; then
  echo "Usage: ./add_node.sh <node-type> {<uuid>}"
  echo ""
  echo "Expected usage is calling add_node, which in turn adds a server (using add_server.sh) if a UUID is not specified."
  echo "If not passing a uuid, pass in NULL for uuid."
  exit 1
fi

# NOTE: This generates a broken pipe error from tr, which we can ignore because
# we're purposefully terminating the pipe early once we have the characters we
# want.

if [ -z "$2" ] || [ "$2" = "NULL" ]; then
  # if a UUID is not specified, then create ond create a new server
  UUID=`tr -dc 'a-z0-9' < /dev/urandom | head -c 16`

  ./add_server.sh $1 $UUID
  kops update cluster --name ${NAME} --yes > /dev/null 2>&1
  kops validate cluster > /dev/null 2>&1
  while [ $? -ne 0 ]
  do
    kops validate cluster > /dev/null 2>&1
  done
else
  UUID=$2
fi

# get the ips of all the different kinds of nodes in the system
PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`

# this one should never be empty
MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MON_IP" = "" ]; do
  MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done

if [ -z "$PROXY_IPS" ]; then
  PROXY_IPS=NONE
fi

if [ "$1" = "m" ]; then
  YML_FILE=yaml/pods/memory-pod.yml
  SERVERS=$PROXY_IPS
elif [ "$1" = "b" ]; then
  YML_FILE=yaml/pods/benchmark-pod.yml
  SERVERS=""
elif [ "$1" = "e" ]; then
  YML_FILE=yaml/pods/ebs-pod.yml
  SERVERS=$PROXY_IPS

  # create new EBS volumes; we have 3 per server by default
  EBS_V0=`aws ec2 create-volume --availability-zone=us-east-1a --size=128 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
  aws ec2 create-tags --resources $EBS_V0 --tags Key=KubernetesCluster,Value=$NAME
elif [ "$1" = "p" ]; then
  YML_FILE=yaml/pods/proxy-pod.yml
  SERVERS=""
else
  echo "Unrecognized node type $1. Valid node types are m (memory), e (EBS), b (benchmark), and p (proxy)."
  exit 1
fi
 

# set the uuid
sed "s|UNIQUE|$UUID|g" $YML_FILE > tmp.yml

# split the proxies into an array and choose a random one as the seed
IFS=' ' read -ra ARR <<< "$SERVERS"
if [ ${#ARR[@]} -eq 0 ]; then
  SEED_SERVER=""
else
  SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
fi

# set EBS volume IDs
sed -i "s|VOLUME_DUMMY_0|$EBS_V0|g" tmp.yml
 
# set the IPs of other system components
sed -i "s|PROXY_IPS_DUMMY|\"$PROXY_IPS\"|g" tmp.yml
sed -i "s|MON_IP_DUMMY|$MON_IP|g" tmp.yml
sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml

echo "Creating pod on the new instance..."
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml
