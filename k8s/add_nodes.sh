#!/bin/bash

if [[ -z "$1" ]] && [[ -z "$2" ]] && [[ -z "$3" ]] && [[ -z "$4" ]]; then
  echo "Usage: ./add_node.sh <memory-nodes> <ebs-nodes> <proxy-nodes> <benchmark-nodes>"
  echo ""
  echo "Expected usage is calling add_nodes, which in turn adds servers (using add_servers.sh)."
  exit 1
fi

# get the ips of all the different kinds of nodes in the system
PROXY_IPS=`kubectl get pods -l role=proxy -o jsonpath='{.items[*].status.podIP}'`

# this one should never be empty
MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MON_IP" = "" ]; do
  MON_IP=`kubectl get pods -l role=monitoring -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done

get_prev_num() {
  NUM_PREV=`kubectl get pods -l role=$1 | wc -l`

  if [ $NUM_PREV -gt 0 ]; then
    ((NUM_PREV--))
  fi 

  echo $NUM_PREV
}

add_servers() {
  if [ $2 -gt 0 ]; then
    NUM_PREV=$(get_prev_num $1)

    ./add_servers.sh $1 $2 $NUM_PREV 
  fi
}

add_servers memory $1
add_servers ebs $2
add_servers proxy $3
add_servers benchmark $4

kops update cluster ${NAME} --yes > /dev/null 2>&1

kops validate cluster > /dev/null 2>&1
while [ $? -ne 0 ]; do
  kops validate cluster > /dev/null 2>&1
done

add_pods() {
  NUM_PREV=$(get_prev_num $1)

  if [ "$1" = "memory" ]; then
    YML_FILE=yaml/pods/memory-pod.yml
  elif [ "$1" = "benchmark" ]; then
    YML_FILE=yaml/pods/benchmark-pod.yml
  elif [ "$1" = "ebs" ]; then
    YML_FILE=yaml/pods/ebs-pod.yml
  elif [ "$1" = "proxy" ]; then
    YML_FILE=yaml/pods/proxy-pod.yml
  else
    exit 1
  fi
   
  # split the proxies into an array and choose a random one as the seed
  IFS=' ' read -ra ARR <<< "$PROXY_IPS"
  if [ ${#ARR[@]} -eq 0 ]; then
    SEED_SERVER=""
  else
    SEED_SERVER=${ARR[$RANDOM % ${#ARR[@]}]}
  fi

  FINAL_NUM=$(($NUM_PREV + $2))
  ((NUM_PREV++))

  for i in $(seq $NUM_PREV $FINAL_NUM); do
    sed "s|NUM_DUMMY|$i|g" $YML_FILE > tmp.yml

    if [ "$1" = "e" ]; then
      # create new EBS volume
      EBS_V0=`aws ec2 create-volume --availability-zone=us-east-1a --size=64 --volume-type=gp2 | grep VolumeId | cut -d\" -f4`
      aws ec2 create-tags --resources $EBS_V0 --tags Key=KubernetesCluster,Value=$NAME

      # set EBS volume IDs
      sed -i "s|VOLUME_DUMMY_0|$EBS_V0|g" tmp.yml
    fi
     
    # set the IPs of other system components
    sed -i "s|PROXY_IPS_DUMMY|\"$PROXY_IPS\"|g" tmp.yml
    sed -i "s|MON_IP_DUMMY|$MON_IP|g" tmp.yml
    sed -i "s|SEED_SERVER_DUMMY|$SEED_SERVER|g" tmp.yml

    echo "Creating pod on the new instance..."
    kubectl create -f tmp.yml > /dev/null 2>&1
    rm tmp.yml
  done 
}

add_pods memory $1
add_pods ebs $2
add_pods proxy $3
add_pods benchmark $4

