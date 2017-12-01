#!/bin/bash

if [ -z "$1" ] && [ -z "$2"]; then
  echo "Usage: ./create_cluster.sh <min_mem_instances> <min_ebs_instances>"
fi

export NAME=kvs.k8s.local
export KOPS_STATE_STORE=s3://tiered-storage-state-store

echo "Creating cluster object..."
kops create cluster --zones us-east-1a ${NAME} > /dev/null 2>&1
# delete default instance group that we won't use
kops delete ig nodes --name ${NAME} --yes > /dev/null 2>&1

# add the kops node
echo "Adding kops management node"
sed "s|CLUSTER_NAME|$NAME|g" yaml/igs/kops-ig.yml > tmp.yml
kops create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

# create the cluster with just the proxy instance group
echo "Creating cluster on AWS..."
kops update cluster --name ${NAME} --yes > /dev/null 2>&1

# wait until the cluster was created
echo "Validating cluster..."
kops validate cluster > /dev/null 2>&1
while [ $? -ne 0 ]
do
  kops validate cluster > /dev/null 2>&1
done

# create the kops pod
sed "s|ACCESS_KEY_ID_DUMMY|$AWS_ACCESS_KEY_ID|g" > tmp.yml
sed -i "s|SECRET_KEY_DUMMY|$AWS_SECRET_ACCESS_KEY|g" tmp.yml
kubectl create -f tmp.yml > /dev/null 2>&1

# TODO: eventually, we will want to add multiple proxies, so this should check
# if we want to create a new server for it; however, the memory and EBS tiers
# will always create their own servers; for now we only have one proxy, so no
# worries about uniqueness.
echo "Creating proxy node..."
./add_node.sh p

# TODO: optimize this to create multiple nodes at once
echo "Creating $1 memory node(s)..."
for i in $(seq 1 $1); do
  ./add_node.sh m
done

echo "Creating $2 EBS node(s)..."
for i in $(seq 1 $2); do
  ./add_node.sh e
done

# TODO: once we have a yaml config file, we will have to set the values in each
# of the pod scripts for the replication factors, etc. otherwise, we'll end up
# with weird inconsistencies
