#!/bin/bash

if [ -z "$1" ] && [ -z "$2"]; then
  echo "Usage: ./create_cluster.sh <min_mem_instances> <min_ebs_instances> {<path-to-ssh-key>}"
  echo ""
  echo "If no SSH key is specified, it is assumed that we are using the default SSH key (/home/ubuntu/.ssh/id_rsa). We assume that the corresponding public key has the same name and ends in .pub."
  exit 1
fi

if [ -z "$3" ]; then
  SSH_KEY=/home/ubuntu/.ssh/id_rsa
else 
  SSH_KEY=$3
fi

export NAME=kvs.k8s.local
export KOPS_STATE_STORE=s3://1-tiered-storage-state-store

echo "Creating cluster object..."
kops create cluster --zones us-east-1a --ssh-public-key ${SSH_KEY}.pub ${NAME} > /dev/null 2>&1
# delete default instance group that we won't use
kops delete ig nodes --name ${NAME} --yes > /dev/null 2>&1

# add the kops node
echo "Adding general instance group"
sed "s|CLUSTER_NAME|$NAME|g" yaml/igs/general-ig.yml > tmp.yml
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
echo "Creating management pods"
sed "s|ACCESS_KEY_ID_DUMMY|$AWS_ACCESS_KEY_ID|g" yaml/pods/kops-pod.yml > tmp.yml
sed -i "s|SECRET_KEY_DUMMY|$AWS_SECRET_ACCESS_KEY|g" tmp.yml
sed -i "s|KOPS_BUCKET_DUMMY|$KOPS_STATE_STORE|g" tmp.yml
sed -i "s|CLUSTER_NAME|$NAME|g" tmp.yml
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

MGMT_IP=`kubectl get pods -l role=kops -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
while [ "$MGMT_IP" = "" ]; do
  MGMT_IP=`kubectl get pods -l role=kops -o jsonpath='{.items[*].status.podIP}' | tr -d '[:space:]'`
done
sed "s|MGMT_IP_DUMMY|$MGMT_IP|g" yaml/pods/monitoring-pod.yml > tmp.yml
kubectl create -f tmp.yml > /dev/null 2>&1
rm tmp.yml

echo "Creating proxy node..."
./add_node.sh p

# TODO: optimize this to create multiple nodes at once
echo "Creating $1 memory node(s)..."

if [ $1 -ge 1 ]; then
  ./add_node.sh m n

  for i in $(seq 2 $1); do
    ./add_node.sh m y
  done
fi

echo "Creating $2 EBS node(s)..."

if [ $2 -ge 1 ]; then
  ./add_node.sh e n

  for i in $(seq 2 $2); do
    ./add_node.sh e y
  done
fi

# copy the SSH key into the management node... doing this later because we need
# to wait for the pod to come up
kubectl cp $SSH_KEY kops-pod:/root/.ssh/id_rsa
kubectl cp ${SSH_KEY}.pub kops-pod:/root/.ssh/id_rsa.pub

echo "Cluster is now ready for use!"

# TODO: once we have a yaml config file, we will have to set the values in each
# of the pod scripts for the replication factors, etc. otherwise, we'll end up
# with weird inconsistencies
