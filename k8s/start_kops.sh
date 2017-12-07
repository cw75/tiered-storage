#!/bin/bash

cd tiered-storage

# set AWS environment variables
mkdir -p ~/.aws
echo "[default]\nregion = us-east-1" > ~/.aws/config
echo "[default]\naws_access_key_id = $AWS_ACCESS_KEY_ID\naws_secret_access_key = $AWS_SECRET_ACCESS_KEY" > ~/.aws/credentials
mkdir -p ~/.ssh
echo $PRIVATE_KEY > ~/.ssh/id_rsa

# start python server
cd k8s && python3 kops_server.py
