#!/bin/bash

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

cd tiered-storage
IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`

if [ "$1" = "mn" ]; then
  #sh k8s/set_ips.sh $MEM_IPS conf/monitoring/existing_memory_servers.txt
  #sh k8s/set_ips.sh $EBS_IPS conf/monitoring/existing_ebs_servers.txt
  #sh k8s/set_ips.sh $PROXY_IPS conf/monitoring/proxy_address.txt
  echo $MGMT_IP > conf/monitoring/management_ip.txt 

  ./build/kv_store/lww_kvs/kvs_monitoring
elif [ "$1" = "p" ]; then
  echo $IP > conf/proxy/proxy_ip.txt
  echo $MON_IP > conf/proxy/monitoring_address.txt
  #sh k8s/set_ips.sh $MEM_IPS conf/proxy/existing_memory_servers.txt
  #sh k8s/set_ips.sh $EBS_IPS conf/proxy/existing_ebs_servers.txt

  ./build/kv_store/lww_kvs/kvs_proxy
else 
  echo $IP > conf/server/server_ip.txt
  sh k8s/set_ips.sh $PROXY_IPS conf/server/proxy_address.txt

  # set the seed server and the monitoring address
  echo $SEED_SERVER > conf/server/seed_server.txt
  echo $MON_IP > conf/server/monitoring_address.txt

  if [ "$1" = "m" ]; then
    ./build/kv_store/lww_kvs/kvs_memory_server $NEW
  elif [ "$1" = "e" ]; then
    ./build/kv_store/lww_kvs/kvs_ebs_server $NEW
  else
    echo "Unrecognized server type: $1. Exiting."
    exit 1
  fi
fi
