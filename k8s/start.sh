#!/bin/bash

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

cd tiered-storage

if [ "$1" = "p" ]; then
  ./build/kv_store/lww_kvs/kvs_proxy
else 
  IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`
  
  echo $IP > conf/server/server_ip.txt

  cd tiered-storage

  sh k8s/set_proxy_ips.sh
  sh k8s/set_seed_server.sh

  sudo chmod +x scripts/add_volume_dummy.sh

  if [ "$1" = "m" ]; then
    ./build/kv_store/lww_kvs/kvs_memory_server n
  elif [ "$1" = "e" ]; then
    ./build/kv_store/lww_kvs/kvs_ebs_server n n
  else
    echo "Unrecognized server type: $1. Exiting."
    exit 1
  fi
fi
