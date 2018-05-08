#!/bin/bash

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT\t- $IP\n"
  done

  RESULT=$"$RESULT"
  echo $RESULT
}

cd tiered-storage

# TODO: Eventually, we should have multiple monitoring nodes.
if [ "$1" = "mn" ]; then
  echo "monitoring:" > conf/config.yml
  echo "\tmgmt_ip: $MGMT_IP" >> conf/config.yml

  ./build/kv_store/lww_kvs/kvs_monitoring
elif [ "$1" = "p" ]; then
  echo "proxy:" > conf/config.yml
  echo "\tmonitoring_ip: $MGMT_IP" >> conf/config.yml
  
  ./build/kv_store/lww_kvs/kvs_proxy
elif [ "$1" = "b" ]; then
  echo "user:" > conf/config.yml
  echo "\tmonitoring_ip: $MON_IP" >> conf/config.yml

  LST=$(gen_yml_list "$PROXY_IP")
  echo "\tproxy_ip:" >> conf/config.yml
  echo "$LST" >> conf/config.yml

  ./build/kv_store/lww_kvs/kvs_benchmark
else 
  echo "server:" > conf/config.yml
  echo "\tmonitoring_ip: $MGMT_IP" >> conf/config.yml
  echo "\tseed_ip: $SEED_IP" >> conf/config.yml

  LST=$(gen_yml_list "$PROXY_IP")
  echo "\tproxy_ip:" >> conf/config.yml
  echo "$LST" >> conf/config.yml
  
  ./build/kv_store/lww_kvs/kvs_server
fi

