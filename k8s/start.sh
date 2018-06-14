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
IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`

# TODO: Eventually, we should have multiple monitoring nodes.
if [ "$1" = "mn" ]; then
  echo "monitoring:" > conf/config.yml
  echo "\tmgmt_ip: $MGMT_IP" >> conf/config.yml
  echo "\tip: $IP" >> conf conf/config.yml

  ./build/src/bedrock/monitoring
elif [ "$1" = "p" ]; then
  echo "routing:" > conf/config.yml
  echo "\tmonitoring_ip: $MGMT_IP" >> conf/config.yml
  
  ./build/src/bedrock/routing
elif [ "$1" = "b" ]; then
  echo "user:" > conf/config.yml
  echo "\tmonitoring_ip: $MON_IP" >> conf/config.yml
  echo "\tip: $IP" >> conf conf/config.yml

  LST=$(gen_yml_list "$ROUTING_IP")
  echo "\trouting:" >> conf/config.yml
  echo "$LST" >> conf/config.yml

  ./build/src/bedrock/benchmark
else 
  echo "server:" > conf/config.yml
  echo "\tmonitoring_ip: $MGMT_IP" >> conf/config.yml
  echo "\tseed_ip: $SEED_IP" >> conf/config.yml
  echo "\tip: $IP" >> conf conf/config.yml

  LST=$(gen_yml_list "$ROUTING_IP")
  echo "\trouting:" >> conf/config.yml
  echo "$LST" >> conf/config.yml
  
  ./build/src/bedrock/server
fi

