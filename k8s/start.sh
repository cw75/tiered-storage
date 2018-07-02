#!/bin/bash

if [ -z "$1" ]; then
  echo "No argument provided. Exiting."
  exit 1
fi

gen_yml_list() {
  IFS=' ' read -r -a ARR <<< $1
  RESULT=""

  for IP in "${ARR[@]}"; do
    RESULT=$"$RESULT    - $IP\n"
  done

  echo -e "$RESULT"
}

cd tiered-storage
mkdir -p conf
IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'`

# TODO: Eventually, we should have multiple monitoring nodes.
if [ "$1" = "mn" ]; then
  echo -e "monitoring:" > conf/config.yml
  echo -e "    mgmt_ip: $MGMT_IP" >> conf/config.yml
  echo -e "    ip: $IP" >> conf/config.yml

  ./build/src/bedrock/monitoring
elif [ "$1" = "r" ]; then
  echo -e "routing:" > conf/config.yml
  echo -e "    monitoring_ip: $MON_IP" >> conf/config.yml
  echo -e "    ip: $IP" >> conf/config.yml
  
  ./build/src/bedrock/routing
elif [ "$1" = "b" ]; then
  echo -e "user:" > conf/config.yml
  echo -e "    monitoring_ip: $MON_IP" >> conf/config.yml
  echo -e "    ip: $IP" >> conf conf/config.yml

  LST=$(gen_yml_list "$ROUTING_IP")
  echo -e "    routing:" >> conf/config.yml
  echo -e "$LST" >> conf/config.yml

  ./build/src/bedrock/benchmark
else 
  echo -e "server:" > conf/config.yml
  echo -e "    monitoring_ip: $MON_IP" >> conf/config.yml
  echo -e "    seed_ip: $SEED_IP" >> conf/config.yml
  echo -e "    ip: $IP" >> conf/config.yml

  LST=$(gen_yml_list "$ROUTING_IP")
  echo -e "    routing:" >> conf/config.yml
  echo -e "$LST" >> conf/config.yml
  
  ./build/src/bedrock/server
fi

