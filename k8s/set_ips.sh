#!/bin/bash

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./set_ips.sh <ips> <file>"
  exit 1
fi
  
# empty out whatever is in the client ip file right now
echo -n "" > $2

if [ "$1" != "NONE" ]; then
  # bash automatically splits this on space; the variable is set at runtime
  for ip in $1
  do
    echo $ip >> conf/server/proxy_address.txt
  done
fi
