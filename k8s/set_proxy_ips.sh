#!/bin/bash

# empty out whatever is in the client ip file right now
echo -n "" > conf/server/proxy_address.txt

# bash automatically splits this on space; the CLIENT_IPS variable is set at
# runtime
for ip in $CLIENT_IPS
do
  echo $ip >> conf/server/proxy_address.txt
done
