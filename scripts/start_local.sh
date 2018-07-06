#!/bin/bash

if [ -z "$1" ] && [ -z "$2" ]; then
  echo "Usage: ./scripts/start_local.sh build start-user"
  echo ""
  echo "You must run this from the project root directory."
  exit 1
fi

if [ "$1" = "y" ] || [ "$1" = "yes" ]; then
  ./scripts/build.sh
fi

./build/src/bedrock/monitoring &
MPID=$!
./build/src/bedrock/routing &
RPID=$!
export SERVER_TYPE=1
./build/src/bedrock/server &
SPID=$!

echo $MPID > pids
echo $RPID >> pids
echo $SPID >> pids

if [ "$2" = "y" ] || [ "$2" = "yes" ]; then
  ./build/src/bedrock/user
fi
