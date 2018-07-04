#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: ./scripts/stop_local.sh <remove-logs>"
  exit 1
fi

while IFS='' read -r line || [[ -n "$line" ]] ; do
  kill $line
done < "pids"

if [ "$1" = "y" ]; then
  rm log*
fi

rm pids
