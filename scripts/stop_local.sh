#!/bin/bash

while IFS='' read -r line || [[ -n "$line" ]] ; do
  kill $line
done < "pids"

rm pids
