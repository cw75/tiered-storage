#!/bin/bash

SCRIPTS=("scripts/check-clang.sh" "scripts/build.sh -j4" "tests/simple/test-simple.sh")

for SCRIPT in ${SCRIPTS[@]}; do
  ./"$SCRIPT"
  if [[ $? -ne 0 ]]; then
    echo "$SCRIPT failed with exit code $?."
    exit 1
  fi
done

exit 0
