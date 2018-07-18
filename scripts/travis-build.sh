#!/bin/bash

SCRIPTS=("scripts/build.sh" "scripts/check-clang.sh" "tests/simple/test-simple.sh" "build/tests/test_all")

for SCRIPT in ${SCRIPTS[@]}; do
  ./"$SCRIPT"
  if [[ $? -ne 0 ]]; then
    echo "$SCRIPT failed with exit code $?."
    exit 1
  fi
done

exit 0
