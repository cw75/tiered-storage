#!/bin/bash

HAS_PATH=`pwd | grep tests/simple | wc -l`

if [ $HAS_PATH -eq 1 ]; then
  echo "Must run this test from the project root directory."
  exit 1
fi

./scripts/start_local.sh n n
sleep 5
./build/src/bedrock/user tests/simple/input > tmp.out

DIFF=`diff tmp.out tests/simple/expected.out`

if [ "$DIFF" != "" ]; then
  echo "Output did not match expected output (tests/simple/expected.out). Observed output was: "
  cat tmp.out
  rm tmp.out
  echo $DIFF
  CODE=1
else
  echo "Test succeeded!"
  CODE=0
fi

./scripts/stop_local.sh y
exit $CODE
