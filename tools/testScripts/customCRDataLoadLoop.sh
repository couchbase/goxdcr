#!/usr/bin/env bash
set -u

i=1

while :
do
  echo "=========== Start dataLoad run $i ==========="
  ./customCRTest.sh dataLoad
  if (( $? != 0 ));then
    exit $?
  fi
  i=$(( $i + 1))
done
