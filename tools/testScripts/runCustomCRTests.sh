#!/usr/bin/env bash
set -u

# Copyright (c) 2020 Couchbase, Inc.
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

# This is an example provision script that can be edited to quickly conjure up a
# 2 1-node clusters, bidirectional replication to one another via a clean "cluster_run -n 2"
# then load 10k documents on each bucket, resulting in 20k total docs per bucket after
# bi-directional replication

# main logic all exist elsewhere
. ./clusterRunProvision.shlib
if (($? != 0)); then
  echo "Provision failed"
  exit 1
fi

. ./testLibrary.shlib
if (($? != 0)); then
  echo "testLibrary.shlib failed"
  exit 1
fi

. customConflict/ccr_tests.shlib
if (($? != 0)); then
  echo "ccr_tests.shlib failed"
  exit 1
fi

testCase="${1:-}"

DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

CLUSTER_NAME_PORT_MAP=(["C1"]=9000 ["C2"]=9001 ["C3"]=9002 ["C4"]=9003)
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=13000 ["C2"]=13001 ["C3"]=13002 ["C4"]=13003)
CLUSTER_NAME_BUCKET_MAP=(["C1"]="CCR1" ["C2"]="CCR2" ["C3"]="CCR3" ["C4"]="CCR4")

# See MB-39731 for conflictResolutionType=custom
declare -A BucketProperties=(["ramQuotaMB"]=100 ["CompressionMode"]="active" ["conflictResolutionType"]="custom")
for bucket in "${CLUSTER_NAME_BUCKET_MAP[@]}"; do
  insertPropertyIntoBucketNamePropertyMap $bucket BucketProperties
done

testForClusterRun
if (($? != 0)); then
  exit 1
fi

if [[ "$testCase" == "" ]]; then
  setUpCcrReplication
  for testFile in $(ls customConflict/testCases); do
    . customConflict/testCases/$testFile
    runTestCase
  done
  cleanupCcrReplication
else
  if [[ "${testCase}" == "Loop" ]]; then
    setUpCcrReplication
    . ./customConflict/testCases/1_data_load.shlib
    i=1
    while :; do
      echo
      echo "=========== Start dataLoad run $i ==========="
      runTestCase
      if (($? != 0)); then
        exit 1
      fi
      echo "=========== end dataLoad run $i ==========="
      echo
      i=$(($i + 1))
    done
    cleanupCcrReplication
  else
    testFile=$(find customConflict/testCases/${testCase}*)
    if [[ -z "$testFile" ]]; then
      echo "Cannot find test case ${testCase}"
      exit 1
    fi
    setUpCcrReplication
    . $testFile
    runTestCase
    cleanupCcrReplication
  fi

fi

grepForPanics
