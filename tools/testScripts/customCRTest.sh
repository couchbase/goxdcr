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
if (( $? != 0 ));then
	echo "Provision failed"
	exit $?
fi

. ./testLibrary.shlib
if (( $? != 0 ));then
  echo "testLibrary.shlib failed"
  exit $?
fi

. ./ccr_tests.shlib
if (( $? != 0 ));then
  echo "ccr_tests.shlib failed"
  exit $?
fi

testCase="${1:-}"

if [[ "$testCase" != "eventingFunctionUIHandlerTest" ]] && [[ "$testCase" != "configureResolver" ]] \
&& [[ "$testCase" != "remoteClusterUserPermission" ]] && [[ "$testCase" != "dataLoad" ]] && [[ "$testCase" != "" ]]
then
  echo "Only the following test cases are available:"
  echo   eventingFunctionUIHandlerTest
  echo   configureResolver
  echo   remoteClusterUserPermission
  echo   dataLoad
  exit 1
fi

DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

CLUSTER_NAME_PORT_MAP=(["C1"]=9000 ["C2"]=9001)
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=13000 ["C2"]=13001)
CLUSTER_NAME_BUCKET_MAP=(["C1"]="CCR1"  ["C2"]="CCR2")

# See MB-39731 for conflictResolutionType=custom
declare -A BucketProperties=(["ramQuotaMB"]=100 ["CompressionMode"]="active" ["conflictResolutionType"]="custom")
for bucket in ${CLUSTER_NAME_BUCKET_MAP[@]}
do
  insertPropertyIntoBucketNamePropertyMap $bucket BucketProperties
done

testForClusterRun
if (( $? != 0 ));then
  exit $?
fi

setupTopologies -d
if (( $? != 0 ));then
  echo "setupTopologies failed"
	exit $?
fi

sleep 5

declare -A CCRReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500 ["compressionType"]="Auto" ["mergeFunctionMapping"]='{"default":"defaultLWW"}')

for cluster1 in ${!CLUSTER_NAME_PORT_MAP[@]}
do
    bucket1=${CLUSTER_NAME_BUCKET_MAP[$cluster1]}
    for cluster2 in ${!CLUSTER_NAME_PORT_MAP[@]}
    do
      bucket2=${CLUSTER_NAME_BUCKET_MAP[$cluster2]}
      if [[ "$cluster1" != "$cluster2" ]];then
        echo "createRemoteClusterReference $cluster1 $cluster2"
        createRemoteClusterReference $cluster1 $cluster2
        createBucketReplication $cluster1 $bucket1 $cluster2 $bucket2 CCRReplProperties
      fi
    done
done

if [[ "$testCase" == "" ]];then
    eventingFunctionUIHandlerTest
    configureResolver
    remoteClusterUserPermission
    dataLoad
else
    $testCase
fi

cleanupBucketReplications
cleanupBuckets
cleanupRemoteClusterRefs