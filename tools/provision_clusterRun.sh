#!/usr/bin/env bash
set -u

# Copyright (c) 2019 Couchbase, Inc.
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
	exit $?
fi

# set globals
# -----------------
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

# =============================
# topological map information
# =============================
# cluster -> Bucket(s)
# -----------------
CLUSTER_NAME_PORT_MAP=(["C1"]=9000 ["C2"]=9001)
# Set c1 to have 2 buckets and c2 to have 1 bucket
declare -a cluster1BucketsArr
cluster1BucketsArr=("B0" "B1")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]}  ["C2"]="B2")
BUCKET_NAME_RAMQUOTA_MAP=(["B0"]=100 ["B1"]=100 ["B2"]=100)

# Bucket -> Scopes
# -----------------
declare -a scope1Arr=("S1" "S2")
BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S3")

# Scopes -> Collections
# ----------------------
declare -a collection1Arr=("col1" "col2")
declare -a collection2Arr=("col1" "col2" "col3")
SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})


function runDataLoad {
	# Run CBWorkloadgen in parallel
	runCbWorkloadGenBucket "C1" "B0" &
	runCbWorkloadGenBucket "C1" "B1" &
	runCbWorkloadGenBucket "C2" "B2" &
	waitForBgJobs
}

#MAIN
testForClusterRun
if (( $? != 0 ));then
	exit $?
fi

setupTopologies
if (( $? != 0 ));then
	exit $?
fi
# Wait for vbuckets and all the other things to propagate before XDCR provisioning
sleep 1
createRemoteClusterReference "C1" "C2"
createRemoteClusterReference "C2" "C1"
sleep 1
createBucketReplication "C1" "B1" "C2" "B2"
createBucketReplication "C2" "B2" "C1" "B1"
printGlobalScopeAndCollectionInfo
runDataLoad
