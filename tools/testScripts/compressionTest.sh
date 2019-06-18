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
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=13000 ["C2"]=13001)
declare -a cluster1BucketsArr
declare -a cluster2BucketsArr
cluster1BucketsArr=("B0" "B1")
cluster2BucketsArr=("B2" "B3")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]}  ["C2"]=${cluster2BucketsArr[@]})

# Bucket properties
declare -A Bucket0Properties=(["ramQuotaMB"]=100 ["compressionMode"]="off")
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["compressionMode"]="active")
declare -A Bucket2Properties=(["ramQuotaMB"]=100 ["compressionMode"]="off")
declare -A Bucket3Properties=(["ramQuotaMB"]=100 ["compressionMode"]="off")
insertPropertyIntoBucketNamePropertyMap "B0" Bucket0Properties
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "B2" Bucket2Properties
insertPropertyIntoBucketNamePropertyMap "B3" Bucket3Properties

declare -A NoCompressionReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500 ["compressionType"]="None")
declare -A CompressionReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500 ["compressionType"]="Auto")

# Bucket -> Scopes
# -----------------
#declare -a scope1Arr=("S1" "S2")
#BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S3")

# Scopes -> Collections
# ----------------------
#declare -a collection1Arr=("col1" "col2")
#declare -a collection2Arr=("col1" "col2" "col3")
#SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})


function runDataLoad {
	# Run CBWorkloadgen in parallel
	runCbWorkloadGenBucket "C1" "B0" &
	runCbWorkloadGenBucket "C1" "B1" &
	runCbWorkloadGenBucket "C2" "B2" &
	runCbWorkloadGenBucket "C2" "B3" &
	waitForBgJobs
}

function checkItemCnt {
	local cluster=$1
	local bucket=$2
	
	itemCount=`getBucketItemCount "$cluster" "$bucket"`
	echo "Item count for cluster $cluster bucket $bucket: $itemCount"
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
runDataLoad
sleep 1
createBucketReplication "C1" "B0" "C2" "B2" NoCompressionReplProperties
createBucketReplication "C1" "B1" "C2" "B3" NoCompressionReplProperties
createBucketReplication "C2" "B2" "C1" "B0" NoCompressionReplProperties
createBucketReplication "C2" "B3" "C1" "B1" NoCompressionReplProperties

sleep 1
checkItemCnt "C1" "B1"
checkItemCnt "C1" "B2"
checkItemCnt "C2" "B3"
checkItemCnt "C2" "B4"

#exportProvisionedConfig
