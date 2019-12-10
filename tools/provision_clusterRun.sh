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
# Set c1 to have 2 buckets and c2 to have 1 bucket
declare -a cluster1BucketsArr
cluster1BucketsArr=("B0" "B1")
cluster2BucketsArr=("B1" "B2")
#CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]}  ["C2"]="B2")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]}  ["C2"]=${cluster2BucketsArr[@]})

# Bucket properties
declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
insertPropertyIntoBucketNamePropertyMap "B0" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "B2" BucketProperty

#pcre_filter='REGEXP_CONTAINS(click, "q(?!uit)")'
#declare -A DefaultBucketReplProperties=(["filterExpression"]="$pcre_filter" ["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)
declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)

# Bucket -> Scopes
# -----------------
declare -a scope1Arr=("S1" "S2")
#BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S3")
BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]=${scope1Arr[@]})

# Scopes -> Collections
# ----------------------
declare -a collection1Arr=("col1" "col2")
declare -a collection2Arr=("col1" "col2" "col3")
SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})


function runDataLoad {
	# Run CBWorkloadgen in parallel
	runCbWorkloadGenBucket "C1" "B0" &
#	runCbWorkloadGenBucket "C1" "B1" &
#	runCbWorkloadGenBucket "C2" "B2" &
	runCbWorkloadGenCollection "C1" "B1" "S1" "col1"
	runCbWorkloadGenCollection "C2" "B1" "S1" "col1"
	runCbWorkloadGenCollection "C2" "B2" "S1" "col1"
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
printGlobalScopeAndCollectionInfo
runDataLoad
sleep 3
createBucketReplication "C1" "B1" "C2" "B1" DefaultBucketReplProperties
createBucketReplication "C1" "B1" "C2" "B2" DefaultBucketReplProperties
createBucketReplication "C2" "B2" "C1" "B1" DefaultBucketReplProperties

exportProvisionedConfig
