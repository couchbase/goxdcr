#!/usr/bin/env bash
set -u

# Copyright 2019-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This is an example provision script that can be edited to quickly conjure up a
# 2 1-node clusters, bidirectional replication to one another via a clean "cluster_run -n 2"
# then load 10k documents on each bucket, resulting in 20k total docs per bucket after
# bi-directional replication

# main logic all exist elsewhere
. ./clusterRunProvision.shlib
if (($? != 0)); then
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
cluster2BucketsArr=("B0" "B2")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]} ["C2"]=${cluster2BucketsArr[@]})

# Bucket properties
declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
declare -A Bucket0Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active" ["flushEnabled"]=1)
insertPropertyIntoBucketNamePropertyMap "B0" Bucket0Properties
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "B2" BucketProperty

#pcre_filter='REGEXP_CONTAINS(click, "q(?!uit)")'
#declare -A DefaultBucketReplProperties=(["filterExpression"]="$pcre_filter" ["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)
declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)

# Bucket -> Scopes
# -----------------
declare -a scope1Arr=("S1" "S2")
#BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S3")
BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S1")

# Scopes -> Collections
# ----------------------
declare -a collection1Arr=("col1" "col2")
declare -a collection2Arr=("col1" "col2" "col3")
SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})

function runDataLoad {
	# Run CBWorkloadgen in parallel
	#	runCbWorkloadGenBucket "C1" "B0" &
	runCbWorkloadGenBucket "C1" "B1" &
	#	runCbWorkloadGenBucket "C2" "B2" &
	runCbWorkloadGenCollection "C1" "B1" "S1" "col1" "col_"
	waitForBgJobs
}

#MAIN
testForClusterRun
if (($? != 0)); then
	exit $?
fi

# NOTE: Sets up all clusters with developer preview to enable all features
setupTopologies -d
if (($? != 0)); then
	exit $?
fi
# Wait for vbuckets and all the other things to propagate before XDCR provisioning
sleep 5
createRemoteClusterReference "C1" "C2"
createRemoteClusterReference "C2" "C1"
sleep 1
createBucketReplication "C1" "B1" "C2" "B2" DefaultBucketReplProperties
createBucketReplication "C2" "B2" "C1" "B1" DefaultBucketReplProperties
printGlobalScopeAndCollectionInfo
runDataLoad

writeJSONDocument "C1" "B1" "regDoc" '{"foo":"bar"}'
writeCollectionJSONDoc "C1" "B1" "S1" "col2" "collectionDoc" '{"colFoo":"colBar"}'

exportProvisionedConfig
