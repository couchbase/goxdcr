#!/usr/bin/env bash
set -u

# Copyright 2020-Present Couchbase, Inc.
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

. ./testLibrary.shlib
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
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]} ["C2"]="B2")

declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
insertPropertyIntoBucketNamePropertyMap "B0" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "B2" BucketProperty

declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)
declare -A ExplicitReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500 ["collectionsExplicitMapping"]="true" ["colMappingRules"]='{"S1.col1":"S1.col2", "S2.col1":"S3.col1"}')

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
	runCbWorkloadGenCollection "C1" "B1" "S1" "col1" &
	runCbWorkloadGenCollection "C1" "B1" "S2" "col1" &
	waitForBgJobs
}

function demoEcho {
	echo "=============================================================="
	echo $1
	echo "=============================================================="
	read -p ""
}

declare -i ORIG_TARGET_MAN_PULL_INTERVAL

function cleanupTestCaseInternalSettings {
	resetCustomManifestRefreshInterval "C1"
}

#MAIN
testForClusterRun
if (($? != 0)); then
	exit $?
fi

setupTopologies
sleep 5

setCustomManifestRefreshInterval "C1"
trap cleanupTestCaseInternalSettings EXIT

echo "==================="
echo "Cluster 1 Bucket1:"
echo "==================="
printSingleClusterBucketScopeAndCollection "C1" "B1"
echo "==================="
echo "Cluster 2 Bucket2:"
echo "==================="
printSingleClusterBucketScopeAndCollection "C2" "B2"

demoEcho "Load data on B1.S1.col1 and B1.S2.col1"
runDataLoad

demoEcho 'Creating Bucket1 -> Bucket 2 replication with rules: "S1.col1":"S1.col2", "S2.col1":"S3.col1"'

createRemoteClusterReference "C1" "C2"
createBucketReplication "C1" "B1" "C2" "B2" ExplicitReplProperties

demoEcho "Creating missing collection (S1.col2) in target bucket"
createScope "C2" "B2" "S1"
createCollection "C2" "B2" "S1" "col2"
echo "==================="
echo "Cluster 2 Bucket2:"
echo "==================="
printSingleClusterBucketScopeAndCollection "C2" "B2"

demoEcho "Demo finished"
