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
# 2-cluster 12 buckets by 12 buckets mesh

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

MAX_MESH_INDEX=12

# cluster -> Bucket(s)
# -----------------
CLUSTER_NAME_PORT_MAP=(["C1"]=9000 ["C2"]=9001)
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=13000 ["C2"]=13001)
# Set c1 to have 2 buckets and c2 to have 1 bucket
declare -a cluster1BucketsArr
cluster1BucketsArr=()
for ((i = 0; $i < $MAX_MESH_INDEX; i = $(($i + 1)))); do
	cluster1BucketsArr[${#cluster1BucketsArr[@]}]="B${i}"
done
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]} ["C2"]=${cluster1BucketsArr[@]})

# Bucket properties
declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
for ((i = 0; $i < 12; i = $(($i + 1)))); do
	insertPropertyIntoBucketNamePropertyMap "B${i}" Bucket1Properties
done

declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=600 ["statsInterval"]=20000)

# Bucket -> Scopes
# -----------------
#declare -a scope1Arr=("S1" "S2")
#BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S3")
#BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S1")

# Scopes -> Collections
# ----------------------
#declare -a collection1Arr=("col1" "col2")
#declare -a collection2Arr=("col1" "col2" "col3")
#SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})

function runDataLoad {
	# Run CBWorkloadgen in parallel
	#	runCbWorkloadGenBucket "C1" "B0" &
	#	runCbWorkloadGenBucket "C1" "B1" &
	#	runCbWorkloadGenBucket "C2" "B2" &
	#	runCbWorkloadGenCollection "C1" "B1" "S1" "col1" "col_"
	waitForBgJobs
}

function createMesh {
	local srcCluster=$1
	local tgtCluster=$2
	local srcBucketIdx
	local tgtBucketIdx

	for ((srcBucketIdx = 0; $srcBucketIdx < $MAX_MESH_INDEX; srcBucketIdx = $(($srcBucketIdx + 1)))); do
		for ((tgtBucketIdx = 0; $tgtBucketIdx < $MAX_MESH_INDEX; tgtBucketIdx = $(($tgtBucketIdx + 1)))); do
			createBucketReplication "$srcCluster" "B${srcBucketIdx}" "$tgtCluster" "B${tgtBucketIdx}" DefaultBucketReplProperties
		done
	done
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

echo "Setting internal settings: TopologyChangeCheckInterval..."
setInternalSettingsWithoutImport "C1" "TopologyChangeCheckInterval=60"
setInternalSettingsWithoutImport "C2" "TopologyChangeCheckInterval=60"
sleep 5
echo "Setting internal settings: RefreshRemoteClusterRefInterval..."
setInternalSettingsWithoutImport "C1" "RefreshRemoteClusterRefInterval=60"
setInternalSettingsWithoutImport "C2" "RefreshRemoteClusterRefInterval=60"
sleep 5
echo "Setting internal settings: ReplSpecCheckInterval..."
setInternalSettingsWithoutImport "C1" "ReplSpecCheckInterval=60"
setInternalSettingsWithoutImport "C2" "ReplSpecCheckInterval=60"
sleep 5

# Do a mesh
createMesh "C1" "C2"
createMesh "C2" "C1"

if [[ $(uname) =~ Darwin ]]; then
	echo "Sleeping 120 seconds for system to stabilize before capturing rss..."
	sleep 120

	echo "Resident memory..."
	ps x -ho rss,vsz,command | grep goxdcr | grep -v grep
fi

exportProvisionedConfig
