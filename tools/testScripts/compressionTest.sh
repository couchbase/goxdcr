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
declare -a cluster1BucketsArr
declare -a cluster2BucketsArr
cluster1BucketsArr=("B0" "B1")
cluster2BucketsArr=("B2" "B3")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]} ["C2"]=${cluster2BucketsArr[@]})

# Bucket properties
declare -A BucketNoCompression=(["ramQuotaMB"]=100 ["compressionMode"]="off")
declare -A BucketActiveCompression=(["ramQuotaMB"]=100 ["compressionMode"]="active")
declare -A BucketPassiveCompression=(["ramQuotaMB"]=100 ["compressionMode"]="passive")

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
	# sleep 3 seconds before running
	sleep 3
	# Run CBWorkloadgen in parallel
	runCbWorkloadGenBucket "C1" "B0" &
	runCbWorkloadGenBucket "C1" "B1" &
	runCbWorkloadGenBucket "C2" "B2" &
	runCbWorkloadGenBucket "C2" "B3" &
	waitForBgJobs
}

EXPECTED_CNT=20000

#MAIN
sleepTime=10
testForClusterRun
if (($? != 0)); then
	exit $?
fi
sleep 1

function printTestCaseStats {
	local srcCluster=$1
	local srcBucket=$2
	local targetCluster=$3
	local targetBucket=$4
	local -a statsKeys=("docs_written" "data_replicated")

	echo "FROM $srcCluster $srcBucket TO $targetCluster $targetBucket"
	echo "----------------------------------------------------------------------------"
	for key in "${statsKeys[@]}"; do
		local data=$(getStats "$srcCluster" "$srcBucket" "$targetCluster" "$targetBucket" "$key")
		if (($? != 0)); then
			continue
		fi
		echo "$key: $data"
	done
	echo ""
}

function runTestCase {
	local testCaseName=$1
	local sourceBucketPolicy=$2
	local targetBucketPolicy=$3
	local replPolicy=$4

	echo ""
	echo "============================================================================"
	echo "RUNNING TEST CASE $testCaseName"
	echo "============================================================================"
	cleanupBucketNamePropertyMap
	if [[ "$sourceBucketPolicy" == "Active" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B0" BucketActiveCompression
		insertPropertyIntoBucketNamePropertyMap "B1" BucketActiveCompression
	elif [[ "$sourceBucketPolicy" == "Passive" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B0" BucketPassiveCompression
		insertPropertyIntoBucketNamePropertyMap "B1" BucketPassiveCompression
	elif [[ "$sourceBucketPolicy" == "None" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B0" BucketNoCompression
		insertPropertyIntoBucketNamePropertyMap "B1" BucketNoCompression
	fi

	if [[ "$targetBucketPolicy" == "Active" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B2" BucketActiveCompression
		insertPropertyIntoBucketNamePropertyMap "B3" BucketActiveCompression
	elif [[ "$targetBucketPolicy" == "Passive" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B2" BucketPassiveCompression
		insertPropertyIntoBucketNamePropertyMap "B3" BucketPassiveCompression
	elif [[ "$targetBucketPolicy" == "None" ]]; then
		insertPropertyIntoBucketNamePropertyMap "B2" BucketNoCompression
		insertPropertyIntoBucketNamePropertyMap "B3" BucketNoCompression
	fi

	setupTopologies
	if [[ "$testCaseName" == "1a" ]]; then
		# create rC reference once
		createRemoteClusterReference "C1" "C2"
		createRemoteClusterReference "C2" "C1"
		if (($? != 0)); then
			exit $?
		fi
	fi

	# Wait for vbuckets and all the other things to propagate before XDCR provisioning
	sleep 10
	runDataLoad
	sleep 1

	if [[ "$replPolicy" == "Auto" ]]; then
		createBucketReplication "C1" "B0" "C2" "B2" CompressionReplProperties
		createBucketReplication "C1" "B1" "C2" "B3" CompressionReplProperties
		createBucketReplication "C2" "B2" "C1" "B0" CompressionReplProperties
		createBucketReplication "C2" "B3" "C1" "B1" CompressionReplProperties
	elif [[ "$replPolicy" == "None" ]]; then
		createBucketReplication "C1" "B0" "C2" "B2" NoCompressionReplProperties
		createBucketReplication "C1" "B1" "C2" "B3" NoCompressionReplProperties
		createBucketReplication "C2" "B2" "C1" "B0" NoCompressionReplProperties
		createBucketReplication "C2" "B3" "C1" "B1" NoCompressionReplProperties
	fi

	echo "Waiting $sleepTime seconds for I/O to quiesce"
	sleep $sleepTime
	checkItemCnt "C1" "B0" $EXPECTED_CNT
	checkItemCnt "C1" "B1" $EXPECTED_CNT
	checkItemCnt "C2" "B2" $EXPECTED_CNT
	checkItemCnt "C2" "B3" $EXPECTED_CNT

	# Cleaning up buckets should remove all existing replications
	echo "----------------------------------------------------------------------------"
	echo "TEST CASE $testCaseName Summary: sourceBucket $sourceBucketPolicy targetBucket $targetBucketPolicy replCompression $replPolicy"
	printTestCaseStats "C1" "B0" "C2" "B2"
	printTestCaseStats "C1" "B1" "C2" "B3"
	printTestCaseStats "C2" "B2" "C1" "B0"
	printTestCaseStats "C2" "B3" "C1" "B1"
	echo "----------------------------------------------------------------------------"

	echo "============================================================================"
	echo "PASSED TEST CASE $testCaseName"
	echo "============================================================================"
	echo ""
	
	cleanupBucketReplications
	cleanupBuckets
	sleep $sleepTime
}

runTestCase "1a" "Active" "Active" "Auto" \
&& runTestCase "1b" "Active" "Active" "None" \
&& runTestCase "2a" "Active" "Passive" "Auto" \
&& runTestCase "2b" "Active" "Passive" "None" \
&& runTestCase "3a" "Active" "None" "Auto" \
&& runTestCase "3b" "Active" "None" "None" \
&& runTestCase "4a" "Passive" "Active" "Auto" \
&& runTestCase "4b" "Passive" "Active" "None" \
&& runTestCase "5a" "Passive" "Passive" "Auto" \
&& runTestCase "5b" "Passive" "Passive" "None" \
&& runTestCase "6a" "Passive" "None" "Auto" \
&& runTestCase "6b" "Passive" "None" "None" \
&& runTestCase "7a" "None" "Active" "Auto" \
&& runTestCase "7b" "None" "Active" "None" \
&& runTestCase "8a" "None" "Passive" "Auto" \
&& runTestCase "8b" "None" "Passive" "None" \
&& runTestCase "9a" "None" "None" "Auto" \
&& runTestCase "9b" "None" "None" "None"

if (($? != 0)); then
	echo "============================================================================"
	echo "Compression Test FAILED!"
	echo "============================================================================"
	exit 1
fi

echo "============================================================================"
echo "All Compression Tests PASSED!"
echo "============================================================================"