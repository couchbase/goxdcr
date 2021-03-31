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

. ./settingsCommon.shlib
if (($? != 0)); then
	exit $?
fi

importProvisionedConfig

function usage {
	cat <<EOF
$0 [-h] -l | -g <ClusterName> -b <bucketName> | -n/-d <ClusterName> -b <bucketName> -s <ScopeName> [-c <CollectionName>]
	h: This help page
	l: List all available <clusterName> for setting and displaying
	b: Bucket name to operate on
	g: Gets all collections for cluster <clusterName>
	n: Creates a *new* collection or scope on a cluster
	d: Deletes a collection or scope on a cluster
	s: Specifies a scope
	c: (optional) Specifies a collection under a scope

EOF
}

declare mode="None"
declare clusterName
declare bucketName
declare scopeName
declare collectionName
declare -i bucketEntered=0
declare -i scopeEntered=0
declare -i collectionEntered=0

function listScopesAndCollections {
	printSingleClusterBucketScopeAndCollection "$clusterName" "$bucketName"
}

function checkBucket {
	if (($bucketEntered == 0)); then
		echo "Bucket is not specified"
		exit 1
	fi
}

function checkScope {
	if (($scopeEntered == 0)); then
		echo "Scope is not specified"
		exit 1
	fi
}

jqLocation=$(which jq)
if (($? != 0)); then
	echo "Cannot run this script without jq"
	exit 1
fi

while getopts ":hlg:n:d:b:s:c:" opt; do
	case ${opt} in
	h) # process option h
		usage
		exit 0
		;;
	l)
		listAllClusters
		exit 0
		;;
	g)
		clusterName=$OPTARG
		if [[ $mode != "None" ]]; then
			echo "Cannot do -g when -s is specified"
			exit 1
		fi
		mode="Get"
		;;
	n)
		clusterName=$OPTARG
		if [[ $mode != "None" ]]; then
			echo "Cannot do -n when -g or -d is specified"
			exit 1
		fi
		mode="Create"
		;;
	d)
		clusterName=$OPTARG
		if [[ $mode != "None" ]]; then
			echo "Cannot do -d when -g or -n is specified"
			exit 1
		fi
		mode="Delete"
		;;
	b)
		bucketName=$OPTARG
		bucketEntered=1
		;;
	s)
		scopeName=$OPTARG
		scopeEntered=1
		;;
	c)
		collectionName=$OPTARG
		collectionEntered=1
		;;
	esac
done

if [[ "$mode" == "None" ]]; then
	usage
elif [[ "$mode" == "Get" ]]; then
	checkBucket
	listScopesAndCollections
elif [[ "$mode" == "Create" ]]; then
	checkBucket
	checkScope
	if (($collectionEntered == 0)); then
		createScope "$clusterName" "$bucketName" "$scopeName"
	else
		createCollection "$clusterName" "$bucketName" "$scopeName" "$collectionName"
	fi
elif [[ "$mode" == "Delete" ]]; then
	checkBucket
	checkScope
	if (($collectionEntered == 0)); then
		deleteScope "$clusterName" "$bucketName" "$scopeName"
	else
		deleteCollection "$clusterName" "$bucketName" "$scopeName" "$collectionName"
	fi
else
	usage
fi
