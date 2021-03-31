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

importProvisionedConfig

function usage {
	cat <<EOF
$0 [-h] -l | -g <ID> | -s <ID> -v "key=val" [-v...]
	h: This help page
	l: List all ID and configurations
	g: Gets all replication settings for specified <ID>
	s: Sets replication settings for specified <ID> with one or more values of "key=val"

Example: $0 -s 1 -v "filterExpression=" -v "filterSkipRestream=true"
EOF
}

declare FoundREST=""

# Input:
# 1. # - an ID number to get the REST friendly specID. If input is 0, then just display
# Output:
# An Array of 2 elements: 1. cluster port 2. ReplID
# Return value:
# If non-0 return value, then output is an error msg
function cycleThroughAllRepl {
	local specifiedId=${1:-0}

	local counter=1
	if (($specifiedId == 0)); then
		echo "List of bucket replications:"
		echo -e "ID \t SourceCluster \t SourceBucket \t TargetCluster \t TargetBucket \t REST"
	fi
	for bucketReplKey in "${!BUCKET_REPL_EXPORT_MAP[@]}"; do
		local sourceCluster=$(echo "$bucketReplKey" | cut -d, -f1)
		local sourceBucket=$(echo "$bucketReplKey" | cut -d, -f2)
		local targetCluster=$(echo "$bucketReplKey" | cut -d, -f3)
		local targetBucket=$(echo "$bucketReplKey" | cut -d, -f4)
		local replId=${BUCKET_REPL_EXPORT_MAP[$bucketReplKey]:-}
		# Get rid of " in replId
		replId=$(echo "$replId" | sed 's/"//g')
		if (($specifiedId == 0)); then
			echo -e "$counter \t\t $sourceCluster \t\t $sourceBucket \t\t $targetCluster \t\t $targetBucket \t $replId"
		else
			local port=${CLUSTER_NAME_PORT_MAP[$sourceCluster]:-}
			if [[ -z "$port" ]]; then
				echo "Unable to find port"
				return 1
			fi
			local outArr=($port $replId)
			echo ${outArr[@]}
			return 0
		fi
		counter=$(($counter + 1))
	done

}

function listAllReplications {
	cycleThroughAllRepl 0
}

function getReplInternalsPort {
	lookupPair=($(cycleThroughAllRepl $id))
	if (($? != 0)); then
		echo "$lookupPair"
		return 1
	fi
	local port=${lookupPair[0]:-}
	if [[ -z "{port:-}" ]]; then
		echo "Unable to retrieve cluster port for ID $id"
		return 1
	fi
	echo "$port"
	return 0
}

function getReplInternalsReplId {
	lookupPair=($(cycleThroughAllRepl $id))
	if (($? != 0)); then
		echo "$lookupPair"
		return 1
	fi
	local replId=${lookupPair[1]:-}
	if [[ -z "{replId:-}" ]]; then
		echo "Unable to retrieve replication ID for ID $id"
		return 1
	fi
	echo "$replId"
	return 0
}

# TODO - need to think about how to scale this for collections and scope
function getReplicationSettings {
	local port=$(getReplInternalsPort)
	if (($? != 0)); then
		return $?
	fi
	local replId=$(getReplInternalsReplId)
	if (($? != 0)); then
		return $?
	fi

	getReplicationSettingsInternal "$replId" "$port"
}

function setReplicationSettings {
	local port=$(getReplInternalsPort)
	if (($? != 0)); then
		return $?
	fi
	local replId=$(getReplInternalsReplId)
	if (($? != 0)); then
		return $?
	fi

	setReplicationSettingsInternal "$replId" "$port" ${keyVal[@]}
}

declare id
declare mode="None"
declare -a keyVal
declare jqStr

jqLocation=$(which jq)
if (($? == 0)); then
	jqStr="$jqLocation"
fi

while getopts ":hlg:s:v:" opt; do
	case ${opt} in
	h) # process option a
		usage
		exit 0
		;;
	l)
		listAllReplications
		exit 0
		;;
	g)
		id=$OPTARG
		if [[ $mode != "None" ]]; then
			echo "Cannot do -g when -s is specified"
			exit 1
		fi
		mode="Get"
		;;
	s)
		id=$OPTARG
		if [[ $mode != "None" ]]; then
			echo "Cannot do -s when -g is specified"
			exit 1
		fi
		mode="Set"
		;;
	v)
		keyVal+=("$OPTARG")
		;;
	esac
done

if [[ "$mode" == "None" ]]; then
	usage
elif [[ "$mode" == "Get" ]]; then
	getReplicationSettings
elif [[ "$mode" == "Set" ]]; then
	setReplicationSettings
fi
