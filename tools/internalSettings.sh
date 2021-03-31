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
$0 [-h] -l | -g <ClusterName> | -s <ClusterName> -v "key=val" [-v... ]
	h: This help page
	l: List all available <clusterName> for setting and displaying
	g: Gets all internal settings for cluster <clusterName>
	s: Sets internal setting on cluster <clusterName> with one or more values of "key=val"
EOF
}

declare mode="None"
declare clusterName
declare -a keyVal
declare jqStr

jqLocation=$(which jq)
if (($? == 0)); then
	jqStr="$jqLocation"
fi

function ListInternalSettings {
	listInternalSettings "$clusterName"
}

function SetInternalSettings {
	setInternalSettings "$clusterName" "${keyVal[@]}"
	# sometimes setting doesn't return output always, so wait and then re-list
	sleep 1
	listInternalSettings "$clusterName"
}

while getopts ":hlg:s:v:" opt; do
	case ${opt} in
	h) # process option a
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
	s)
		clusterName=$OPTARG
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
	ListInternalSettings
elif [[ "$mode" == "Set" ]]; then
	SetInternalSettings
fi
