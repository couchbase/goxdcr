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

. ./settingsCommon.shlib
if (( $? != 0 ));then
	exit $?
fi

importProvisionedConfig

function usage {
cat << EOF
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

jqLocation=`which jq`
if (( $? == 0 ));then
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
    h ) # process option a
    	usage
    	exit 0
    	;;
    l)
    	listAllClusters
    	exit 0
    	;;
    g)
    	clusterName=$OPTARG
    	if [[ $mode != "None" ]];then
    		echo "Cannot do -g when -s is specified"
    		exit 1
    	fi
    	mode="Get"
		;;
	s)
    	clusterName=$OPTARG
    	if [[ $mode != "None" ]];then
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

if [[ "$mode" == "None" ]];then
	usage
elif [[ "$mode" == "Get" ]];then
	ListInternalSettings
elif [[ "$mode" == "Set" ]];then
	SetInternalSettings
fi
