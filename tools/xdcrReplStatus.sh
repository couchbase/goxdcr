#!/usr/local/bin//bash

# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This is a example script of how to use XDCR REST APIs to find out
# 1. The replications that exist and whether or not they are paused
# 2. If each replication contains errors or not
# 3. Calculate time remaining for the replication per node to catch up using "changes_left" and "rate_replicated" stats

# START CONFIGURABLE SECTION
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"
HOSTNAME="127.0.0.1:9000"

# set to 1 for prod
declare LIVEPROD=0

# If changes_left is below this number, it is considered caught up
declare CHANGES_LEFT_THRESHOLD=150

# END CONFIGURABLE SECTION

# const msgs
caughtUpMsg="Caught up"
stuckMsg="Unknown - replication rate is 0"

function getReplicationPauses {
	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$HOSTNAME/pools/default/replications | jq | grep -e "id" -e "pause" | grep -v limit
}

function getReplicationIDs {
	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$HOSTNAME/pools/default/replications | jq | grep \"id\" | awk '{print $NF}' | sed 's/"//g' | sed 's/,//g' | sed 's/\//%2F/g'
}

function getAllNodes {
	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$HOSTNAME/pools/default | jsonpp | grep hostname | awk '{print $2}' | sed 's/,//g' | sed 's/"//g'
}

function getErrorsList {
	local hostname=$1
	if (($LIVEPROD == 1)); then
		# check this logic - should output FQDN:9998
		hostname=$(echo "$hostname" | sed 's/:8091/:9998/g')
	fi
	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$hostname/pools/default/replicationInfos | jq | grep ErrorList
}

function getChangesLeft {
	local hostname=$1
	local replId=$2
	local srcBucket=$(echo "$replId" | sed 's/%2F/,/g' | cut -d',' -f2)

	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$hostname/pools/default/buckets/${srcBucket}/stats/replications%2F${replId}%2Fchanges_left?zoom=hour
}

function getHostnamesFromChangesLeft {
	local input=$(</dev/stdin)

	echo "$input" | jq .'nodeStats' | jq 'keys' | grep -v '\[' | grep -v '\]' | sed 's/"//g' | sed 's/,//g' | awk '{print $1}'
}

declare _getLastStatForNodeName
function getLastStatForNode {
	local input=$(</dev/stdin)

	echo "$input" | jq .'nodeStats' | jq .\""$_getLastStatForNodeName"\" | jq '.[-1]'
}

function getReplicationRate {
	local hostname=$1
	local replId=$2
	local srcBucket=$(echo "$replId" | sed 's/%2F/,/g' | cut -d',' -f2)

	curl -sX GET -u $DEFAULT_ADMIN:$DEFAULT_PW http://$hostname/pools/default/buckets/${srcBucket}/stats/replications%2F${replId}%2Frate_replicated?zoom=minute
}

#MAIN

bash_major_version=$(echo ${BASH_VERSION} | cut -d. -f1)
if (($bash_major_version < 4)); then
	echo "===================================================================="
	echo "Bash version >= 4 is required. Current bash version: ${BASH_VERSION}. Script may fail"
	echo "===================================================================="
	exit 1
fi

declare -a replIDs

echo "-----------------"
echo "Found following replication IDs:"
unparsedIDs=$(getReplicationIDs)
echo "$unparsedIDs"
for id in $(echo "$unparsedIDs"); do
	replIDs[${#replIDs[@]}]="$id"
done

echo "-----------------"
echo "The following replications are listed and whether paused or not..."
getReplicationPauses

echo "-----------------"
echo "The cluster has the following nodes..."
nodes=$(getAllNodes)
echo "$nodes"

declare -a xdcrNodesPorts
if (($LIVEPROD == 1)); then
	for node in $(echo "$nodes"); do
		replacedNode=$(echo "$node" | sed 's/:8091/:9998/g')j
		xdcrNodesPorts[${#xdcrNodesPorts[@]}]="$replacedNode"
	done
else
	#dev env
	counter=0
	for node in $(echo "$nodes"); do
		xdcrNodesPorts[${#xdcrNodesPorts[@]}]="127.0.0.1:1300${counter}"
		counter=$((counter + 1))
	done
fi

echo "-----------------"
echo "Expanding each node and its error list..."
for node in $(echo "${xdcrNodesPorts[@]}"); do
	echo "Node $node has errors list:"
	getErrorsList $node
done

declare -A changesLeftHostnameMap
declare -A replicationRateHostnameMap
declare -A timeRemainingHostnameMap
declare -A estimatedTimeHostnameMap

echo "-----------------"
echo "Per replication, finding stats"

for id in $(echo ${replIDs[@]}); do
	echo "===================="
	echo "Replication ID: $id"
	echo "===================="
	echo "-------------------"
	echo "Changes left per replication..."

	changesNodes=$(getChangesLeft "$HOSTNAME" "$id" | getHostnamesFromChangesLeft)
	for oneNode in $(echo "$changesNodes"); do
		_getLastStatForNodeName="$oneNode"
		oneChangesLeft=$(getChangesLeft "$HOSTNAME" "$id" | getLastStatForNode)
		changesLeftHostnameMap["$oneNode"]="$oneChangesLeft"
	done

	for x in "${!changesLeftHostnameMap[@]}"; do printf "[%s]=%s\n" "$x" "${changesLeftHostnameMap[$x]}"; done

	echo "-------------------"
	echo "Replication Rate (docs per second) per replication..."
	for oneNode in $(echo "$changesNodes"); do
		oneRate=$(getReplicationRate "$HOSTNAME" "$id" | getLastStatForNode)
		replicationRateHostnameMap["$oneNode"]="$oneRate"
	done

	for x in "${!replicationRateHostnameMap[@]}"; do printf "[%s]=%s\n" "$x" "${replicationRateHostnameMap[$x]}"; done

	# calculate how much time (secs) to reach the end
	echo "-------------------"
	echo "Estimated catch up time per replication..."
	for oneNode in $(echo "$changesNodes"); do
		oneChangesLeft=${changesLeftHostnameMap["$oneNode"]}
		if (($oneChangesLeft < $CHANGES_LEFT_THRESHOLD)); then
			timeRemainingHostnameMap["$oneNode"]="$caughtUpMsg"
			continue
		fi

		# First handle ones that are not replicating - impossible to calculate
		oneRate=${replicationRateHostnameMap["$oneNode"]}
		if [[ "$oneRate" == "0" ]]; then
			timeRemainingHostnameMap["$oneNode"]="$stuckMsg"
			continue
		fi

		secsNeeded="$(echo "$oneChangesLeft / $oneRate" | bc)"
		if (($secsNeeded <= 60)); then
			timeRemainingHostnameMap["$oneNode"]="$secsNeeded secs"
		else
			minRemain="$(echo "$secsNeeded / 60" | bc)"
			timeRemainingHostnameMap["$oneNode"]="$minRemain mins"
		fi
	done

	for x in "${!timeRemainingHostnameMap[@]}"; do printf "[%s]=%s\n" "$x" "${timeRemainingHostnameMap[$x]}"; done
done
