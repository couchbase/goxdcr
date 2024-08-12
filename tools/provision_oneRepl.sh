#!/usr/bin/env bash
set -u

# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Script to quicky setup:
# 1. Two cluster_run clusters, with one data-service node each, of choice (Default=9000 and 9001)
# 2. One bucket on each on them of choice (Default=B1 and B2 in 9000 and 9002 clusters resp.)
# 3. Uni or Bi-directional replication between the setup buckets (Default=UniDirectional)
# 4. Load data into the setup buckets (Default=0 items loaded).
# Example:
# ./provision_oneRepl.sh --cluster1=9000 --cluster2=9001 --bucket1=B1 --bucket2=B2 --dir=2 --items=100

function man() {
	echo "Usage: $0 [--dir=<1|2>] [--bucket1=<bucket1Name>] [--bucket2=<bucket2Name>] [--cluster1=<cluster1Port>] [--cluster2=<cluster2Port>] [--items=<numItems>]" 1>&2
	echo "-h OR --help		: Help" 1>&2
	echo "-d OR --dir		: Unidirectional (=1) or Bidirectional (=2). Default=Unidirectional from bucket1 to bucket2" 1>&2
	echo "-a or --bucket1		: bucket1 name to create for bucket1 <-> bucket2 replication. Default=B1" 1>&2
	echo "-b or --bucket2		: bucket2 name to create for bucket1 <-> bucket2 replication. Default=B2" 1>&2
	echo "-e or --cluster1	: cluster1 port where bucket1 resides for bucket1 <-> bucket2 replication. Default=9000" 1>&2
	echo "-f or --cluster2	: cluster2 port where bucket2 resides for bucket1 <-> bucket2 replication. Default=9001" 1>&2
	echo "-i or --items		: number of items to load on both buckets. Default=0" 1>&2
	exit 1
}

declare BUCKET1
declare CLUSTER1
declare BUCKET2
declare CLUSTER2
declare DIR
declare ITEMS

while getopts "a:b:d:e:f:i:-:h" opt; do
	case "${opt}" in
	-)
		case "${OPTARG}" in
		bucket1=*)
			BUCKET1=${OPTARG#*=}
			;;
		bucket2=*)
			BUCKET2=${OPTARG#*=}
			;;
		cluster1=*)
			CLUSTER1=${OPTARG#*=}
			;;
		cluster2=*)
			CLUSTER2=${OPTARG#*=}
			;;
		dir=*)
			DIR=${OPTARG#*=}
			;;
		items=*)
			ITEMS=${OPTARG#*=}
			;;
		help)
			man $0
			exit 0
			;;
		*)
			echo "ERRO: Unknown option --${OPTARG}" >&2
			man $0
			exit 1
			;;
		esac
		;;
	a)
		BUCKET1=${OPTARG}
		;;
	b)
		BUCKET2=${OPTARG}
		;;
	e)
		CLUSTER1=${OPTARG}
		;;
	f)
		CLUSTER2=${OPTARG}
		;;
	d)
		DIR=${OPTARG}
		;;
	i)
		ITEMS=${OPTARG}
		;;
	h)
		man $0
		;;
	*)
		echo "ERRO: Unknown option --${OPTARG}" >&2
		man $0
		;;
	esac
done

if [[ -z "${BUCKET1:-}" ]]; then
	echo "bucket1 name defaulted to B1"
	BUCKET1="B1"
fi

if [[ -z "${BUCKET2:-}" ]]; then
	echo "bucket2 name defaulted to B2"
	BUCKET2="B2"
fi

if [[ -z "${CLUSTER1:-}" ]]; then
	echo "cluster1 port defaulted to 9000"
	CLUSTER1=9000
fi

if [[ -z "${CLUSTER2:-}" ]]; then
	echo "cluster2 port defaulted to 9001"
	CLUSTER2=9001
fi

if [[ -z "${DIR:-}" ]] || ((DIR >= 3)) || ((DIR <= 0)); then
	echo "Direction defaulted to unidirectional from bucket1 to bucket2 only"
	DIR=1
fi

if [[ -z "${ITEMS:-}" ]] || ((ITEMS < 0)); then
	echo "Number of items to load defaulted to 0"
	ITEMS=0
fi

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
CLUSTER_NAME_PORT_MAP=(["C1"]=$CLUSTER1 ["C2"]=$CLUSTER2)
CLUSTER_NAME_BUCKET_MAP=(["C1"]=$BUCKET1 ["C2"]=$BUCKET2)

# Bucket properties
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active" ["flushEnabled"]=1)

# Replication properties
declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)

insertPropertyIntoBucketNamePropertyMap "$BUCKET1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "$BUCKET2" Bucket1Properties

#MAIN
testForClusterRun
if (($? != 0)); then
	exit $?
fi

setupTopologies
if (($? != 0)); then
	exit 1
fi

createRemoteClusterReference "C1" "C2"
echo "Creating replication C1 ($BUCKET1 of $CLUSTER1) -> C2 ($BUCKET2 of $CLUSTER2)"
createBucketReplication "C1" "$BUCKET1" "C2" "$BUCKET2" DefaultBucketReplProperties

if [[ ! -z $DIR ]] && ((DIR == 2)); then
	createRemoteClusterReference "C2" "C1"
	echo "Creating replication C2 ($BUCKET2 of $CLUSTER2) -> C1 ($BUCKET1 of $CLUSTER1)"
	createBucketReplication "C2" "$BUCKET2" "C1" "$BUCKET1" DefaultBucketReplProperties
fi

if [[ ! -z $ITEMS ]] && ((ITEMS > 0)); then
	runCbWorkloadGenBucket "C1" "$BUCKET1" $ITEMS
	runCbWorkloadGenBucket "C2" "$BUCKET2" $ITEMS
fi
