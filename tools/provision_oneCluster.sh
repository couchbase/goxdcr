#!/usr/bin/env bash
set -u

# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This is an example provision script that can be edited to quickly conjure up a
# One 1-node cluster at localhost:<clusterRunPort>, which has 1 bucket called <bucketName>
# <clusterRunPort> and <bucketName> should be input from CLI.
# If it is not input, 9000 and "B1" are taken as default resp.
# Example:
# ./provision_oneCluster.sh --bucket=B1 --cluster=9000

function man() {
	echo "Usage: $0 [--cluster=<clusterRunPort>] [--bucket=<bucketName>]" 1>&2
	echo "-h OR --help      : Help" 1>&2
	echo "-c OR --cluster   : cluster_run port on which the node should run. Eg: 9000, 9001,... Default=9000" 1>&2
	echo "-b or --bucket    : bucket name to create. Default=B1" 1>&2
	exit 1
}

declare BUCKET
declare CLUSTER

while getopts "c:b:-:h" opt; do
	case "${opt}" in
	-)
		case "${OPTARG}" in
		bucket=*)
			BUCKET=${OPTARG#*=}
			;;
		cluster=*)
			CLUSTER=${OPTARG#*=}
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
	b)
		BUCKET=${OPTARG}
		;;
	c)
		CLUSTER=${OPTARG}
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

if [[ -z "${BUCKET:-}" ]]; then
	echo "bucketname defaulted to B1"
	BUCKET="B1"
fi

if [[ -z "${CLUSTER:-}" ]]; then
	echo "clusterRunPort defaulted to 9000"
	CLUSTER=9000
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
CLUSTER_NAME_PORT_MAP=(["C1"]=$CLUSTER)
CLUSTER_NAME_BUCKET_MAP=(["C1"]=$BUCKET)

# Bucket properties
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active" ["flushEnabled"]=1)
insertPropertyIntoBucketNamePropertyMap "$BUCKET" Bucket1Properties

#MAIN
testForClusterRun
if (($? != 0)); then
	exit $?
fi

setupTopologies
