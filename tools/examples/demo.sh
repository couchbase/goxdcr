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

# set globals
# -----------------
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

# =============================
# topological map information
# =============================
# cluster -> Bucket(s)
# -----------------
CLUSTER_NAME_PORT_MAP=(["HQ"]=9000 ["NA"]=9001 ["EU"]=9002)
# Set c1 to have 2 buckets and c2 to have 1 bucket
CLUSTER_NAME_BUCKET_MAP=(["HQ"]="B1"  ["NA"]="B2" ["EU"]="B3")

declare -A BucketProperty=(["ramQuotaMB"]=100)
insertPropertyIntoBucketNamePropertyMap "B1" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B2" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B3" BucketProperty

US_filter='(country == "United States" OR country = "Canada") AND type="brewery"'
NonUS_filter='country != "United States" AND country != "Canada" AND type="brewery"'
declare -A USRepl=(["filterExpression"]="$US_filter" ["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)
declare -A NonUSRepl=(["filterExpression"]="$NonUS_filter" ["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)

function runDataLoad {
	#export CBDOCLOADER="/Users/neil.huang/source/couchbase/install/bin/cbdocloader"
	beerSample=`locate beer-sample.zip | grep install | grep samples | head -n 1`
	runDocLoader "HQ" "B1" "$beerSample"
}

function demoEcho {
	echo "=============================================================="
	echo $1
	echo "=============================================================="
	read -p ""
}

#MAIN
testForClusterRun
if (( $? != 0 ));then
	exit $?
fi

demoEcho "1. Setting up 3 clusters -> 1. HQ, 2. North America, 3. Europe"
setupCluster
demoEcho "2. Setting up 1 bucket per cluster -> 1. B1 2. B2 3. B3"
setupBuckets
# Wait for vbuckets and all the other things to propagate before XDCR provisioning
#sleep 1
demoEcho "3. Setting up remote cluster references on HQ to US"
createRemoteClusterReference "HQ" "NA"
demoEcho "4. Setting up remote cluster references on HQ to Non-US"
createRemoteClusterReference "HQ" "EU"
demoEcho "5. Loading beer-sample on HQ"
runDataLoad

demoEcho "6. Creating North-America replication with rule: $US_filter"
createBucketReplication "HQ" "B1" "NA" "B2" USRepl
demoEcho "7. Creating European replication with rule: $NonUS_filter"
createBucketReplication "HQ" "B1" "EU" "B3" NonUSRepl

