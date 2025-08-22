#!/usr/bin/env bash
set -u

# Copyright 2022-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This is a script that drives vagrant and virtualBox to load a 2-node source cluster and
# a 2-node target cluster with a specified debian build of Couchbase Server
# and then creates bi-directional replications between them

# main logic all exist elsewhere
. ./vagrantProvision.shlib
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
CLUSTER_NAME_PORT_MAP=(["C1"]=15000 ["C1P"]=15001 ["C2"]=15002 ["C2P"]=15003)
CLUSTER_NAME_SSLPORT_MAP=(["C1"]=15020 ["C1P"]=15021 ["C2"]=15022 ["C2P"]=15023)
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=15996 ["C1P"]=15997 ["C2"]=15998 ["C2P"]=15999)
CLUSTER_DEPENDENCY_MAP=(["C1P"]="C1" ["C2P"]="C2")
VAGRANT_KV_EXTERNAL_MAP=(["C1"]=15100 ["C1P"]=15101 ["C2"]=15102 ["C2P"]=15103)
VAGRANT_KVSSL_EXTERNAL_MAP=(["C1"]=15200 ["C1P"]=15201 ["C2"]=15202 ["C2P"]=15203)
VAGRANT_CAPI_EXTERNAL_MAP=(["C1"]=15010 ["C1P"]=15011 ["C2"]=15012 ["C2P"]=15013)
VAGRANT_VM_IP_MAP=(["C1"]="192.168.56.2" ["C1P"]="192.168.56.3" ["C2"]="192.168.56.4" ["C2P"]="192.168.56.5")
VAGRANT_VM_IDX_MAP=(["0"]="C1" ["1"]="C1P" ["2"]="C2" ["3"]="C2P")
# Set c1 to have 2 buckets and c2 to have 1 bucket
declare -a cluster1BucketsArr
cluster1BucketsArr=("B0" "B1")
CLUSTER_NAME_BUCKET_MAP=(["C1"]=${cluster1BucketsArr[@]} ["C2"]="B2")

# Bucket properties
declare -A BucketProperty=(["ramQuotaMB"]=100)
declare -A Bucket1Properties=(["ramQuotaMB"]=100 ["CompressionMode"]="Active")
insertPropertyIntoBucketNamePropertyMap "B0" BucketProperty
insertPropertyIntoBucketNamePropertyMap "B1" Bucket1Properties
insertPropertyIntoBucketNamePropertyMap "B2" BucketProperty

declare -A DefaultBucketReplProperties=(["replicationType"]="continuous" ["checkpointInterval"]=60 ["statsInterval"]=500)

# Bucket -> Scopes
# -----------------
declare -a scope1Arr=("S1" "S2")
BUCKET_NAME_SCOPE_MAP=(["B1"]=${scope1Arr[@]} ["B2"]="S1")

# Scopes -> Collections
# ----------------------
declare -a collection1Arr=("col1" "col2")
declare -a collection2Arr=("col1" "col2" "col3")
SCOPE_NAME_COLLECTION_MAP=(["S1"]=${collection1Arr[@]} ["S2"]=${collection2Arr[@]} ["S3"]=${collection2Arr[@]})

function runDataLoad {
	# Run CBWorkloadgen in parallel
	runCbWorkloadGenBucket "C1" "B1" &
	runCbWorkloadGenBucket "C2" "B2" &
	runCbWorkloadGenCollection "C1" "B1" "S1" "col1" "col_" &
	waitForBgJobs
}

function setupSGW {
	local clusterName=$1
	vagrantRemoveSGW "$clusterName"
	vagrantInstallSGW "$clusterName" "4.0.0-2"
	vagrantStopSGW "$clusterName"
	vagrantSetConfigSGW "$clusterName"
	vagrantStartSGW "$clusterName"
}

if (($(vagrant plugin list | grep -c scp) == 0)); then
	vagrant plugin install vagrant-scp
fi

if (($(vagrant plugin list | grep -c parallels) == 0)); then
	vagrant plugin install vagrant-parallels
fi

#MAIN
vagrantUp
if (($? != 0)); then
	exit $?
fi

vagrantRemoveCbServerAll
vagrantInstallCBServerAll "trinity"

setupTopologies

addNodesIn
sleep 5
startRebalancing "C1"
startRebalancing "C2"

sleep 5
setupCertsForTesting
vagrantLoadCerts
for clusterName in $(echo ${!CLUSTER_NAME_PORT_MAP[@]}); do
	setEnableClientCert "$clusterName"
done

# Below should be activated once Mobile XDCR intergration is done
#setupSGW "C1"
#setupSGW "C2"
#sleep 5
#vagrantSGWAddDbConfig "C1" "B1" "b1db"
#vagrantSGWAddDbConfig "C2" "B2" "b2db"
# At this point, "runDataLoad" should trigger SGW import

sleep 5
#createRemoteClusterReference "C1" "C2"
#createRemoteClusterReference "C2" "C1"
createSecureRemoteClusterReference "C1" "C2" "${CLUSTER_ROOT_CERTIFICATE_MAP["C2"]}"
createSecureRemoteClusterReference "C2" "C1" "${CLUSTER_ROOT_CERTIFICATE_MAP["C1"]}"
sleep 1

createBucketReplication "C1" "B1" "C2" "B2" DefaultBucketReplProperties
createBucketReplication "C2" "B2" "C1" "B1" DefaultBucketReplProperties

runDataLoad

checkUnidirectionalChangesLeft
dumpDebugInfoBeforeExit
read -p "Press enter to tear-down..."
vagrantHalt
