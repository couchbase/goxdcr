#!/usr/bin/env bash

# Copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# A dev script to get CNG up

if [[ $# -lt 6 ]]; then
	echo "Usage: setupCng.sh <cngNodeName> <repoPath> <clusterAddr> <cbUser> <cbPass> <cngPort>" >&2
	exit 1
fi

. ./clusterRunProvision.shlib
. ./cngProvision.shlib


cngNodeName="$1"
repoPath="$2"
clusterAddr="$3"
cbUser="$4"
cbPass="$5"
cngPort=$6

export cngMode=1

if [[ "${repoPath}" = "default" ]]; then
	export cngPath="https://github.com/couchbase/stellar-gateway"
fi

echo "Initializing CNG setup... $cngPath"
cngSetupInit

cloneStellarGatewayRepo
if [[ $? -ne 0 ]]; then
	echo "ERRO: Failed to clone Stellar Gateway repo" >&2
	exit 1
fi
buildStellarGateway
if [[ $? -ne 0 ]]; then
	echo "ERRO: Failed to build Stellar Gateway" >&2
	exit 1
fi

generateCNGCerts $cngNodeName
if [[ $? -ne 0 ]]; then
	echo "ERRO: Failed to generate CNG certs" >&2
	exit 1
fi

startCNG "$cngNodeName" $clusterAddr $cbUser $cbPass $cngPort
if [[ $? -ne 0 ]]; then
	echo "ERRO: Failed to start CNG" >&2
	exit 1
fi