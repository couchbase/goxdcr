#!/usr/bin/env bash
set -u

# Copyright 2022-Present Couchbase, Inc.
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

if [[ ! -f "./Vagrantfile" ]]; then
	echo "Cannot find vagrant file"
	exit 1
fi

testCasesDirectory="vagrantTestCases"

# Need special vagrant libraries
. ./vagrantProvision.shlib
if (($? != 0)); then
       exit $?
fi

vagrantUp

# main logic all exist elsewhere
. ./commonTestRunner.shlib
if (($? != 0)); then
	exit $?
fi


vagrantHalt
