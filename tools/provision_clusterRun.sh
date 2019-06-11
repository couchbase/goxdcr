#!/bin/bash
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

# set globals
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"
NODEPORTS=(9000 9001)
CLUSTERNAMES=("C1" "C2")
BUCKETNAMES=("B1" "B2")
RAMQUOTA=(100 100)

#MAIN
testForClusterRun
if (( $? != 0 ));then
	exit $?
fi

setupCluster
setupBuckets
createRemoteClusterReference 0 1
createRemoteClusterReference 1 0
createReplication 0 1
createReplication 1 0

runCbWorkloadGen 0
runCbWorkloadGen 1
