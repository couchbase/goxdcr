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

. ./testLibrary.shlib
if (( $? != 0 ));then
	exit $?
fi

# set globals
# -----------------
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

testForClusterRun
if (( $? != 0 ));then
	exit $?
fi

testCasesDirectory="collectionTestcases"
for testcase in `ls $testCasesDirectory`
do
	. $testCasesDirectory/$testcase
	runTestCase
	# Sleeping is needed because if test cases happen too quickly in succession, it's possible KV will
	# will give "NOT_MY_VBUCKET" errors back when opening DCP streams. Right now, checkpointing mechanism
	# hasn't been done yet, so any sort of resume will break
	# TODO MB-38021 - once checkpointing has done
	echo "Sleeping 30 seconds for cleanup"
	sleep 30
done

