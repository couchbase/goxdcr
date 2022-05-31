#!/usr/bin/env bash
set -u

# Copyright 2022-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Need this before running this script:
#   cluster_run --serverless -n 2

# main logic all exist elsewhere
. ./clusterRunProvision.shlib
if (($? != 0)); then
	exit $?
fi

. ./testLibrary.shlib
if (($? != 0)); then
	exit $?
fi

# set globals
# -----------------
DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

testForClusterRun
if (($? != 0)); then
	exit $?
fi

testCaseNumber="${1:-}"
testCasesDirectory="serverlessTestcases"

if [[ -z "$testCaseNumber" ]]; then
	for testcase in $(ls $testCasesDirectory); do
		if [[ "$testcase" =~ _idle_ ]]; then
			# test cases with _idle_ in the filename means they take too long for the whole suite
			# and should be run on an individual basis only
			continue
		fi
		. $testCasesDirectory/$testcase
		runTestCase
	done
else
	testCase=$(find $testCasesDirectory/${testCaseNumber}*)
	if [[ -z "$testCase" ]]; then
		echo "Cannot find test case number $testCaseNumber"
		exit 1
	fi
	. $testCase
	runTestCase
fi
