#!/usr/bin/env bash

# Copyright 2019-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

testCasesDirectory="perfTests"

# main logic all exist elsewhere
. ./commonTestRunner.shlib
if (($? != 0)); then
	exit $?
fi
