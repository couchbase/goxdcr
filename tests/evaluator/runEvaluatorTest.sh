#!/bin/bash

# Copyright 2017-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

. setup.sh

echo "Test start: "; date

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorError

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorWorkloadMultiFunctionsLargeDocs -test.timeout 30m

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorWorkloadMultiFunctionsSmallDoc

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorWorkloadSingleFunctionLargeDocs -test.timeout 30m

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorWorkloadSingleFunctionSmallDocs

export EVALUATOR_WORKERS=10
export EVALUATOR_QUOTA=1536 #1.5KB
go test -run TestEvaluatorWorkloadSlowFunction

echo "Test end: "; date
