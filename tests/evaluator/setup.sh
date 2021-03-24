#!/bin/bash

# Copyright 2017-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

REPO_PATH=$(cd ../../../../../../../; pwd)
export CGO_CFLAGS="-I$REPO_PATH/goproj/src/github.com/couchbase/eventing-ee/evaluator/worker/include -I$REPO_PATH/sigar/include"
export CGO_LDFLAGS="-L $REPO_PATH/install/lib"
export DYLD_LIBRARY_PATH="$REPO_PATH/install/lib"
export GODEBUG=madvdontneed=1
export CBAUTH_REVRPC_URL="http://Administrator:wewewe@127.0.0.1:9000"

