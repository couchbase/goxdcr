#!/usr/bin/env bash
set -u

# Copyright (c) 2020 Couchbase, Inc.
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
(cd .. && . ./clusterRunProvision.shlib)
if (( $? != 0 ));then
	echo "Provision failed"
	exit $?
fi

function eventingFunctionUIHandlerTest {
	echo "====== Test eventing function UIHandler ======"
	echo "=== Create library math function sub at XDCR admin port ==="
	curl -X POST \
	  http://localhost:13000/functions/v1/libraries/math/functions/sub \
	  -u Administrator:wewewe \
	  -H 'content-type: application/json' \
	  -d '{
	  		"name": "sub",
	  		"code": "function sub(a,b) { return helper(a,b); }\n function helper(a,b) { return a - b; }"
		  }'

	if (($? != 0)); then
		echo "Failed to create function"
		exit $?
	fi

	echo "=== Get library math function sub at XDCR admin port ==="
	curl -s -X GET http://localhost:13000/functions/v1/libraries/math/functions/sub \
	  -u Administrator:wewewe

	if (($? != 0)); then
		echo "Failed to GET function"
		exit $?
	fi
	echo

	echo "=== Delete library math function sub at XDCR admin port ==="
	curl -s -X DELETE http://localhost:13000/functions/v1/libraries/math/functions/sub \
	  -u Administrator:wewewe

	if (($? != 0)); then
		echo "Failed to DELETE function"
		exit $?
	fi
}

eventingFunctionUIHandlerTest