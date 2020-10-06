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
. ./clusterRunProvision.shlib
if (( $? != 0 ));then
	echo "Provision failed"
	exit $?
fi

. ./testLibrary.shlib
if (( $? != 0 ));then
  echo "testLibrary.shlib failed"
  exit $?
fi

DEFAULT_ADMIN="Administrator"
DEFAULT_PW="wewewe"

CLUSTER_NAME_PORT_MAP=(["C1"]=9000 ["C2"]=9001)
CLUSTER_NAME_XDCR_PORT_MAP=(["C1"]=13000 ["C2"]=13001)

function eventingFunctionUIHandlerTest {
	echo "====== Test eventing function UIHandler ======"
	echo "=== Create library math function sub at XDCR admin port ==="
	curl -X POST \
	  http://localhost:13000/functions/v1/libraries/math/functions/sub \
	  -u $DEFAULT_ADMIN:$DEFAULT_PW \
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
	  -u $DEFAULT_ADMIN:$DEFAULT_PW

	if (($? != 0)); then
		echo "Failed to GET function"
		exit $?
	fi

	echo "=== Delete library math function sub at XDCR admin port ==="
	curl -s -X DELETE http://localhost:13000/functions/v1/libraries/math/functions/sub \
	  -u $DEFAULT_ADMIN:$DEFAULT_PW

	if (($? != 0)); then
		echo "Failed to DELETE function"
		exit $?
	fi
}

function configureResolver {
  echo "=== Configure Resolver ==="
  WorkersPerNode=2
  ThreadsPerWorker=4
  setInternalSettings "C1" "JSEngineWorkersPerNode"=$WorkersPerNode "JSEngineThreadsPerWorker"=$ThreadsPerWorker
  echo "Setting JSEngineWorkersPerNode and JSEngineThreadsPerWorker. Sleep 10 seconds for XDCR to reboot"
  sleep 10
  setting=`getSpecificInternalSettings "C1" "JSEngineWorkersPerNode"`
  if (( $setting != $WorkersPerNode ));then
    echo "JSEngineWorkersPerNode is $setting, expected $WorkersPerNode"
  else
    echo "JSEngineWorkersPerNode is set to $setting"
  fi
  setting=`getSpecificInternalSettings "C1" "JSEngineThreadsPerWorker"`
  if (( $setting != $ThreadsPerWorker ));then
    echo "JSEngineThreadsPerWorker is $setting, expected $ThreadsPerWorker"
  else
    echo "JSEngineThreadsPerWorker is set to $setting"
  fi
}
eventingFunctionUIHandlerTest
configureResolver
