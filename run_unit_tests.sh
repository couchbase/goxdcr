#!/bin/bash

# Copyright 2017-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

function man() {
	echo "Usage: $0 [-d <path>] [-l]" 1>&2
	echo ""
	echo "d: Root directory absolute path of goxdcr (default is pwd)" 1>&2
	echo "h: Help" 1>&2
	echo "l: Run all tests including lengthy tests (which are not run by default)" 1>&2
	exit 1
}

declare ROOT_DIR
declare runAllTests

while getopts "d:lh" opt; do
	case "${opt}" in
	l)
		runAllTests=1
		;;
	d)
		ROOT_DIR=${OPTARG}
		;;
	h)
		man
		;;
	*)
		man
		;;
	esac
done

if [[ -z "$ROOT_DIR" ]]; then
	ROOT_DIR=$(pwd)
fi

if [[ ${ROOT_DIR:0:1} == "." ]]; then
	echo "Cannot pass in relative path for -d"
	man
fi

declare -a DIRS_WITH_UT
declare -a outputs

DIRS_WITH_UT=(
backfill_manager
base
base/filter
crMeta
factory
hlv
metadata
metadata_svc
parts
peerToPeer
pipeline
pipeline_svc
pipeline_manager
service_impl
streamApiWatcher
utils
)

function killAllBgOnExit {
	for job in $(jobs -p); do
		kill $job
	done
}

trap killAllBgOnExit EXIT

pids=""
totalTasks=0
for directory in ${DIRS_WITH_UT[@]}; do
	cd ${ROOT_DIR}/${directory}
	fileFriendlyFileName=$(echo "${directory}" | sed 's/\//_/g')
	if [[ -z "$runAllTests" ]]; then
		go test -short >/tmp/${fileFriendlyFileName}.out 2>&1 &
	else
		go test >/tmp/${fileFriendlyFileName}.out 2>&1 &
	fi
	lastPid="$!"
	echo "Test $directory with background PID $lastPid"
	outputs[$lastPid]="/tmp/${fileFriendlyFileName}.out"
	pids+=" $lastPid"
	totalTasks=$(($totalTasks + 1))
	pcreTestsFound=false
	for testFile in $(ls *_test.go); do
		if (($(head $testFile | grep -c "build pcre") > 0)); then
			pcreTestsFound=true
			break
		fi
	done
	if [[ "$pcreTestsFound" == "true" ]]; then
		fileFriendlyFileName=$(echo "${directory}" | sed 's/\//_/g')
		if [[ -z "$runAllTests" ]]; then
			go test -short -tags=pcre >/tmp/${fileFriendlyFileName}_pcre.out 2>&1 &
		else
			go test -tags=pcre >/tmp/${fileFriendlyFileName}_pcre.out 2>&1 &
		fi
		lastPid2="$!"
		echo "Test $directory PCRE tests with background PID $lastPid2"
		pids+=" $lastPid2"
		outputs[$lastPid2]="/tmp/${fileFriendlyFileName}_pcre.out"
		totalTasks=$(($totalTasks + 1))
	fi
done
# Do a pretty print progress bar
# https://stackoverflow.com/questions/12498304/using-bash-to-display-a-progress-indicator
echo "Total tasks running: $totalTasks"
count=0
failedCnt=0
pstr="[=======================================================================]"
while (($count < $totalTasks)); do
	replacementPids=""
	for p in $pids; do
		kill -0 $p >/dev/null 2>&1
		if (($? == 0)); then
			# process is still running
			replacementPids+=" $p"
		else
			# process is done running
			count=$(($count + 1))
			wait $p
			if (($? > 0)); then
				echo ""
				echo "PID $p failed unit test"
				failedCnt=$(($failedCnt + 1))
			else
				rm ${outputs[p]}
				unset outputs[p]
			fi
		fi
	done
	pids=$replacementPids
	pd=$(($count * 73 / $totalTasks))
	printf "\r%3d.%1d%% %.${pd}s" $(($count * 100 / $totalTasks)) $((($count * 1000 / $totalTasks) % 10)) $pstr
	sleep 0.5
done

if (($failedCnt > 0)); then
	echo ""
	echo "See ${outputs[@]} for failed test outputs"
	exit 1
fi
