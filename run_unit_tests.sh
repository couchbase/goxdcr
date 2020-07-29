#!/bin/bash

ROOT_DIR="$1"

if [[ -z "$ROOT_DIR" ]];then
	ROOT_DIR=`pwd`
fi

declare -a DIRS_WITH_UT
declare -a outputs

DIRS_WITH_UT=(
base
backfill_manager
pipeline
pipeline_svc
pipeline_manager
parts
metadata
metadata_svc
service_impl
utils
)

function killAllBgOnExit {
	for job in `jobs -p`;do
		kill $job
	done
}

trap killAllBgOnExit EXIT

pids=""
totalTasks=0
for directory in ${DIRS_WITH_UT[@]}
do
	cd ${ROOT_DIR}/${directory}
	go test  > /tmp/${directory}.out 2>&1 &
	lastPid="$!"
	echo "Test $directory with background PID $lastPid"
	outputs[$lastPid]="/tmp/${directory}.out"
	pids+=" $lastPid"
	totalTasks=$(( $totalTasks + 1 ))
	pcreTestsFound=false
	for testFile in `ls *_test.go`
	do
		if (( `head $testFile | grep -c "build pcre"` > 0));then
			pcreTestsFound=true
			break
		fi
	done
	if [[ "$pcreTestsFound" == "true" ]];then
		go test -tags=pcre > /tmp/${directory}_pcre.out 2>&1 &
		lastPid2="$!"
		echo "Test $directory PCRE tests with background PID $lastPid2"
		pids+=" $lastPid2"
		outputs[$lastPid2]="/tmp/${directory}_pcre.out"
		totalTasks=$(( $totalTasks + 1 ))
	fi
done
# Do a pretty print progress bar
# https://stackoverflow.com/questions/12498304/using-bash-to-display-a-progress-indicator
echo "Total tasks running: $totalTasks"
count=0
failedCnt=0
pstr="[=======================================================================]"
while (( $count < $totalTasks))
do
	replacementPids=""
	for p in $pids
	do
		kill -0 $p > /dev/null 2>&1
		if (( $? == 0 ));then
			# process is still running
			replacementPids+=" $p"
		else
			# process is done running
			count=$(( $count + 1 ))
			wait $p
			if (( $? > 0 ));then
				echo ""
				echo "PID $p failed unit test"
				failedCnt=$(( $failedCnt + 1 ))
			else
				rm ${outputs[p]}
				unset outputs[p]
			fi
		fi
	done
	pids=$replacementPids
	pd=$(( $count * 73 / $totalTasks ))
	printf "\r%3d.%1d%% %.${pd}s" $(( $count * 100 / $totalTasks )) $(( ($count * 1000 / $totalTasks) % 10 )) $pstr
	sleep 0.5
done

if (( $failedCnt > 0 ));then
  echo ""
	echo "See ${outputs[@]} for failed test outputs"
	exit 1
fi
