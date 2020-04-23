#!/usr/bin/env bash

# main logic all exist elsewhere
. ./clusterRunProvision.shlib
if (( $? != 0 ));then
	exit $?
fi

. ./testLibrary.shlib
if (( $? != 0 ));then
	exit $?
fi

declare -i c1Cnt
declare -i c2Cnt
declare -i c1CntNew
declare -i c2CntNew

MSG="Unknown Collection"

function preCheck {
	memcachedLogOutput=`getInternalNodeMemcachedLog "C1"`
	c1Cnt=`echo "$memcachedLogOutput" | grep -c "$MSG"`
	memcachedLogOutput=`getInternalNodeMemcachedLog "C2"`
	c2Cnt=`echo "$memcachedLogOutput" | grep -c "$MSG"`
	echo "PRECHECK Setting c1Cnt to $c1Cnt c2Cnt to $c2Cnt"
}

function postCheck {
	memcachedLogOutput=`getInternalNodeMemcachedLog "C1"`
	c1CntNew=`echo "$memcachedLogOutput" | grep -c "$MSG"`
	memcachedLogOutput=`getInternalNodeMemcachedLog "C2"`
	c2CntNew=`echo "$memcachedLogOutput" | grep -c "$MSG"`

	if (( $c1CntNew > $c1Cnt ));then
		echo "Cluster 1 has hit error - previous count $c1Cnt new Count $c1CntNew rerun"
		return 1
	fi

	if (( $c2CntNew > $c2Cnt ));then
		echo "Cluster 2 has hit error - previous count $c2Cnt new Count $c2CntNew rerun"
		return 1
	fi

	# If postCheck is called, and counts are the same, then it's a legit failure
	exit 1
}

# This is to wrap around the MB-38781 issue

testCasesDirectory="collectionTestcases"
actualScript="./runCollectionsTests.sh"
for testcase in `ls $testCasesDirectory`
do
	testCaseNumber="${testcase:0:2}"
	importProvisionedConfig
	if [[ "$testCaseNumber" == "0_" ]];then
		echo "First time running, no count"
		c1Cnt=0
		c2Cnt=0
	else
		preCheck
	fi
	bash $actualScript $testCaseNumber
	if (( $? == 1 ));then
		importProvisionedConfig
		postCheck
	fi
	while (( $? == 1 ))
	do
		echo "Cleaning up..."
		importProvisionedConfig
		cleanupBucketReplications
		cleanupRemoteClusterRefs
		cleanupBuckets
		sleep 10
		echo "Done cleaning up... rerunning"

		preCheck
		bash $actualScript $testCaseNumber
		if (( $? == 1 ));then
			postCheck
		fi
	done
done

