#!/bin/bash

ROOT_DIR="$1"

if [[ -z "$ROOT_DIR" ]];then
	ROOT_DIR=`pwd`
fi

declare -a DIRS_WITH_UT

DIRS_WITH_UT=(
base
pipeline
pipeline_svc
pipeline_manager
parts
metadata
metadata_svc
utils
)

for directory in ${DIRS_WITH_UT[@]}
do
	cd ${ROOT_DIR}/${directory}
	go test
	result=$?
	if (( !$result == 0 ));then
		exit $result
	fi
	pcreTestsFound=false
	for testFile in `ls *_test.go`
	do
		if (( `head $testFile | grep -c "build pcre"` > 0));then
			pcreTestsFound=true
			break
		fi
	done
	if [[ "$pcreTestsFound" == "true" ]];then
		go test -tags=pcre
		result=$?
		if (( !$result == 0 ));then
			exit $result
		fi
	fi
done
