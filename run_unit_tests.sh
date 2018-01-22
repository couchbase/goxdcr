#!/bin/bash

ROOT_DIR="$1"

if [[ -z "$ROOT_DIR" ]];then
	ROOT_DIR=`pwd`
fi

declare -a DIRS_WITH_UT

DIRS_WITH_UT=(
base
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
done
