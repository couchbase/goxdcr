#!/usr/bin/env bash

# Copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# A dev script to get CNG up

. ./cngProvision.shlib
. ./testLibrary.shlib

# Usage: setupCng.sh <repoPath> <cngNodeName> [cbHost] [cbPass]
# 1. Generates CA and node certs via setupCNGCerts
# 2. Starts gateway via setupCNG
# 3. Echoes only the PID to stdout (first line) then tails /tmp/cng.log
# 4. On Ctrl-C (SIGINT) or SIGTERM, kills the CNG process and exits
repoPath="$1"
cngNodeName="$2"
cbPort="${3:-$CNG_CB_PORT}"
cbPass="${4:-$CNG_CB_PASS}"

if [[ -z "$repoPath" || -z "$cngNodeName" ]]; then
	echo "ERRO: Missing required args. Usage: setupCng.sh <repoPath> <cngNodeName> [userName] [cbHost] [cbPass]" >&2
	exit 1
fi

# Ensure certs exist
setupCNGCerts "$cngNodeName" || {
	echo "ERRO: setupCNGCerts failed" >&2
	exit 1
}

logFile="$CNG_PATH/$cngNodeName/cng.log"
touch "$logFile" 2>/dev/null || { echo "WARN: Unable to touch $logFile; tail may wait for file creation" >&2; }

echo "" >$logFile

# Start gateway and capture PID
pid=$(setupCNG "$repoPath" "$cngNodeName" "$cbPort" "$CNG_CB_PASS") || {
	echo "ERRO: setupCNG failed" >&2
	exit 1
}

echo "Gateway PID: $pid"

cleanup() {
	local ec=$?
	echo "INFO: Caught signal - stopping CNG (pid $pid)" >&2
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		# Give it a moment to exit gracefully
		sleep 3

		if kill -0 "$pid" 2>/dev/null; then
			# if the process still exists force kill it
			echo "INFO: Force killing CNG (pid $pid)" >&2
			kill -9 "$pid" 2>/dev/null || true
		fi
	fi

	# stop the tail incase its still running - should not happen
	if [[ -n "$tailPid" ]] && kill -0 "$tailPid" 2>/dev/null; then
		kill "$tailPid" 2>/dev/null || true
	fi

	exit $ec
}

trap cleanup INT TERM

echo "INFO: Tailing $logFile (Ctrl-C to stop)" >&2

# Use tail -F the log file
tail -F "$logFile" &
tailPid=$!

# Wait on tail; if it exits, perform cleanup
wait "$tailPid"

# If tail exited naturally, perform cleanup
cleanup
