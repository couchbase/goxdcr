#!/usr/bin/env bash
set -u

# Copyright 2020-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

if (( `locate shfmt | grep -c .` == 0 ));then
	echo "shfmt not found"
	exit 1
fi

files=`find -f . | grep "\.sh" | grep -v $0 | grep -v "\.swp"`
for file in `echo "$files"`
do
	if [[ -L "$file" ]];then
		#symlnk
		continue
	fi
	echo "Running shfmt on $file"
	shfmt -w $file
done
