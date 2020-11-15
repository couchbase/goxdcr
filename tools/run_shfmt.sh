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
