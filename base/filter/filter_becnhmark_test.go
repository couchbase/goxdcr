/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package filter

import (
	"fmt"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
)

func BenchmarkFilterUprEvent(b *testing.B) {
	fmt.Println("============== Test case start: BenchmarkFilterUprEvent =================")
	perfDataUprEvent, _ := RetrieveUprFile("../utils/testFilteringData/xattrSlice.bin")
	benchFilter, _ := NewFilter(filterId, "META().xattrs.stringType EXISTS", realUtil, 0, base.MobileCompatibilityOff)

	for n := 0; n < b.N; n++ {
		benchFilter.FilterUprEvent(perfDataUprEvent)
	}

	fmt.Println("============== Test case end: BenchmarkFilterUprEvent =================")
}
