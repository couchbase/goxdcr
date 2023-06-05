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
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
)

type Filter interface {
	// Returns:
	// 1. bool - Whether or not it was a match
	// 2. err code
	// 3. If err is not nil, additional description
	// 4. Total bytes of failed datapool gets - which means len of []byte alloc (garbage)
	// Note - may modify the wrappedUprEvent as it passes through the filter, if txnFilter is on
	FilterUprEvent(wrappedUprEvent *base.WrappedUprEvent) (bool, error, string, int64)

	SetShouldSkipUncommittedTxn(val bool)
	SetShouldSkipBinaryDocs(val bool)
}

type FilterUtils interface {
	// uncompressedUprValue is either the original uprValue OR if snappy compressed, the uncompressed value
	// The data slice will be recycled automatically later
	CheckForTransactionXattrsInUprEvent(uprEvent *memcached.UprEvent, dp base.DataPool, slicesToBeReleased *[][]byte, needToFilterBody bool) (hasTxnXattrs bool, body []byte, endBodyPos int, err error, additionalErrDesc string, totalFailedCnt int64, uncompressedUprValue []byte)
	ProcessUprEventForFiltering(uprEvent *memcached.UprEvent, body []byte, endBodyPos int, dp base.DataPool, flags base.FilterFlagType, slicesBuf *[][]byte) ([]byte, error, string, int64)
	NewDataPool() base.DataPool
}
