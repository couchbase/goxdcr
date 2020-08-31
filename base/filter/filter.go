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
	FilterUprEvent(uprEvent *memcached.UprEvent) (bool, error, string, int64)
}

type FilterUtils interface {
	CheckForTransactionXattrsInUprEvent(uprEvent *memcached.UprEvent, dp base.DataPool, slicesToBeReleased *[][]byte, needToFilterBody bool) (hasTxnXattrs bool, body []byte, endBodyPos int, err error, additionalErrDesc string, totalFailedCnt int64)
	ProcessUprEventForFiltering(uprEvent *memcached.UprEvent, body []byte, endBodyPos int, dp base.DataPool, flags base.FilterFlagType, slicesBuf *[][]byte) ([]byte, error, string, int64)
	NewDataPool() base.DataPool
}
