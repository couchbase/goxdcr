package conflictlog

import (
	"time"

	"github.com/couchbase/goxdcr/v8/base"
)

const (
	// Manager consts
	ConflictManagerLoggerName        = "conflictMgr"
	ConflictLoggerName        string = "conflictLogger"
	MemcachedConnUserAgent           = "conflictLog"
	DefaultPoolConnLimit             = 10

	// Logger consts
	DefaultSetMetaTimeout       = 5 * time.Second
	DefaultGetTimeout           = 5 * time.Second
	DefaultPoolGetTimeout       = 5 * time.Second
	DefaultNetworkRetryCount    = 5
	DefaultNetworkRetryInterval = 5 * time.Second
	LoggerShutdownChCap         = 10

	// ConnPool consts
	// DefaultPoolGCInterval is the GC frequency for connection pool
	DefaultPoolGCInterval = 60 * time.Second
	// DefaultPoolReapInterval is the last used threshold for reaping unused connections
	DefaultPoolReapInterval = 120 * time.Second

	// ConflictRecord consts
	SourcePrefix    string = "src"
	TargetPrefix    string = "tgt"
	CRDPrefix       string = "crd"
	TimestampFormat string = "2006-01-02T15:04:05.000Z07:00" // YYYY-MM-DDThh:mm:ss:SSSZ format
	// max increase in document body size after adding `"_xdcr_conflict":true` xattr.
	MaxBodyIncrease int = 4 /* entire xattr section size */ +
		4 /* _xdcr_conflict xattr size */ +
		len(base.ConflictLoggingXattrKey) +
		len(base.ConflictLoggingXattrVal) + 2 /* null terminators one each after key and value */
)

// [TEMP] - SUMUKH TODO - can move to const when actually implemented.
var (
	DefaultLoggerWorkerCount = 3
	DefaultLogCapacity       = 5
)
