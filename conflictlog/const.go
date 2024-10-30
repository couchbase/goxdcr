package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base"
)

const (
	// Manager consts
	ConflictManagerLoggerName string = "conflictMgr"
	ConflictLoggerName        string = "conflictLogger"
	MemcachedConnUserAgent    string = "conflictLog"

	// Logger consts
	LoggerShutdownChCap  int = 10
	DefaultPoolConnLimit int = 10

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
