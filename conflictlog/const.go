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
	LoggerShutdownChCap       int    = 10
	DefaultPoolConnLimit      int    = 10
	HibernationSystemErrUIStr string = "Conflict logger saw %d timeout or throttling errors continuously for %d seconds. " +
		"It will not accept any logging requests for the next %d minutes. " +
		"Manually disable the feature, make sure the system is in a good state, and re-enable the feature to continue logging."
	HibernationRulesErrUIStr string = "The configured conflict logging mapping and rules are invalid. " +
		"One or more buckets or scopes or collections doesn't exist. " +
		"It will not accept any logging requests for the next %d minutes. " +
		"Manually disable the feature, make sure the mappings and rules are fixed, then re-enable the feature to continue logging."

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
