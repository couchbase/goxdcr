/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base"
)

const (
	// Manager consts
	ConflictManagerLoggerName string = "conflictMgr"
	ConflictLoggerName        string = "conflictLogger"
	MemcachedConnUserAgent    string = "cLog"

	// Logger consts
	LoggerShutdownChCap       int    = 10
	DefaultPoolConnLimit      int    = 10
	HibernationSystemErrUIStr string = "Conflict logger saw %d timeout or throttling errors continuously for %d seconds. " +
		"It will not accept any logging requests for the next %d minutes. " +
		"Manually disable the feature, make sure the system is in a good state, and re-enable the feature to continue logging."
	HibernationRulesErrUIStr string = "The configured conflict logging mapping and rules are invalid. " +
		"One or more buckets or scopes or collections doesn't exist. " +
		"The conflict logger will not accept any logging requests for the next %d minutes. " +
		"Manually disable the feature, make sure the mappings and rules are fixed, then re-enable the feature to continue logging."
	AutoPauseErrUIStr string = "%s: Replication was autopaused because the detected conflict count: %d conflicts exceeded the " +
		"configured threshold: %d conflicts within a monitoring duration of %d seconds."

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

// qosType represents the fault tolerance technique in which the conflict logger
// will react to maintain a reliable performance of the replication to which it
// is attached to. It details the behaviour of the logger when too many conflicts
// are detected for a replication.
type qosType uint32

const (
	// hibernation represents the default QoS mechanism of the conflict logger,
	// wherein the logger will stop accepting requests for some duration, if it
	// observes certain number of errors in a given interval of time. The errors
	// include throttling and timeout errors observed when too many conflicts are
	// detected for a replication.
	hibernation qosType = iota

	// autopauseReplication represents an optional and configurable mechanism wherein
	// the replication will be autopaused if the number of conflicts detected for the
	// replication is greater than a given threshold. When this is the QoS type for a
	// logger, the monitoring of the conflict rate will be done by the CLog manager
	// at the process level and not by individual replication conflict logger instance.
	autopauseReplication
)

// String converts q into a stringer representation.
func (q qosType) String() string {
	switch q {
	case hibernation:
		return "hibernation"
	case autopauseReplication:
		return "autopause_replication"
	default:
		return "<unknown>"
	}
}
