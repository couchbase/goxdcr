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
	"time"

	"github.com/couchbase/goxdcr/v8/base"
)

// Logger interface allows logging of conflicts in an abstracted manner
type Logger interface {
	// Id() returns the unique id for the logger
	Id() int64

	// Log writes the conflict to the conflict buccket
	Log(c Conflict) (base.ConflictLoggerHandle, error)

	// UpdateWorkerCount changes the underlying log worker count
	UpdateWorkerCount(count int) error

	// UpdateRules allow updates to the the rules which map
	// the conflict to the target conflict bucket
	UpdateRules(*Rules) error

	// UpdateQueueCapcity changes the underlying queue capcity.
	// The update only happens through this function when cap > exisiting queue capacity.
	// If newCap < oldCap, an error will be returned.
	// The pipeline is expected to restart when newCap < oldCap.
	UpdateQueueCapcity(newCap int) error

	// the following updates are assumed to happen on a not so regular basis.
	// The updates are not protected under any locks.
	UpdateNWRetryCount(cnt int)
	UpdateNWRetryInterval(t time.Duration)
	UpdateSetMetaTimeout(t time.Duration)
	UpdateGetFromPoolTimeout(t time.Duration)

	// Closes the logger. Hence forth the logger will error out
	Close() error
}

type LoggerGetter func() Logger
