/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import "errors"

var (
	ErrManagerNotInitialized error = errors.New("conflict manager not initialized")
	ErrWriterTimeout         error = errors.New("conflict writer timed out")
	ErrWriterClosed          error = errors.New("conflict writer closed")
	ErrQueueFull             error = errors.New("conflict log is full")
	ErrLoggerClosed          error = errors.New("conflict logger is closed")
	ErrLogWaitAborted        error = errors.New("conflict log handle received abort")
	ErrEmptyRules            error = errors.New("empty conflict rules")
	ErrUnknownCollection     error = errors.New("unknown collection")
	ErrClosedConnPool        error = errors.New("use of closed connection pool")
	ErrNotMyBucket           error = errors.New("not my bucket")
	ErrConnPoolGetTimeout    error = errors.New("conflict logging pool get timedout")
	ErrSetMetaTimeout        error = errors.New("conflict logging SetMeta timedout")
	ErrConflictLoggingIsOff  error = errors.New("conflict logging is off")
	ErrNoChange              error = errors.New("no change in setting value")
)
