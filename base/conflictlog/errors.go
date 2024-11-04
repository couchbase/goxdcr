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
	ErrWriterClosed          error = errors.New("conflict writer closed")
	ErrQueueFull             error = errors.New("conflict log is full")
	ErrLoggerClosed          error = errors.New("conflict logger is closed")
	ErrEmptyRules            error = errors.New("empty conflict rules")
	ErrClosedConnPool        error = errors.New("use of closed connection pool")
	ErrConflictLoggingIsOff  error = errors.New("conflict logging is off")
	ErrNoChange              error = errors.New("no change in setting value")
	ErrTimeout               error = errors.New("conflict logging write timeout")
	ErrThrottle              error = errors.New("conflict writer is throttled")
	ErrLogWaitAborted        error = errors.New("conflict log handle received abort")

	// errors mapped to setMeta gomemcached responses.
	ErrTMPFAIL           error = errors.New("temporary write failure")
	ErrGuardrail         error = errors.New("guardrail hit")
	ErrEACCESS           error = errors.New("access error")
	ErrImpossibleResp    error = errors.New("impossible write error")
	ErrUnknownResp       error = errors.New("unknown error")
	ErrFatalResp         error = errors.New("fatal error")
	ErrNotMyBucket       error = errors.New("not my bucket")
	ErrUnknownCollection error = errors.New("unknown collection")
)
