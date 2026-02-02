/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

// Monitor provides the methods to monitor the conflict rate in the system and take
// necessary actions based on it.
type Monitor interface {
	// GetConflictsCount returns the number of conflict detected for a given replication topic.
	GetConflictsCount(topic string) (int64, error)

	// AutoPauseReplication pauses the replication with the input topic.
	AutoPauseReplication(topic string) error

	// RaiseUIError alerts the UI with the input errMsg of a given replication.
	RaiseUIError(topic string, errMsg string) (int64, error)
}
