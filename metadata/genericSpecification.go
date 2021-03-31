/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

type GenericSpecification interface {
	SameSpecGeneric(other GenericSpecification) bool
	CloneGeneric() GenericSpecification
	RedactGeneric() GenericSpecification

	GetReplicationSpec() *ReplicationSpecification
	GetBackfillSpec() *BackfillReplicationSpec
	Type() ReplicationType
	GetFullId() string
}

type ReplicationType int

func (t ReplicationType) String() string {
	switch t {
	case MainReplication:
		return "MainReplication"
	case BackfillReplication:
		return "BackfillReplication"
	default:
		return "?? (ReplicationType)"
	}
}

const (
	MainReplication     ReplicationType = iota
	BackfillReplication ReplicationType = iota
)
