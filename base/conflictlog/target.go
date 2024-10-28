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
	"fmt"

	"github.com/couchbase/goxdcr/v8/base"
)

// Target describes the target bucket, scope and collection where
// the conflicts will be logged. There are few terms:
// Complete => The triplet (Bucket, Scope, Collection) are populated
// Blacklist => If the target will be used as blacklist target.
// i.e. a special target value indicating blacklisting conflict logging rule.
type Target struct {
	// Bucket is the conflict bucket
	Bucket string `json:"bucket"`

	// NS is namespace which defines scope and collection
	NS base.CollectionNamespace `json:"ns"`
}

func (t Target) String() string {
	return fmt.Sprintf("%v.%v", t.Bucket, t.NS.ToIndexString())
}

func NewTarget(bucket, scope, collection string) Target {
	return Target{
		Bucket: bucket,
		NS: base.CollectionNamespace{
			ScopeName:      scope,
			CollectionName: collection,
		},
	}
}

// Blacklist target is a special value indicating
// that the conflict mapped to this target won't be logged.
var blacklistTarget Target = NewTarget("", "", "")

func (t Target) IsBlacklistTarget() bool {
	return t.SameAs(blacklistTarget)
}

func BlacklistTarget() Target {
	return blacklistTarget
}

// IsComplete implies that all components of the triplet (bucket, scope & collection)
// are populated
func (t Target) IsComplete() bool {
	return t.Bucket != "" && t.NS.ScopeName != "" && t.NS.CollectionName != ""
}

// Returns true if the scope and/or collection used is a system scope and/or collection.
// The criteria checked is the underscore (_) at the beginning of the scope/collection name,
// except the _default scope/collection.
func (t Target) IsSystemTarget() bool {
	if t.NS.ScopeName != base.DefaultScopeCollectionName &&
		len(t.NS.ScopeName) > 0 && t.NS.ScopeName[0] == '_' {
		return true
	}

	if t.NS.CollectionName != base.DefaultScopeCollectionName &&
		len(t.NS.CollectionName) > 0 && t.NS.CollectionName[0] == '_' {
		return true
	}

	return false
}

// The function defaults the collection and scope names to _default if not set.
func (t *Target) Sanitize() {
	if t == nil {
		return
	}

	if t.Bucket == "" {
		// let Validate take care of returning the error
		return
	}

	if t.NS.ScopeName == "" {
		t.NS.ScopeName = base.DefaultScopeCollectionName
	}

	if t.NS.CollectionName == "" {
		t.NS.CollectionName = base.DefaultScopeCollectionName
	}
}

func (t Target) SameAs(other Target) bool {
	return t.Bucket == other.Bucket && t.NS.IsSameAs(other.NS)
}
