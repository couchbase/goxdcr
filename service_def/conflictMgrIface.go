/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package service_def

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/hlv"
)

type ConflictManagerIface interface {
	ResolveConflict(source *base.WrappedMCRequest, target *base.SubdocLookupResponse, sourceId, targetId hlv.DocumentSourceId, recycler func(*base.WrappedMCRequest)) error
	SetBackToSource(source *base.WrappedMCRequest, target *base.SubdocLookupResponse, sourceId, targetId hlv.DocumentSourceId, recycler func(*base.WrappedMCRequest)) error
}
