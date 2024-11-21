// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
)

type ThroughSeqnoTrackerSvc interface {
	Attach(pipeline common.Pipeline) error
	// get through seqno for a vb
	GetThroughSeqno(vbno uint16) uint64
	// get through seqnos for all vbs managed by the pipeline
	GetThroughSeqnos() map[uint16]uint64
	SetStartSeqno(vbno uint16, seqno uint64, manifestIds base.CollectionsManifestIdsTimestamp)
	PrintStatusSummary()

	// Get ManifestIDs alongside with the through sequence numbers
	GetThroughSeqnosAndManifestIds() (throughSeqnos, srcManifestIds, tgtManifestIds map[uint16]uint64)
}
