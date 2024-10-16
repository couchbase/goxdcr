// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"time"

	"github.com/couchbase/goxdcr/v8/base"
)

type HeartbeatMetadata struct {
	SourceClusterUUID string
	SourceSpecsList   ReplSpecList
	SourceClusterName string
	NodesList         []string
	TTL               time.Duration
}

func (hb *HeartbeatMetadata) SameAs(other *HeartbeatMetadata) bool {
	if hb == nil || other == nil {
		return false
	}

	if hb.SourceClusterUUID != other.SourceClusterUUID ||
		hb.SourceClusterName != other.SourceClusterName ||
		hb.TTL != other.TTL {
		return false
	}

	if !base.EqualStringLists(hb.NodesList, other.NodesList) {
		return false
	}

	return hb.SourceSpecsList.SameAs(other.SourceSpecsList)
}
