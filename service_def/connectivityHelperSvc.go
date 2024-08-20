// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type ConnectivityHelperSvc interface {
	MarkNodeHeartbeatStatus(nodeName string, heartbeatMap map[string]base.HeartbeatStatus)
	GetOverallStatus() metadata.ConnectivityStatus
	SyncWithValidList(nodeList base.StringPairList)
	MarkNode(nodeName string, status metadata.ConnectivityStatus) (changedState, authErrFixed bool)
	MarkIpFamilyError(bool)
	MarkEncryptionError(val bool)
	String() string
}
