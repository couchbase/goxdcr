// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

import (
	"github.com/couchbase/goxdcr/v8/metadata"
)

//Connector abstracts the logic which moves data from one processing steps to another
type Connector interface {
	Component

	Forward(data interface{}) error

	//get this node's down stream nodes
	DownStreams() map[string]Part

	//add a node to its existing set of downstream nodes
	AddDownStream(partId string, part Part) error

	UpdateSettings(settings metadata.ReplicationSettingsMap) error

	IsStartable() bool
	Start() error
	Stop() error

	GetUpstreamObjRecycler() func(obj interface{})
}
