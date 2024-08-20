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

type PartState int

const (
	Part_Initial  PartState = iota
	Part_Starting PartState = iota
	Part_Running  PartState = iota
	Part_Stopping PartState = iota
	Part_Stopped  PartState = iota
	Part_Error    PartState = iota
)

type Part interface {
	Component
	Connectable

	//Start makes goroutine for the part working
	Start(settings metadata.ReplicationSettingsMap) error

	//Stop stops the part,
	Stop() error

	//Receive accepts data passed down from its upstream
	Receive(data interface{}) error

	//return the state of the part
	State() PartState

	UpdateSettings(settings metadata.ReplicationSettingsMap) error
}
