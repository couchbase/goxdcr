// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

//Nozzle models the openning that data streaming out of the source system
//and the outlet where the data flowing into the target system.
//
//Nozzle can be opened or closed. An closed nozzle will not allow data flow through
//
//Each nozzle is a goroutine
type Nozzle interface {
	Part

	//Open opens the Nozzle
	//
	//Data can be passed to the downstream
	Open() error

	//Close closes the Nozzle
	//
	//Data can get to this nozzle, but would not be passed to the downstream
	Close() error

	//IsOpen returns true if the nozzle is open; returns false if the nozzle is closed
	IsOpen() bool

	// Returns a list of its responsible VBs (Read-only) and unlocker when done
	ResponsibleVBs() []uint16

	// To avoid garbage
	RecycleDataObj(obj interface{})
}

type OutNozzle interface {
	Nozzle

	SetUpstreamObjRecycler(func(interface{}))

	SetUpstreamErrReporter(func(interface{}))
}
