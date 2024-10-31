// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package common

//Connectable abstracts the step that can be connected to one or more downstream
//steps.
//
//Any processing step which has a downstream step would need to implement this
//
//The processing step that implements Connectable would delegate the job of passing
//data to its downstream steps to its Connector. Different connector can have
//different data passing schem
type Connectable interface {
	Connector() Connector
	SetConnector(connector Connector) error
}

type MultiConnectable interface {
	Connectors() []Connector
}
