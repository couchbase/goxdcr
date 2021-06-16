// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import "github.com/couchbase/goxdcr/metadata"

// Bucket Topology Service is responsible for coordinating retrieval of bucket
// topologies from either local or remote nodes in a responsible manner
// and feeding the information back to those who need it
type BucketTopologySvc interface {
	RegisterLocalBucket(spec *metadata.ReplicationSpecification) (BucketTopologySvcWatcher, chan Notification, error)
	RegisterRemoteBucket(spec *metadata.ReplicationSpecification) (BucketTopologySvcWatcher, chan Notification, error)

	UnRegisterLocalBucket(spec *metadata.ReplicationSpecification) error
	UnRegisterRemoteBucket(spec *metadata.ReplicationSpecification) error
}

type Notification interface {
	SourceNotification
	Clone() Notification
	IsSourceNotification() bool
}

type SourceNotification interface {
	GetNumberOfSourceNodes() (int, error)
	GetKvVbMapRO() (map[string][]uint16, error)
}

type BucketTopologySvcWatcher interface {
	Start() error
	Stop() error
}
