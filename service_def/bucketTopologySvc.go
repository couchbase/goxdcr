// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/metadata"
	"sync"
)

// Bucket Topology Service is responsible for coordinating retrieval of bucket
// topologies from either local or remote nodes in a responsible manner
// and feeding the information back to those who need it
type BucketTopologySvc interface {
	SubscribeToLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan SourceNotification, error)
	SubscribeToRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan TargetNotification, error)

	UnSubscribeLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error

	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error
}

type Notification interface {
	IsSourceNotification() bool
	CloneRO() interface{}
}

type SourceNotification interface {
	Notification
	GetNumberOfSourceNodes() int
	GetSourceVBMapRO() map[string][]uint16
	GetKvVbMapRO() map[string][]uint16
}

type TargetNotification interface {
	Notification
	GetTargetServerVBMap() map[string][]uint16
	GetTargetBucketUUID() string
	GetTargetBucketInfo() map[string]interface{}
}
