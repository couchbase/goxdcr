// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"errors"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

var ErrorBucketTopSvcUndergoingGC = errors.New("Specified bucket/spec is undergoing GC")

// Bucket Topology Service is responsible for coordinating retrieval of bucket
// topologies from either local or remote nodes in a responsible manner
// and feeding the information back to those who need it
type BucketTopologySvc interface {
	SubscribeToLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan SourceNotification, error)
	SubscribeToLocalBucketDcpStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan SourceNotification, error)
	SubscribeToLocalBucketDcpStatsLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan SourceNotification, error)
	SubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan SourceNotification, func(time.Duration), error)
	SubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string, requestedInterval time.Duration) (chan SourceNotification, func(time.Duration), error)
	SubscribeToRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) (chan TargetNotification, error)

	UnSubscribeLocalBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeToLocalBucketDcpStatsFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeToLocalBucketDcpStatsLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeRemoteBucketFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeToLocalBucketHighSeqnosFeed(spec *metadata.ReplicationSpecification, subscriberId string) error
	UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec *metadata.ReplicationSpecification, subscriberId string) error

	// This service also provides a functionality to register a garbage collection function call associated with a
	// specific VB. If the time is up and the VB is not owned by this node, then the garbage collect function will
	// be called
	// If the VB is owned/re-owned by this node, then the registered garbage collect function will not run and be discarded
	// Each requestId is unique. If used again, then the gcFunc will replace the previous instance
	RegisterGarbageCollect(specId string, srcBucketName string, vbno uint16, requestId string, gcFunc func() error, timeToFire time.Duration) error

	ReplicationSpecChangeCallback(id string, oldVal, newVal interface{}, wg *sync.WaitGroup) error
}

type Notification interface {
	IsSourceNotification() bool
	Clone(numOfReaders int) interface{}
	GetReplicasInfo() (int, *base.VbHostsMapType, *base.StringStringMap, []uint16)
	Recycle()
}

type SourceNotification interface {
	Notification
	GetNumberOfSourceNodes() int
	GetSourceVBMapRO() base.KvVBMapType
	GetKvVbMapRO() base.KvVBMapType
	GetDcpStatsMap() base.DcpStatsMapType
	GetDcpStatsMapLegacy() base.DcpStatsMapType
	GetHighSeqnosMap() base.HighSeqnosMapType
	GetHighSeqnosMapLegacy() base.HighSeqnosMapType
	GetSourceStorageBackend() string
	GetSourceCollectionManifestUid() uint64
	GetLocalTopologyUpdatedTime() time.Time
	GetEnableCrossClusterVersioning() bool
	GetVersionPruningWindowHrs() int
	GetVbucketsMaxCas() []interface{}
}

type TargetNotification interface {
	Notification
	GetTargetServerVBMap() base.KvVBMapType
	GetTargetBucketUUID() string
	GetTargetBucketInfo() base.BucketInfoMapType
	GetTargetStorageBackend() string
}
