// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"context"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"
	"github.com/couchbase/goxdcr/v8/utils"
)

func init() {
	// Register the RemoteCngAgent constructor with the factory function on metadata_svc
	// so that the RemoteCngAgent can be created by the RemoteClusterService when a CNG-target is created.
	metadata_svc.RegisterRemoteCngAgentFactory(NewRemoteCngAgent)
}

// metakvOp represents the type of metakv operation being performed.
type MetakvOp uint8

const (
	// metakvOpAdd represents an metakv add operation.
	MetakvOpAdd MetakvOp = iota
	// metakvOpSet represents an metakv set operation.
	MetakvOpSet
	// metakvOpDel represents an metakv delete operation.
	MetakvOpDel
)

// services encapsulates the external services required by the RemoteCngAgent.
type services struct {
	// utils provides utility functions.
	utils utils.UtilsIface
	// metakv provides access to the metakv related APIs/operations.
	metakv service_def.MetadataSvc
	// uiLog provides access to the UI logging service.
	uiLog service_def.UILogSvc
	// topologySvc provides access to the topology service.
	topologySvc service_def.XDCRCompTopologySvc
}

// referenceCache denotes the in-memory component of the remote cluster reference.
type referenceCache struct {
	// reference denotes the current active reference
	reference metadata.RemoteClusterReference
	// history denotes the previous reference - before the last update
	history metadata.RemoteClusterReference
	// refDeletedFromMetakv indicates whether the reference has been deleted from metakv
	refDeletedFromMetakv bool
	// mutex to protect 'this' struct from concurrent access
	mutex sync.RWMutex
}

// metakvOpState manages the state of metakv operations on the remote cluster reference.
type metakvOpState struct {
	// activeOp is the currently active metakv operation, if any
	activeOp *MetakvOp
	// activeCancelFunc is the cancel function for the active operation's context
	activeCancelFunc context.CancelFunc
	// mutex to protect 'this' struct from concurrent access
	mutex sync.RWMutex
	// gen is a generation counter to track ownership of the active operation
	// It is incremented each time a new operation begins
	gen uint32
}

// refreshState manages the state of refresh operations on the remote cluster reference.
type refreshState struct {
	// active indicates whether there's an ongoing refresh operation
	active bool
	// cancelActiveOp is the cancel function for the active refresh operation's context
	cancelActiveOp context.CancelFunc
	// resultCh denotes list of callers to be notified when the ongoing refresh operation completes
	resultCh []chan error
	// mutex to protect 'this' struct from concurrent access
	mutex sync.RWMutex
	// abortState indicates whether an abort has been requested/acknowledged
	abortState metadata_svc.RefreshAbortState
	// cond is used to signal all the callers waiting for an abort to complete
	cond *sync.Cond
	// isTemporarilyDisabled indicates whether refresh operations are temporarily disabled
	isTemporarilyDisabled bool
}

// targetHealthTracker tracks the health of the remote cluster.
type targetHealthTracker struct {
	// connectivityHelper manages the connectivity status of the remote
	connectivityHelper service_def.ConnectivityHelperSvc
	// configurationChanged indicates whether the configuration has changed and needs to be reported
	configurationChanged bool
	// authErrReportStatus indicates whether an authentication error has been reported or not
	authErrReportStatus metadata_svc.AuthErrReportStatus
	// mutex protects access to the health tracker state
	mutex sync.RWMutex
	// services provides access to the external services used by the health tracker
	services services
	// registerConnErr is a callback to register connection errors on the remote reference
	registerConnErr func(ce metadata.ConnErr)
	// clearConnErrs is a callback to clear connection errors on the remote reference
	clearConnErrs func()
	// number of consecutive connection errors seen
	connErrCount int
}

// heartbeatStats tracks the statistics of the heartbeat operations
type heartbeatStats struct {
	// totalHeartbeatsSent tracks the total number of heartbeat attempts
	totalHeartbeatsSent uint64
	// successfulHeartbeats tracks the number of successful heartbeats
	successfulHeartbeats uint64
	// connectionErrors tracks heartbeats that failed due to connection issues
	connectionErrors uint64
	// otherErrors tracks heartbeats that failed due to other reasons
	otherErrors uint64
	// mutex protects access to the heartbeat stats
	mutex sync.RWMutex
}

// heartBeatManager is responsible for managing and tracking the heartbeat metadata sent to the remote cluster.
type heartBeatManager struct {
	// remoteClusterUuid is the UUID of the remote cluster
	remoteClusterUuid string
	// lastSentHeartbeatMetadata is the last heartbeat metadata sent to the remote cluster
	lastSentHeartbeatMetadata *metadata.HeartbeatMetadata
	// specsReader is used to fetch the replication specs for the remote cluster.
	specsReader service_def.ReplicationSpecReader
	// services provides access to the external services used by the heartbeat tracker
	services services
	// logger for logging
	logger *log.CommonLogger
	// mutex protects "this" struct from concurrent access
	mutex sync.RWMutex
	// heartbeatStats tracks the statistics of the heartbeat operations
	heartbeatStats heartbeatStats
}

type remoteDataProvider struct {
	// bucketManifestWatcher maps the bucket name to the bucket manifest watcher
	bucketManifestWatcher map[string]cngWatcher.GrpcStreamManager[*metadata.CollectionsManifest]
	// mutex protects "this" struct from concurrent access
	mutex sync.RWMutex
	// refCount tracks the number of references to a target bucket
	refCount map[string]uint32
}

// RemoteCngAgent implements the RemoteAgentIface for CNG targets.
type RemoteCngAgent struct {
	// services denotes the external services required by the agent
	services services
	// refCache maintains the current and previous remote cluster references
	refCache referenceCache
	// initDone indicates whether the agent has been fully initialized
	initDone uint32
	// finCh is closed to signal the agent to stop
	finCh chan struct{}
	// logger for logging
	logger *log.CommonLogger
	// waitGrp is the wait group to manage concurrent operations launched by the agent
	waitGrp sync.WaitGroup
	// metaKvOpState manages the state of metakv operations on the reference
	metakvOpState metakvOpState
	// mutex to protect 'this' struct from concurrent access
	mutex sync.RWMutex
	// metadataChangeCallback is the callback to notify if there's any change to the reference
	metadataChangeCallback base.MetadataChangeHandlerCallback
	// refreshState manages the state of refresh operations
	refreshState refreshState
	// capability denotes the remote capability
	capability metadata.Capability
	// healthTracker records the health of the remote cluster
	healthTracker *targetHealthTracker
	// heartbeatManager manages the heartbeat metadata sent to the remote cluster
	heartbeatManager *heartBeatManager
	// remoteDataProvider encapsulates the data providers for the remote cluster
	remoteDataProvider *remoteDataProvider
}

var _ metadata_svc.RemoteAgentIface = &RemoteCngAgent{}

// refreshSnapShot encapsulates the working(transient) state of a remote cluster during refresh operation.
type refreshSnapShot struct {
	groundTruthRef        *metadata.RemoteClusterReference
	workingRef            *metadata.RemoteClusterReference
	knownCapability       metadata.Capability
	currentCapability     metadata.Capability
	promoteStageToPrimary bool
	logger                *log.CommonLogger
	services              services
}

// NewRefreshSnapShot is a constructor for refreshSnapShot.
func newRefreshSnapShot(ref *metadata.RemoteClusterReference, capability metadata.Capability, services services, logger *log.CommonLogger) *refreshSnapShot {
	return &refreshSnapShot{
		// ref is already a clone
		groundTruthRef:  ref,
		workingRef:      ref.Clone(),
		knownCapability: capability,
		services:        services,
		logger:          logger,
	}
}

// NewRemoteCngAgent is a constructor for RemoteCngAgent.
func NewRemoteCngAgent(utils utils.UtilsIface, metakv service_def.MetadataSvc, uiLog service_def.UILogSvc, topologySvc service_def.XDCRCompTopologySvc, specsReader service_def.ReplicationSpecReader, logger *log.CommonLogger) metadata_svc.RemoteAgentIface {
	services := services{
		utils:       utils,
		metakv:      metakv,
		uiLog:       uiLog,
		topologySvc: topologySvc,
	}

	cngAgent := &RemoteCngAgent{
		services: services,
		finCh:    make(chan struct{}),
		logger:   logger,
		heartbeatManager: &heartBeatManager{
			specsReader: specsReader,
			services:    services,
			logger:      logger,
		},
		remoteDataProvider: &remoteDataProvider{
			bucketManifestWatcher: make(map[string]cngWatcher.GrpcStreamManager[*metadata.CollectionsManifest]),
			refCount:              make(map[string]uint32),
		},
	}
	cngAgent.refreshState.cond = sync.NewCond(&cngAgent.refreshState.mutex)

	targetHealthTracker := &targetHealthTracker{
		connectivityHelper: metadata_svc.NewConnectivityHelper(base.RefreshRemoteClusterRefInterval),
		services:           services,
		registerConnErr:    cngAgent.registerConnErr,
		clearConnErrs:      cngAgent.clearConnErrs,
	}
	cngAgent.healthTracker = targetHealthTracker

	return cngAgent
}
