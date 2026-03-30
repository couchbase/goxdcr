// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	Component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/streamApiWatcher/cngWatcher"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
)

var _ service_def.BucketStatsProvider = &ClusterBucketStatsProvider{}

// ClusterBucketStatsProvider is responsible for fetching bucket specific statistics from target couchbase cluster
type ClusterBucketStatsProvider struct {
	// bucketName is the name of the target bucket
	bucketName string
	// clusterUuid is the uuid of the remote cluster
	clusterUuid string
	// remoteClusterSvc provides access to the remote cluster service
	remoteClusterSvc service_def.RemoteClusterSvc
	// remoteMemcachedComponent is the component responsible for interacting with the remote memcached server
	remoteMemcachedComponent *Component.RemoteMemcachedComponent
	// utils provides access to the utilities
	utils utils.UtilsIface
	// bucketTopologySvc provides access to the bucket topology service
	bucketTopologySvc service_def.BucketTopologySvc
	// initMu protects initialization, allowing only one goroutine to attempt initialization at a time
	initMtx sync.Mutex
	// initialized is set to true after successful initialization
	initialized atomic.Bool
	// finCh is used to denote the end of the lifecycle of the ClusterBucketStatsProvider
	finCh chan bool
	// logger provides access to the logger
	logger *log.CommonLogger
	// subscriptionCounter is used to construct unique subscriber id for bucket topology service
	subscriptionCounter uint64
	// context is used to track the lifecycle of the ClusterBucketStatsProvider
	context context.Context
	// cancelCtx is used to cancel all active operations
	cancelCtx context.CancelFunc
	// mutex is used to protect the operations
	mutex sync.Mutex
	// getTargetKvVbMap is the function to get the target kv vb map
	getTargetKvVbMap func(spec *metadata.ReplicationSpecification) (base.KvVBMapType, error)
	// remoteMemcachedTunables are the connection pool tunables.
	remoteMemcachedTunables base.RemoteMemcachedTunables
	// remoteMemcachedTunablesMtx protects access to remoteMemcachedTunables
	cachedTunablesMtx sync.RWMutex
}

// NewClusterBucketStatsProvider is the constructor for ClusterBucketStatsProvider.
// The tunables parameter should be read from global settings to ensure persisted
// values are used instead of defaults after a process restart.
func NewClusterBucketStatsProvider(bucketName string, clusterUuid string, remoteClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, bucketTopologySvc service_def.BucketTopologySvc, tunables base.RemoteMemcachedTunables, logger *log.CommonLogger, getTargetKvVbMap func(spec *metadata.ReplicationSpecification) (base.KvVBMapType, error)) *ClusterBucketStatsProvider {
	context, cancelCtx := context.WithCancel(context.Background())
	return &ClusterBucketStatsProvider{
		bucketName:              bucketName,
		clusterUuid:             clusterUuid,
		remoteClusterSvc:        remoteClusterSvc,
		utils:                   utils,
		logger:                  logger,
		finCh:                   make(chan bool),
		bucketTopologySvc:       bucketTopologySvc,
		context:                 context,
		cancelCtx:               cancelCtx,
		getTargetKvVbMap:        getTargetKvVbMap,
		remoteMemcachedTunables: tunables,
	}
}

func (provier *ClusterBucketStatsProvider) SetRemoteMemcachedTunables(tunables base.RemoteMemcachedTunables) {
	provier.cachedTunablesMtx.Lock()
	defer provier.cachedTunablesMtx.Unlock()
	provier.remoteMemcachedTunables = tunables
}

func (provier *ClusterBucketStatsProvider) GetRemoteMemcachedTunables() base.RemoteMemcachedTunables {
	provier.cachedTunablesMtx.RLock()
	defer provier.cachedTunablesMtx.RUnlock()
	return provier.remoteMemcachedTunables
}

// Init the ClusterBucketStatsProvider
func (provider *ClusterBucketStatsProvider) Init() error {
	// Fast path: already initialized successfully
	if provider.initialized.Load() {
		return nil
	}

	provider.initMtx.Lock()
	defer provider.initMtx.Unlock()

	// Double-check after acquiring lock
	if provider.initialized.Load() {
		return nil
	}

	userAgentStr := base.ComposeHELOMsgKey(fmt.Sprintf("BucketStatsProvider %s", provider.bucketName))
	provider.remoteMemcachedComponent = Component.NewRemoteMemcachedComponent(provider.logger, provider.finCh, provider.utils, provider.bucketName, userAgentStr, provider.GetRemoteMemcachedTunables())
	provider.remoteMemcachedComponent.SetRefGetter(func() *metadata.RemoteClusterReference {
		ref, _ := provider.remoteClusterSvc.RemoteClusterByUuid(provider.clusterUuid, false)
		return ref
	}).SetAlternateAddressChecker(func(ref *metadata.RemoteClusterReference) (bool, error) {
		return provider.remoteClusterSvc.ShouldUseAlternateAddress(ref)
	}).SetTargetKvVbMapGetter(func() (base.KvVBMapType, error) {
		remoteOnlySpec := &metadata.ReplicationSpecification{
			TargetClusterUUID: provider.clusterUuid,
			TargetBucketName:  provider.bucketName,
		}
		return provider.getTargetKvVbMap(remoteOnlySpec)
	})

	err := base.ExecWithTimeout(provider.remoteMemcachedComponent.InitConnections, base.ShortHttpTimeout, provider.logger)
	if err != nil {
		return err
	}

	// Only start MonitorTopology, idle connection GC, and mark as initialized on success
	go provider.remoteMemcachedComponent.MonitorTopology()
	go provider.remoteMemcachedComponent.RunIdleConnectionsGC()
	provider.initialized.Store(true)
	return nil
}

// InitDone indicates whether the provider has been fully initialized successfully
func (provider *ClusterBucketStatsProvider) InitDone() bool {
	return provider.initialized.Load()
}

// Close the ClusterBucketStatsProvider
func (provider *ClusterBucketStatsProvider) Close() error {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	select {
	case <-provider.finCh:
		// Already closed
		return nil
	default:
		close(provider.finCh)
		provider.cancelCtx()
	}

	if provider.remoteMemcachedComponent != nil {
		provider.remoteMemcachedComponent.Close()
	}

	return nil
}

// isClosed checks if the provider is closed by checking finCh
func (provider *ClusterBucketStatsProvider) isClosed() bool {
	select {
	case <-provider.finCh:
		return true
	default:
		return false
	}
}

// canStartOp checks if an operation can be started.
// If the provider is not yet initialized, it attempts a lazy Init().
func (provider *ClusterBucketStatsProvider) canStartOp() error {
	if provider.isClosed() {
		return errors.New("ClusterBucketStatsProvider is closed")
	}

	if !provider.InitDone() {
		if err := provider.Init(); err != nil {
			return fmt.Errorf("ClusterBucketStatsProvider lazy init failed: %w", err)
		}
		provider.logger.Infof("ClusterBucketStatsProvider lazy init completed successfully")
	}
	return nil
}

// getServerListAndVbList is a helper function to get the list of servers and vbuckets from the target kv vb map
func (provider *ClusterBucketStatsProvider) getServerListAndVbList(ctx context.Context) (map[string]struct{}, []uint16, error) {
	serverSet := make(map[string]struct{})
	vblist := make([]uint16, 0)

	// if the context is cancelled, return the error
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	kvVbMap, err := provider.remoteMemcachedComponent.TargetKvVbMap()
	if err != nil {
		return nil, nil, err
	}

	for server, vbs := range kvVbMap {
		serverSet[server] = struct{}{}
		vblist = append(vblist, vbs...)
	}

	return serverSet, vblist, nil
}

// getServerListFromVbList is a helper function to get the server list from a vbucket list
func (provider *ClusterBucketStatsProvider) getServerListFromVbList(ctx context.Context, vblist []uint16) (map[string]struct{}, error) {
	serverSet := make(map[string]struct{})

	// if the context is cancelled, return the error
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	kvVbMap, err := provider.remoteMemcachedComponent.TargetKvVbMap()
	if err != nil {
		return nil, err
	}

	indexMap := kvVbMap.CompileLookupIndex()
	for _, vb := range vblist {
		if server, ok := indexMap[vb]; ok {
			serverSet[server] = struct{}{}
		} else {
			return nil, fmt.Errorf("getServerListFromVbList: vbucket %v not found in target kv vb map for bucket %s", vb, provider.bucketName)
		}
	}

	return serverSet, nil
}

// getFailoverLogFromServer is a helper function to get the failover log from a specific server
func (provider *ClusterBucketStatsProvider) getFailoverLogFromServer(server string, dataTransferCtx *utils.Context) (base.FailoverLogMapType, map[uint16]error, error) {
	// Acquire a client from the pool
	client, err := provider.remoteMemcachedComponent.AcquireClient(server)
	if err != nil {
		return nil, nil, err
	}

	resp, vbErrorMap, err := provider.utils.GetFailoverLog(client, dataTransferCtx)
	if err != nil {
		if provider.utils.IsSeriousNetError(err) {
			// Connection is broken due to network error, close it instead of returning to pool
			if discardErr := provider.remoteMemcachedComponent.DiscardClient(server, client); discardErr != nil {
				provider.logger.Warnf("getFailoverLogFromServer: error closing connection for %v: %v", server, discardErr)
			}
		} else {
			// Non-network error, release client to pool for reuse
			provider.remoteMemcachedComponent.ReleaseClient(server, client)
		}
		return nil, nil, err
	}

	// release client to pool
	provider.remoteMemcachedComponent.ReleaseClient(server, client)
	return resp, vbErrorMap, nil
}

// getFailoverLogFromServerWithRetry is a helper function to get the failover log from a specific server with retry
func (provider *ClusterBucketStatsProvider) getFailoverLogFromServerWithRetry(ctx context.Context, server string, dataTransferCtx *utils.Context) (base.FailoverLogMapType, map[uint16]error, error) {
	waitTime := base.RemoteMcRetryWaitTime
	var err error
	var result base.FailoverLogMapType
	var vbErrorMap map[uint16]error
	for i := 1; i <= base.MaxRemoteMcRetry; i++ {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("getFailoverLogFromServerWithRetry: aborting %w", ctx.Err())
			provider.logger.Warnf(err.Error())
		default:
			result, vbErrorMap, err = provider.getFailoverLogFromServer(server, dataTransferCtx)
		}
		if err == nil {
			return result, vbErrorMap, nil
		}
		if provider.IsRetryableError(err) && i != base.MaxRemoteMcRetry {
			base.WaitForTimeoutOrContextDone(waitTime, ctx.Done())
			waitTime *= time.Duration(base.RemoteMcRetryFactor)
		} else {
			break
		}
	}
	return nil, nil, fmt.Errorf("getFailoverLogFromServerWithRetry: failed to get failover log from server %v: %w", server, err)
}

// GetFailoverLog returns the failover log of the target bucket
func (provider *ClusterBucketStatsProvider) GetFailoverLog(requestOpts *base.FailoverLogRequest, dataTransferCtx *utils.Context) (*base.BucketFailoverLog, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, err
	}

	reqCtx, reqCancel := context.WithCancel(provider.context)
	defer reqCancel()

	go func() {
		select {
		case <-requestOpts.FinCh:
			reqCancel()
		case <-reqCtx.Done():
			// either the request is done or provider is closed
			return
		}
	}()

	var serverSet map[string]struct{}
	var err error
	if requestOpts.AllVBuckets {
		serverSet, requestOpts.VBuckets, err = provider.getServerListAndVbList(reqCtx)
		if err != nil {
			return nil, nil, fmt.Errorf("GetFailoverLog: failed to get server list and vbucket list: %w", err)
		}
	} else {
		serverSet, err = provider.getServerListFromVbList(reqCtx, requestOpts.VBuckets)
		if err != nil {
			return nil, nil, fmt.Errorf("GetFailoverLog: failed to get server list from vbucket list: %w", err)
		}
	}

	var (
		failoverlogResult = &base.BucketFailoverLog{FailoverLogMap: make(base.FailoverLogMapType)}
		wg                sync.WaitGroup
		errMap            = make(base.ErrorMap)
		missingVbList     []uint16
		mutex             sync.Mutex
		resultCh          = make(chan base.FailoverLogMapType, len(serverSet))
	)

	// check if the context is cancelled before starting the actual operation
	if reqCtx.Err() != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: context cancelled before starting the operation: %w", reqCtx.Err())
	}

	for server := range serverSet {
		wg.Add(1)
		go func(serverAddr string) {
			defer wg.Done()
			resp, vbErrorMap, err := provider.getFailoverLogFromServerWithRetry(reqCtx, serverAddr, dataTransferCtx)
			if err != nil {
				mutex.Lock()
				errMap[serverAddr] = err
				mutex.Unlock()
			} else {
				resultCh <- resp
				// Merge per-VB errors into the error map
				if len(vbErrorMap) > 0 {
					mutex.Lock()
					for vbno, vbErr := range vbErrorMap {
						errMap[fmt.Sprintf("vb_%d", vbno)] = vbErr
					}
					mutex.Unlock()
				}
			}
		}(server)
	}

	wg.Wait()
	close(resultCh)

	// if the request is cancelled, return an error
	if reqCtx.Err() != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: context cancelled mid way through the operation: %w", reqCtx.Err())
	}

	// collect the results
	for result := range resultCh {
		failoverlogResult.LoadFrom(requestOpts.VBuckets, result)
	}

	if len(failoverlogResult.FailoverLogMap) != len(requestOpts.VBuckets) {
		missingVbList = make([]uint16, 0, len(requestOpts.VBuckets)-len(failoverlogResult.FailoverLogMap))
		for _, vb := range requestOpts.VBuckets {
			if _, ok := failoverlogResult.FailoverLogMap[vb]; !ok {
				missingVbList = append(missingVbList, vb)
			}
		}
		err := fmt.Errorf("failed to fetch failover log for some vbuckets of bucket %s on target cluster %s: %v", provider.bucketName, provider.clusterUuid, missingVbList)
		provider.logger.Warn(err.Error())
		errMap["MissingVbs"] = err
	}

	return failoverlogResult, errMap, nil
}

// getVBucketStatsFromServer is a helper function to get the vbucket stats from a specific server
func (provider *ClusterBucketStatsProvider) getVBucketStatsFromServer(requestOpts *base.VBucketStatsRequest, server string, dataTransferCtx *utils.Context) (base.VBucketStatsMap, error) {
	// Acquire a client from the pool
	client, err := provider.remoteMemcachedComponent.AcquireClient(server)
	if err != nil {
		return nil, err
	}

	resp, err := provider.utils.GetVBucketStats(requestOpts, client, dataTransferCtx)
	if err != nil {
		if provider.utils.IsSeriousNetError(err) {
			// Connection is broken due to network error, close it instead of returning to pool
			if discardErr := provider.remoteMemcachedComponent.DiscardClient(server, client); discardErr != nil {
				provider.logger.Warnf("getVBucketStatsFromServer: error closing connection for %v: %v", server, discardErr)
			}
		} else {
			// Non-network error, release client to pool for reuse
			provider.remoteMemcachedComponent.ReleaseClient(server, client)
		}
		return nil, err
	}

	// release client to pool
	provider.remoteMemcachedComponent.ReleaseClient(server, client)
	return resp, nil
}

// getVBucketStatsFromServerWithRetry is a helper function to get the vbucket stats from a specific server with retry
func (provider *ClusterBucketStatsProvider) getVBucketStatsFromServerWithRetry(ctx context.Context, requestOpts *base.VBucketStatsRequest, server string, dataTransferCtx *utils.Context) (base.VBucketStatsMap, error) {
	waitTime := base.RemoteMcRetryWaitTime
	var err error
	var result base.VBucketStatsMap
	for i := 1; i <= base.MaxRemoteMcRetry; i++ {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("getVBucketStatsFromServerWithRetry: aborting %w", ctx.Err())
			provider.logger.Warnf(err.Error())
		default:
			result, err = provider.getVBucketStatsFromServer(requestOpts, server, dataTransferCtx)
		}
		if err == nil {
			return result, nil
		}
		if provider.IsRetryableError(err) && i != base.MaxRemoteMcRetry {
			base.WaitForTimeoutOrContextDone(waitTime, ctx.Done())
			waitTime *= time.Duration(base.RemoteMcRetryFactor)
		} else {
			break
		}
	}
	return nil, fmt.Errorf("getVBucketStatsFromServerWithRetry: failed to get vbucket stats from server %v: %w", server, err)
}

// GetVBucketStats returns the vbucket stats of the target bucket
func (provider *ClusterBucketStatsProvider) GetVBucketStats(requestOpts *base.VBucketStatsRequest, dataTransferCtx *utils.Context) (*base.BucketVBStats, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, err
	}

	reqCtx, reqCancel := context.WithCancel(provider.context)
	defer reqCancel()

	go func() {
		select {
		case <-requestOpts.FinCh:
			reqCancel()
		case <-reqCtx.Done():
			// either the request is cancelled/done or provider is closed
			return
		}
	}()

	var serverSet map[string]struct{}
	var err error
	if requestOpts.AllVBuckets {
		serverSet, requestOpts.VBuckets, err = provider.getServerListAndVbList(reqCtx)
		if err != nil {
			return nil, nil, fmt.Errorf("GetVBucketStats: failed to get server list and vbucket list: %w", err)
		}
	} else {
		serverSet, err = provider.getServerListFromVbList(reqCtx, requestOpts.VBuckets)
		if err != nil {
			return nil, nil, fmt.Errorf("GetVBucketStats: failed to get server list from vbucket list: %w", err)
		}
	}

	var (
		bucketVBStats = &base.BucketVBStats{VBStatsMap: make(map[uint16]*base.VBucketStats)}
		wg            sync.WaitGroup
		errMap        = make(base.ErrorMap)
		missingVbList []uint16
		mutex         sync.Mutex
		resultCh      = make(chan base.VBucketStatsMap, len(serverSet))
	)

	// check if the context is cancelled before starting the actual operation
	if reqCtx.Err() != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: context cancelled before starting the operation: %w", reqCtx.Err())
	}

	for server := range serverSet {
		wg.Add(1)
		go func(serverAddr string) {
			defer wg.Done()
			resp, err := provider.getVBucketStatsFromServerWithRetry(reqCtx, requestOpts, serverAddr, dataTransferCtx)
			if err != nil {
				mutex.Lock()
				errMap[serverAddr] = err
				mutex.Unlock()
			} else {
				resultCh <- resp
			}
		}(server)
	}

	wg.Wait()
	close(resultCh)

	// if the request is cancelled, return an error
	if reqCtx.Err() != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: context cancelled mid way through the operation: %w", reqCtx.Err())
	}

	// collect the results
	for result := range resultCh {
		bucketVBStats.LoadFrom(requestOpts.VBuckets, result)
	}

	if len(bucketVBStats.VBStatsMap) != len(requestOpts.VBuckets) {
		missingVbList = make([]uint16, 0, len(requestOpts.VBuckets)-len(bucketVBStats.VBStatsMap))
		for _, vb := range requestOpts.VBuckets {
			if _, ok := bucketVBStats.VBStatsMap[vb]; !ok {
				missingVbList = append(missingVbList, vb)
			}
		}
		err := fmt.Errorf("failed to fetch vbucket stats for some vbuckets of bucket %s on target cluster %s: %v", provider.bucketName, provider.clusterUuid, missingVbList)
		provider.logger.Warn(err.Error())
		errMap["MissingVbs"] = err
	}

	return bucketVBStats, errMap, nil
}

// UpdateConnPoolTunables updates the connection pool tunables based on the provided global settings.
// This method should be called when global settings are changed.
func (provider *ClusterBucketStatsProvider) UpdateConnPoolTunables(settings *metadata.GlobalSettings) {
	if provider.remoteMemcachedComponent == nil {
		return
	}
	tunables := base.RemoteMemcachedTunables{
		MaxConnsPerServer: settings.GetRemoteMemcachedConnPoolMaxConns(),
		MinConnsPerServer: settings.GetRemoteMemcachedConnPoolMinConns(),
		GCInterval:        settings.GetRemoteMemcachedConnPoolGCInterval(),
	}
	provider.SetRemoteMemcachedTunables(tunables)
	provider.remoteMemcachedComponent.SetTunables(tunables)
	provider.logger.Infof("Updated connection pool tunables for bucket %s on cluster %s: %+v", provider.bucketName, provider.clusterUuid, tunables)
}

// IsRetryableError checks if the error is retryable
func (provider *ClusterBucketStatsProvider) IsRetryableError(err error) bool {
	// if error is nil, there is no need to retry
	if err == nil {
		return false
	}

	// if error is a serious network error, it is retryable
	if provider.utils.IsSeriousNetError(err) {
		return true
	}

	if errors.Is(err, Component.ErrSslConStrNotFound) {
		// This error indicates that the server is newly added and the SSL connection string map has not been refreshed yet.
		// This is a temporary condition that should be resolved with time, so we consider it retryable.
		return true
	}

	// try to unwrap the error into a memcached response
	if resp, ok := err.(*gomemcached.MCResponse); ok {
		switch resp.Status {
		// Retryable errors - temporary conditions that may resolve
		case gomemcached.TMPFAIL:
			// temporary failure (e.g., during bucket warmup)
			return true
		case gomemcached.ENOMEM:
			// out of memory (temporary memory pressure)
			return true
		case gomemcached.EBUSY:
			// too busy (system overloaded)
			return true
		case gomemcached.LOCKED:
			// resource locked
			return true
		case gomemcached.EINTERNAL:
			// internal error (may be transient)
			return true
		case gomemcached.NOT_SUPPORTED:
			// When XDCR issues a SelectBucket command and the target bucket is not yet
			// running in memcached, memcached creates a config-only bucket. This is a
			// lightweight placeholder that exists until ns_server creates the actual
			// bucket. While in this state, only the following operations are supported:
			//   1. GetClusterConfig
			//   2. GetErrorMap
			//   3. Hello
			//   4. SelectBucket
			//
			// See - https://review.couchbase.org/c/kv_engine/+/177982/21/daemon/cookie.cc#546
			//
			// Any other operation will return NOT_SUPPORTED when XERROR is disabled,
			// or EConfigOnly when XERROR is enabled.
			//
			// Therefore, XDCR should retry until the real bucket is created and replaces
			// the config-only bucket, after which the operations will succeed.
			return true

		// Non-retryable errors
		case gomemcached.NOT_MY_VBUCKET:
			// VBucket doesn't exist or is in dead state
			return false
		case gomemcached.EINVAL:
			return false
		case gomemcached.EACCESS:
			return false
		case gomemcached.KEY_ENOENT:
			return false
		case gomemcached.NO_BUCKET:
			return false
		default:
			// any other status code, we stay conservative and don't retry
			return false
		}
	}

	// unknown error, don't retry by default
	return false
}

var _ service_def.BucketStatsProvider = &CngBucketStatsProvider{}

// CngBucketStatsProvider is responsible for fetching bucket specific statistics from target cluster via CNG
type CngBucketStatsProvider struct {
	// bucketName is the name of the target bucket
	bucketName string
	// clusterUuid is the uuid of the remote cluster
	clusterUuid string
	// vbucketIds contains the vbucket IDs for the target bucket.
	// The list is fetched on demand from CNG and cached, since the
	// vbucket count is fixed for the lifetime of the bucket.
	vbucketIds []uint32
	// vbucketIdsMu protects access to vbucketIds
	vbucketIdsMu sync.RWMutex
	// utils provides access to the utilities
	utils utils.UtilsIface
	// logger provides access to the logger
	logger *log.CommonLogger
	// getGrpcOpts denotes the function to fetch the gRPC options
	getGrpcOpts func() (*base.GrpcOptions, error)
	// cngConn is the connection to the CNG server
	// Darshan TODO: use the global connection pool instead of creating a new connection here
	// This TODO is a placeholder until we have the conn pool checked in
	cngConn *base.CngConn
	// finCh is used to denote the end of the lifecycle of the CngBucketStatsProvider
	finCh chan struct{}
	// initMu protects initialization - allows only one goroutine to attempt init at a time
	initMu sync.Mutex
	// initialized is set to true after successful initialization
	initialized atomic.Bool
	// mutex is used to protect the operations
	mutex sync.Mutex
	// context is used to track the lifecycle of the CngBucketStatsProvider
	context context.Context
	// cancelCtx is used to cancel all active operations
	cancelCtx context.CancelFunc
	// getTargetKvVbMap is the function to get the target kv vb map
	getTargetKvVbMap func(spec *metadata.ReplicationSpecification) (base.KvVBMapType, error)
}

// NewCngBucketStatsProvider is the constructor for CngBucketStatsProvider
func NewCngBucketStatsProvider(bucketName, clusterUuid string, utils utils.UtilsIface, logger *log.CommonLogger, getGrpcOpts func() (*base.GrpcOptions, error), getTargetKvVbMap func(spec *metadata.ReplicationSpecification) (base.KvVBMapType, error)) *CngBucketStatsProvider {
	context, cancelCtx := context.WithCancel(context.Background())
	return &CngBucketStatsProvider{
		bucketName:       bucketName,
		clusterUuid:      clusterUuid,
		utils:            utils,
		logger:           logger,
		getGrpcOpts:      getGrpcOpts,
		finCh:            make(chan struct{}),
		context:          context,
		cancelCtx:        cancelCtx,
		getTargetKvVbMap: getTargetKvVbMap,
	}
}

// Init the CngBucketStatsProvider
func (provider *CngBucketStatsProvider) Init() error {
	// Fast path: already initialized successfully
	if provider.initialized.Load() {
		return nil
	}

	provider.initMu.Lock()
	defer provider.initMu.Unlock()

	// Double-check after acquiring lock
	if provider.initialized.Load() {
		return nil
	}

	grpcOpts, err := provider.getGrpcOpts()
	if err != nil {
		return fmt.Errorf("failed to get gRPC options: %w", err)
	}

	provider.cngConn, err = base.NewCngConn(grpcOpts)
	if err != nil {
		return err
	}

	provider.initialized.Store(true)
	return nil
}

// Close the CngBucketStatsProvider
func (provider *CngBucketStatsProvider) Close() error {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()

	select {
	case <-provider.finCh:
		// Already closed
		return nil
	default:
		close(provider.finCh)
		provider.cancelCtx()

		if provider.cngConn != nil {
			return provider.cngConn.Close()
		}
	}

	return nil
}

// InitDone indicates whether the provider has been fully initialized successfully
func (provider *CngBucketStatsProvider) InitDone() bool {
	return provider.initialized.Load()
}

// isClosed checks if the provider is closed
func (provider *CngBucketStatsProvider) isClosed() bool {
	select {
	case <-provider.finCh:
		return true
	default:
		return false
	}
}

// canStartOp checks if an operation can be started.
// If the provider is not yet initialized, it attempts a lazy Init().
func (provider *CngBucketStatsProvider) canStartOp() error {
	if provider.isClosed() {
		return errors.New("CngBucketStatsProvider is closed")
	}

	if !provider.InitDone() {
		if err := provider.Init(); err != nil {
			return fmt.Errorf("CngBucketStatsProvider lazy init failed: %w", err)
		}
		provider.logger.Infof("CngBucketStatsProvider lazy init completed successfully")
	}
	return nil
}

// GetFailoverLog returns the failover log of the target bucket via CNG
func (provider *CngBucketStatsProvider) GetFailoverLog(requestOpts *base.FailoverLogRequest, dataTransferCtx *utils.Context) (*base.BucketFailoverLog, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: %w", err)
	}

	// Resolve vbucket IDs as uint32 (the format required by gRPC)
	var vbucketIds []uint32
	var err error
	if requestOpts.AllVBuckets {
		vbucketIds, err = provider.getVBucketList()
		if err != nil {
			return nil, nil, fmt.Errorf("GetFailoverLog: failed to get vbucket list: %w", err)
		}
	} else {
		vbucketIds = base.ConvertUint16ToUint32(requestOpts.VBuckets)
	}

	streamHandler := cngWatcher.NewVbucketInfoHandler()

	includeHistory := true
	includeMaxCas := false
	ctx, cancel := context.WithTimeout(provider.context, base.ShortHttpTimeout)
	defer cancel()

	provider.utils.CngGetVbucketInfo(provider.cngConn.Client(), &base.GrpcRequest[*internal_xdcr_v1.GetVbucketInfoRequest]{
		Context: ctx,
		Request: &internal_xdcr_v1.GetVbucketInfoRequest{
			BucketName:     provider.bucketName,
			VbucketIds:     vbucketIds,
			IncludeHistory: &includeHistory,
			IncludeMaxCas:  &includeMaxCas,
		},
	}, streamHandler)

	result, err := streamHandler.GetResult()
	if err != nil {
		// Darshan TODO: Add retry logic here
		// Need a discussion with Brett to see if CNG should retry or not
		return nil, nil, fmt.Errorf("failed to get failover log: %w", err)
	}
	return result.GetBucketFailoverLog(), nil, nil
}

// GetVBucketStats returns the vbucket stats of the target bucket via CNG
func (provider *CngBucketStatsProvider) GetVBucketStats(requestOpts *base.VBucketStatsRequest, dataTransferCtx *utils.Context) (*base.BucketVBStats, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: %w", err)
	}

	// Resolve vbucket IDs as uint32 (the format required by gRPC)
	var vbucketIds []uint32
	var err error
	if requestOpts.AllVBuckets {
		vbucketIds, err = provider.getVBucketList()
		if err != nil {
			return nil, nil, fmt.Errorf("GetVBucketStats: failed to get vbucket list: %w", err)
		}
	} else {
		vbucketIds = base.ConvertUint16ToUint32(requestOpts.VBuckets)
	}

	streamHandler := cngWatcher.NewVbucketInfoHandler()

	includeHistory := false
	includeMaxCas := requestOpts.MaxCasOnly
	ctx, cancel := context.WithTimeout(provider.context, base.ShortHttpTimeout)
	defer cancel()

	provider.utils.CngGetVbucketInfo(provider.cngConn.Client(), &base.GrpcRequest[*internal_xdcr_v1.GetVbucketInfoRequest]{
		Context: ctx,
		Request: &internal_xdcr_v1.GetVbucketInfoRequest{
			BucketName:     provider.bucketName,
			VbucketIds:     vbucketIds,
			IncludeHistory: &includeHistory,
			IncludeMaxCas:  &includeMaxCas,
		},
	}, streamHandler)

	result, err := streamHandler.GetResult()
	if err != nil {
		// Darshan TODO: Add retry logic here
		// Need a discussion with Brett to see if CNG should retry or not
		return nil, nil, fmt.Errorf("failed to get vbucket stats: %w", err)
	}
	return result.GetBucketVBStats(), nil, nil
}

// getVBucketList returns the full vbucket list [0, 1, ..., numVBuckets-1] as uint32.
func (provider *CngBucketStatsProvider) getVBucketList() ([]uint32, error) {
	// Fast path: read-lock to check if cache is already populated
	provider.vbucketIdsMu.RLock()
	if len(provider.vbucketIds) > 0 {
		cached := provider.vbucketIds
		provider.vbucketIdsMu.RUnlock()
		return cached, nil
	}
	provider.vbucketIdsMu.RUnlock()

	remoteOnlySpec := &metadata.ReplicationSpecification{
		TargetClusterUUID: provider.clusterUuid,
		TargetBucketName:  provider.bucketName,
	}
	tgtKvVBMap, err := provider.getTargetKvVbMap(remoteOnlySpec)
	if err != nil {
		return nil, fmt.Errorf("getVBucketList: failed to get target kv vb map: %w", err)
	}

	vbucketIds := []uint16{}
	for _, vblist := range tgtKvVBMap {
		vbucketIds = append(vbucketIds, vblist...)
	}
	result := base.ConvertUint16ToUint32(vbucketIds)

	// acquire write lock to populate cache if still empty
	provider.vbucketIdsMu.Lock()
	if len(provider.vbucketIds) == 0 {
		provider.vbucketIds = result
	}
	cached := provider.vbucketIds
	provider.vbucketIdsMu.Unlock()

	return cached, nil
}
