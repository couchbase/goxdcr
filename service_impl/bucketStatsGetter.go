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
	// initDoneOnce is used to ensure that the init is done only once
	initDoneOnce sync.Once
	// initDone is used to denote if the init is done
	initDone atomic.Bool
	// finCh is used to denote the end of the lifecycle of the ClusterBucketStatsProvider
	finCh chan bool
	// logger provides access to the logger
	logger *log.CommonLogger
	// subscriptionCounter is used to construct unique subscriber id for bucket topology service
	subscriptionCounter uint64
	// maxConnectionsPerServer is the maximum number of connections per server
	maxConnectionsPerServer int
	// context is used to track the lifecycle of the ClusterBucketStatsProvider
	context context.Context
	// cancelCtx is used to cancel all active operations
	cancelCtx context.CancelFunc
	// mutex is used to protect the operations
	mutex sync.Mutex
}

// NewClusterBucketStatsProvider is the constructor for ClusterBucketStatsProvider
func NewClusterBucketStatsProvider(bucketName string, clusterUuid string, remoteClusterSvc service_def.RemoteClusterSvc, utils utils.UtilsIface, bucketTopologySvc service_def.BucketTopologySvc, maxConnectionsPerServer int, logger *log.CommonLogger) *ClusterBucketStatsProvider {
	context, cancelCtx := context.WithCancel(context.Background())
	return &ClusterBucketStatsProvider{
		bucketName:              bucketName,
		clusterUuid:             clusterUuid,
		remoteClusterSvc:        remoteClusterSvc,
		utils:                   utils,
		logger:                  logger,
		finCh:                   make(chan bool),
		bucketTopologySvc:       bucketTopologySvc,
		maxConnectionsPerServer: maxConnectionsPerServer,
		context:                 context,
		cancelCtx:               cancelCtx,
	}
}

// Init the ClusterBucketStatsProvider
func (provider *ClusterBucketStatsProvider) Init() error {
	var initErr error
	provider.initDoneOnce.Do(func() {
		userAgentStr := base.ComposeHELOMsgKey(fmt.Sprintf("BucketStatsProvider %s", provider.bucketName))
		provider.remoteMemcachedComponent = Component.NewRemoteMemcachedComponent(provider.logger, provider.finCh, provider.utils, provider.bucketName, userAgentStr, provider.maxConnectionsPerServer)
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
			subsId := fmt.Sprintf("bucketInfoGetter_%d", atomic.AddUint64(&provider.subscriptionCounter, 1))
			targetNotificationCh, err := provider.bucketTopologySvc.SubscribeToRemoteBucketFeed(remoteOnlySpec, subsId)
			if err != nil {
				return nil, err
			}
			var latestTargetBucketTopology service_def.TargetNotification
			defer provider.bucketTopologySvc.UnSubscribeRemoteBucketFeed(remoteOnlySpec, subsId)
			select {
			case latestTargetBucketTopology = <-targetNotificationCh:
				defer latestTargetBucketTopology.Recycle()
				kvVBMap := latestTargetBucketTopology.GetTargetServerVBMap()
				return kvVBMap.Clone(), nil
			default:
				return nil, base.ErrorTargetBucketTopologyNotReady
			}
		})

		initErr = base.ExecWithTimeout(provider.remoteMemcachedComponent.InitConnections, base.ShortHttpTimeout, provider.logger)
	})

	if initErr != nil {
		return initErr
	}

	provider.initDone.CompareAndSwap(false, true)

	go provider.remoteMemcachedComponent.MonitorTopology()
	return nil
}

// InitDone indicates whether the provider has been fully initialized
func (provider *ClusterBucketStatsProvider) InitDone() bool {
	return provider.initDone.Load()
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

// canStartOp checks if an operation can be started
func (provider *ClusterBucketStatsProvider) canStartOp() error {
	// check if the provider is initialized
	if !provider.InitDone() {
		return errors.New("ClusterBucketStatsProvider is not initialized")
	}

	// check if the provider is already closed
	if provider.isClosed() {
		return errors.New("ClusterBucketStatsProvider is closed")
	}
	return nil
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
func (provider *ClusterBucketStatsProvider) getFailoverLogFromServer(server string) (base.FailoverLogMapType, map[uint16]error, error) {
	// Acquire a client from the pool
	client, err := provider.remoteMemcachedComponent.AcquireClient(server)
	if err != nil {
		return nil, nil, err
	}

	resp, vbErrorMap, err := provider.utils.GetFailoverLog(client)
	if err != nil {
		if provider.utils.IsSeriousNetError(err) {
			// Connection is broken due to network error, close it instead of returning to pool
			if err := client.Close(); err != nil {
				provider.logger.Warnf("getFailoverLogFromServer: error closing connection for %v: %v", server, err)
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
func (provider *ClusterBucketStatsProvider) getFailoverLogFromServerWithRetry(ctx context.Context, server string) (base.FailoverLogMapType, map[uint16]error, error) {
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
			result, vbErrorMap, err = provider.getFailoverLogFromServer(server)
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
func (provider *ClusterBucketStatsProvider) GetFailoverLog(requestOpts *base.FailoverLogRequest) (*base.BucketFailoverLog, base.ErrorMap, error) {
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

	serverSet, err := provider.getServerListFromVbList(reqCtx, requestOpts.VBuckets)
	if err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: failed to get server list from vbucket list: %w", err)
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
			resp, vbErrorMap, err := provider.getFailoverLogFromServerWithRetry(reqCtx, serverAddr)
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
func (provider *ClusterBucketStatsProvider) getVBucketStatsFromServer(requestOpts *base.VBucketStatsRequest, server string) (base.VBucketStatsMap, error) {
	// Acquire a client from the pool
	client, err := provider.remoteMemcachedComponent.AcquireClient(server)
	if err != nil {
		return nil, err
	}

	resp, err := provider.utils.GetVBucketStats(requestOpts, client)
	if err != nil {
		if provider.utils.IsSeriousNetError(err) {
			// Connection is broken due to network error, delete it instead of returning to pool
			if err := client.Close(); err != nil {
				provider.logger.Warnf("getVBucketStatsFromServer: error closing connection for %v: %v", server, err)
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
func (provider *ClusterBucketStatsProvider) getVBucketStatsFromServerWithRetry(ctx context.Context, requestOpts *base.VBucketStatsRequest, server string) (base.VBucketStatsMap, error) {
	waitTime := base.RemoteMcRetryWaitTime
	var err error
	var result base.VBucketStatsMap
	for i := 1; i <= base.MaxRemoteMcRetry; i++ {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("getVBucketStatsFromServerWithRetry: aborting %w", ctx.Err())
			provider.logger.Warnf(err.Error())
		default:
			result, err = provider.getVBucketStatsFromServer(requestOpts, server)
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
func (provider *ClusterBucketStatsProvider) GetVBucketStats(requestOpts *base.VBucketStatsRequest) (*base.BucketVBStats, base.ErrorMap, error) {
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

	serverSet, err := provider.getServerListFromVbList(reqCtx, requestOpts.VBuckets)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get server list from vbucket list: %w", err)
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
			resp, err := provider.getVBucketStatsFromServerWithRetry(reqCtx, requestOpts, serverAddr)
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
	// initDoneOnce is used to ensure that the init is done only once
	initDoneOnce sync.Once
	// initDone is used to denote if the init is done
	initDone atomic.Bool
	// mutex is used to protect the operations
	mutex sync.Mutex
	// context is used to track the lifecycle of the CngBucketStatsProvider
	context context.Context
	// cancelCtx is used to cancel all active operations
	cancelCtx context.CancelFunc
}

// NewCngBucketStatsProvider is the constructor for CngBucketStatsProvider
func NewCngBucketStatsProvider(bucketName string, utils utils.UtilsIface, logger *log.CommonLogger, getGrpcOpts func() (*base.GrpcOptions, error)) *CngBucketStatsProvider {
	context, cancelCtx := context.WithCancel(context.Background())
	return &CngBucketStatsProvider{
		bucketName:  bucketName,
		utils:       utils,
		logger:      logger,
		getGrpcOpts: getGrpcOpts,
		finCh:       make(chan struct{}),
		context:     context,
		cancelCtx:   cancelCtx,
	}
}

// Init the CngBucketStatsProvider
func (provider *CngBucketStatsProvider) Init() error {
	var initErr error
	provider.initDoneOnce.Do(func() {
		grpcOpts, err := provider.getGrpcOpts()
		if err != nil {
			initErr = fmt.Errorf("failed to get gRPC options: %w", err)
			return
		}
		provider.cngConn, initErr = base.NewCngConn(grpcOpts)
	})

	if initErr != nil {
		return initErr
	}

	provider.initDone.CompareAndSwap(false, true)
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

// InitDone indicates whether the provider has been fully initialized
func (provider *CngBucketStatsProvider) InitDone() bool {
	return provider.initDone.Load()
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

// canStartOp checks if an operation can be started
func (provider *CngBucketStatsProvider) canStartOp() error {
	// check if the provider is initialized
	if !provider.InitDone() {
		return errors.New("CngBucketStatsProvider is not initialized")
	}

	// check if the provider is already closed
	if provider.isClosed() {
		return errors.New("CngBucketStatsProvider is closed")
	}
	return nil
}

// GetFailoverLog returns the failover log of the target bucket via CNG
func (provider *CngBucketStatsProvider) GetFailoverLog(requestOpts *base.FailoverLogRequest) (*base.BucketFailoverLog, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, fmt.Errorf("GetFailoverLog: %w", err)
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
			VbucketIds:     base.ConvertUint16ToUint32(requestOpts.VBuckets),
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
func (provider *CngBucketStatsProvider) GetVBucketStats(requestOpts *base.VBucketStatsRequest) (*base.BucketVBStats, base.ErrorMap, error) {
	if err := requestOpts.Validate(); err != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: invalid request options: %w", err)
	}

	// check if the operation can be started
	if err := provider.canStartOp(); err != nil {
		return nil, nil, fmt.Errorf("GetVBucketStats: %w", err)
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
			VbucketIds:     base.ConvertUint16ToUint32(requestOpts.VBuckets),
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
