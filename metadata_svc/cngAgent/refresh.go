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
	"fmt"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ metadata_svc.RemoteAgentRefresh = &RemoteCngAgent{}

func (rs *refreshState) beginOp(ctx context.Context) (context.Context, chan error, error) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	// If temporarily disabled, skip starting a new refresh
	if rs.isTemporarilyDisabled {
		return nil, nil, metadata_svc.SetInProgress
	}

	// If there's an ongoing refresh, the caller should piggy back on its result.
	// Hence we return a result channel that the caller can wait on.
	if rs.active > 0 {
		resultCh := make(chan error, 1)
		rs.resultCh = append(rs.resultCh, resultCh)
		return nil, resultCh, metadata_svc.RefreshAlreadyActive
	}

	// No ongoing refresh, start a new one
	rs.active = 1
	cancelContext, cancel := context.WithCancel(ctx)
	rs.cancelActiveOp = cancel
	return cancelContext, nil, nil
}

func (rs *refreshState) endOp(err *error) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	// If there's no active refresh, nothing to do
	if rs.active == 0 {
		return
	}

	// Notify all concurrent callers(if any) of the operation outcome
	for _, ch := range rs.resultCh {
		ch <- *err
		close(ch)
	}

	// cancel the context to free up resources if not already done
	rs.cancelActiveOp()

	// reset the state
	rs.active = 0
	rs.cancelActiveOp = nil
	rs.resultCh = nil

	// Check if the endOp was called due an abort requested
	if rs.abortState == metadata_svc.RefreshAbortRequested {
		rs.abortState = metadata_svc.RefreshAbortAcknowledged
		rs.cond.Broadcast()
	}
}

func (rs *refreshState) abortAnyRefreshOp() (needToReEnable bool) {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	// disable refresh ops temporarily if not already done
	if !rs.isTemporarilyDisabled {
		rs.isTemporarilyDisabled = true
		needToReEnable = true
	}

	// If there's no active refresh, nothing to do
	if rs.active == 0 {
		return
	}

	// If already requested, just wait for it to complete
	if rs.abortState == metadata_svc.RefreshAbortRequested {
		for rs.abortState != metadata_svc.RefreshAbortAcknowledged {
			rs.cond.Wait()
		}
		return
	}

	// Request abort
	rs.abortState = metadata_svc.RefreshAbortRequested
	rs.cancelActiveOp()
	for rs.abortState != metadata_svc.RefreshAbortAcknowledged {
		rs.cond.Wait()
	}
	return

}

func (rs *refreshState) reenableRefresh() {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()
	rs.isTemporarilyDisabled = false
	rs.abortState = metadata_svc.RefreshAbortNotRequested
}

// performRefreshOp performs the actual refresh operation to contact the remote cluster
func (r *refreshSnapShot) performRefreshOp(ctx context.Context) (codes.Code, error) {
	connStr, err := r.workingRef.MyConnectionStr()
	if err != nil {
		return codes.FailedPrecondition, fmt.Errorf("failed to get connection string: %w", err)
	}

	// Helper function to create grpcOpts for a given credential
	createGrpcOpts := func(creds base.Credentials) (*base.GrpcOptions, error) {
		return base.NewGrpcOptionsSecure(connStr, func() *base.Credentials { return creds.Clone() }, base.DeepCopyByteArray(r.workingRef.Certificates()))
	}

	// Helper function to perform the RPC
	callClusterInfo := func(grpcOpts *base.GrpcOptions) *base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse] {
		// Darshan TODO: use the global connection pool instead of creating a new connection here
		// This TODO is a placeholder until we have the conn pool checked in
		cngConn, err := base.NewCngConn(grpcOpts)
		if err != nil {
			return &base.GrpcResponse[*internal_xdcr_v1.GetClusterInfoResponse]{
				Status: status.New(codes.Unknown, err.Error()),
				Error:  err,
			}
		}
		defer cngConn.Close()

		timeoutCtx, cancel := context.WithTimeout(ctx, base.ShortHttpTimeout)
		defer cancel()
		request := &base.GrpcRequest[*internal_xdcr_v1.GetClusterInfoRequest]{
			Context: timeoutCtx,
			Request: &internal_xdcr_v1.GetClusterInfoRequest{},
		}
		return r.services.utils.CngGetClusterInfo(cngConn.Client(), request)
	}

	// Ensure context hasn't been canceled before starting
	select {
	case <-ctx.Done():
		return codes.Canceled, ctx.Err()
	default:
	}

	// Try with primary credentials first
	grpcOpts, err := createGrpcOpts(r.workingRef.Credentials)
	if err != nil {
		return codes.Internal, fmt.Errorf("failed to construct grpcOpts with primary credentials: %w", err)
	}

	clusterInfoResponse := callClusterInfo(grpcOpts)
	if clusterInfoResponse.Code() == codes.Unauthenticated && r.workingRef.HasStagedCreds() {
		r.logger.Infof("%v: authentication failed with primary credentials: %w. Retrying with staged credentials", r.workingRef.Name(), clusterInfoResponse.Err())

		// Ensure context hasn't been canceled before retrying with staged credentials
		select {
		case <-ctx.Done():
			return codes.Canceled, ctx.Err()
		default:
		}

		// Retry with staged credentials
		grpcOpts, err = createGrpcOpts(*r.workingRef.StagedCredentials)
		if err != nil {
			return codes.Internal, fmt.Errorf("failed to construct grpcOpts with staged credentials: %w", err)
		}

		clusterInfoResponse = callClusterInfo(grpcOpts)
		if clusterInfoResponse.Code() == codes.OK {
			r.promoteStageToPrimary = true
		} else {
			// Both primary and staged credentials failed
			r.logger.Infof("%v: failed with staged credentials. statusCode=%v err=%v", r.workingRef.Name(), clusterInfoResponse.Code(), clusterInfoResponse.Err())
			return clusterInfoResponse.Code(), fmt.Errorf("%v: failed to contact target with both credentials", r.workingRef.Name())
		}
	}

	if clusterInfoResponse.Code() != codes.OK {
		return clusterInfoResponse.Code(), fmt.Errorf("failed to contact target: status=%v, err=%w", clusterInfoResponse.Code(), clusterInfoResponse.Err())
	}

	// Validate cluster UUID
	if r.workingRef.Uuid() != clusterInfoResponse.Response().GetClusterUuid() {
		r.logger.Errorf("%v: cluster UUID mismatch. cached=%v, actual=%v", r.workingRef.Name(), r.workingRef.Uuid(), clusterInfoResponse.Response().GetClusterUuid())
		return clusterInfoResponse.Code(), metadata_svc.UUIDMismatchError
	}

	// Promote staged credentials if needed
	if r.promoteStageToPrimary {
		r.workingRef.PromoteStageCredsToPrimary()
		r.logger.Infof("Preparing to promote staged credentials to primary in remote cluster reference %v.", r.workingRef.Name())
	}

	// Update capability
	// Darshan TODO: implement MB-68864

	return clusterInfoResponse.Code(), nil
}

func (r *refreshSnapShot) isPersistenceRequired() bool {
	return !r.groundTruthRef.IsEssentiallySame(r.workingRef)

}

func (agent *RemoteCngAgent) runPeriodicRefresh() {
	defer agent.waitGrp.Done()

	ticker := time.NewTicker(base.RefreshRemoteClusterRefInterval)
	defer ticker.Stop()

	agentName := agent.Name()
	agent.logger.Infof("runPeriodicRefresh for remote cluster %s is started", agentName)

	for {
		select {
		case <-agent.finCh:
			agent.logger.Infof("%v: runPeriodicRefresh exiting", agent.Name())
			return
		case <-ticker.C:
			err := agent.Refresh()
			if err != nil {
				agent.logger.Warnf("%v: Periodic refresher encountered error while doing a refresh. err:%w", agentName, err)
			}
		}
	}
}

func (agent *RemoteCngAgent) Refresh() error {
	opContext, resultCh, err := agent.refreshState.beginOp(context.Background())
	if err != nil {
		if err == metadata_svc.RefreshAlreadyActive {
			// If a refresh is already active, wait for its result
			err = <-resultCh
			agent.logger.Infof("%v: Another refresh was already in progress; piggybacked on it. err:%w", agent.Name(), err)
			return err
		}
		return fmt.Errorf("unable to begin refresh op. err:%w", err)
	}
	var opErr error
	var statusCode codes.Code
	defer agent.refreshState.endOp(&opErr)

	ref, _ := agent.GetReferenceClone(false)
	refreshSnapShot := NewRefreshSnapShot(ref, agent.capability.Clone(), agent.services, agent.logger)
	statusCode, opErr = refreshSnapShot.performRefreshOp(opContext)

	// Update health tracker only if the context is not canceled
	select {
	case <-opContext.Done():
		// Refresh op's context was canceled, don't update health tracker.
		agent.logger.Debugf("%s: skipping health tracker update due to context cancellation", agent.Name())
	default:
		// Update health tracker based on the outcome of the refresh operation.
		agent.healthTracker.updateHealth(agent.HostName(), statusCode, opErr)
	}

	if opErr != nil {
		return opErr
	}

	// Check if the refresh was aborted
	select {
	case <-opContext.Done():
		agent.logger.Infof("%v: refresh operation aborted", agent.Name())
		return opContext.Err()
	default:
	}

	if refreshSnapShot.isPersistenceRequired() {
		updateErr := agent.SetOp(opContext, refreshSnapShot.workingRef)
		if updateErr != nil {
			opErr = fmt.Errorf("failed to persist updated reference during refresh: %w", updateErr)
			return opErr
		}
		if refreshSnapShot.promoteStageToPrimary {
			agent.services.uiLog.Write(fmt.Sprintf("Promoted staged credentials to primary on remote cluster \"%s\".", agent.Name()))
		}
		agent.logger.Infof("%v: Refresh has sucessfully commited changes. Original: %v New: %v",
			refreshSnapShot.groundTruthRef.CloneAndRedact(), refreshSnapShot.workingRef.CloneAndRedact())
	} else {
		agent.logger.Infof("%v: Refresh completed successfully with no actual changes", agent.Name())
	}
	return nil
}

// ReenableRefresh reenables refresh operations if they were previously disabled.
func (agent *RemoteCngAgent) ReenableRefresh() {
	agent.refreshState.reenableRefresh()
}

// AbortAnyOngoingRefresh aborts any ongoing refresh operation
// and returns whether the refresh state needs to be re-enabled.
func (agent *RemoteCngAgent) AbortAnyOngoingRefresh() (needToReEnable bool) {
	return agent.refreshState.abortAnyRefreshOp()
}

// Bootstrap support needs to be added when CNG starts supporting DNS SRV
func (agent *RemoteCngAgent) ClearBootstrap() {
	// no-op
}

func (agent *RemoteCngAgent) IsBootstrap() bool {
	// no-op
	return false
}

func (agent *RemoteCngAgent) SetBootstrap() {
	// no-op
}
