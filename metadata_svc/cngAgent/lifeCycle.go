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
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
)

// update updates the local reference cache with the new reference.
func (refCache *referenceCache) update(newRef *metadata.RemoteClusterReference) {
	refCache.mutex.Lock()
	defer refCache.mutex.Unlock()
	refCache.history.LoadFrom(&refCache.reference)
	refCache.reference.LoadFrom(newRef)
}

// clear clears the reference cache, marking the reference as deleted from metakv.
func (refCache *referenceCache) clear() {
	refCache.mutex.Lock()
	defer refCache.mutex.Unlock()
	refCache.history.LoadFrom(&refCache.reference)
	refCache.reference.Clear()
	refCache.refDeletedFromMetakv = true
}

// beginOp attempts to start a new metakv operation.
// If another operation is already in progress, it returns RemoteSyncInProgress error (unless the new op is a delete).
// For delete operations, it preempts any ongoing operation.
// It returns a cancellable context for the operation, a generation number to track ownership, and an error if any.
func (state *metakvOpState) beginOp(ctx context.Context, op MetakvOp) (context.Context, uint32, error) {
	state.mutex.RLock()
	if state.activeOp != nil && op != MetakvOpDel {
		state.mutex.RUnlock()
		return nil, 0, metadata_svc.RemoteSyncInProgress
	}

	state.mutex.RUnlock()

	state.mutex.Lock()
	defer state.mutex.Unlock()

	// After aquiring the write lock, recheck for ongoing operation
	if state.activeOp != nil && op != MetakvOpDel {
		return nil, 0, metadata_svc.RemoteSyncInProgress
	}

	if state.activeOp != nil {
		// preempt the ongoing op since the current op is a delete
		state.activeCancelFunc()
	}

	// prepare for the new op
	cancelCtx, cancel := context.WithCancel(ctx)
	state.activeOp = &op
	state.activeCancelFunc = cancel
	state.gen++
	return cancelCtx, state.gen, nil
}

// endOpNolock ends the metakv operation if the caller still owns it (i.e., the generation number matches).
// It assumes the caller holds the mutex lock.
func (state *metakvOpState) endOpNolock() {
	// Only clear state if the caller still owns it
	// cancel the context to free up resources if not already done
	state.activeCancelFunc()
	state.activeCancelFunc = nil
	state.activeOp = nil
}

var _ metadata_svc.RemoteAgentLifeCycle = &RemoteCngAgent{}

func diagThresholdFor(userInitiated bool) time.Duration {
	if userInitiated {
		return base.DiagNetworkThreshold
	}
	return base.DiagInternalThreshold
}

// Start initializes and starts the RemoteCngAgent for the given remote cluster reference.
// If userInitiated is true, the update is synchronous and persisted to metakv.
func (agent *RemoteCngAgent) Start(newRef *metadata.RemoteClusterReference, userInitiated bool) error {
	if agent == nil {
		return fmt.Errorf("remoteCngAgent.Start: agent is nil")
	}
	if agent.InitDone() {
		return fmt.Errorf("remoteCngAgent.Start: agent already initialized")
	}

	if newRef == nil {
		return fmt.Errorf("RemoteCngAgent.Start: newRef is nil")
	}
	agent.logger.Infof("RemoteCngAgent.Start called for reference %s, userInitiated=%t", newRef.Name(), userInitiated)
	defer agent.logger.Infof("RemoteCngAgent.Start done for reference %s", newRef.Name())

	diagThreshold := diagThresholdFor(userInitiated)
	stopFunc := agent.services.utils.StartDiagStopwatch(fmt.Sprintf("agent.Start(%s, %t)", newRef.Id(), userInitiated), diagThreshold)
	defer stopFunc()

	var err error
	if userInitiated {
		// If user-initiated, we're in the reference creation path;
		// perform a synchronous update.
		err = agent.UpdateReferenceFrom(newRef, true /*writeToMetakv*/)
	} else {
		// If not user-initiated, this can happen in one of two cases:
		//   i) During startup while loading references from metakv, or
		//  ii) Via RemoteClusterServiceCallback() when another node creates a new RemoteClusterReference.
		// In such cases, we skip writing to metakv.
		// The goal is to bring the agent online quickly and let any inconsistencies be corrected by later refresh operations.
		err = agent.UpdateReferenceFrom(newRef, false /*WriteToMetakv*/)
	}

	if err != nil {
		err = fmt.Errorf("failed to start remote agent for reference %s: %w", newRef.Name(), err)
		agent.logger.Error(err.Error())
		return err
	}

	// Set agent InitDone to true
	agent.setInitDone()

	agent.waitGrp.Add(1)
	go agent.runPeriodicRefresh()

	if agent.heartbeatManager.allowedToSendHeartbeats(agent.HostName(), agent.Uuid()) {
		agent.waitGrp.Add(1)
		agent.heartbeatManager.setUUID(newRef.Uuid())
		go agent.runHeartbeatSender()

		agent.waitGrp.Add(1)
		go agent.printHeartbeatStats()
	}

	agent.logger.Infof("successfully started remote agent for reference %s", newRef.Name())
	return nil
}

// Stop gracefully stops the RemoteCngAgent
func (agent *RemoteCngAgent) Stop() {
	select {
	case <-agent.finCh:
		// Agent is already stopped
		return
	default:
		// Close the finCh to signal the agent to stop
		close(agent.finCh)
		// Wait for all the background goroutines to finish
		agent.waitGrp.Wait()
	}
}

// InitDone indicates whether the remote agent has been fully initialized
func (agent *RemoteCngAgent) InitDone() bool {
	return atomic.LoadUint32(&agent.initDone) > 0
}

// SetInitDone marks the agent as fully initialized
func (agent *RemoteCngAgent) setInitDone() {
	atomic.StoreUint32(&agent.initDone, 1)
}

// Darshan TODO: Fix MB-69094
// To allow the cache to be updated in case a user induced ADD/SET/DEL fails due to rev mismatch or key not found.

// AddOp adds the remote cluster reference to metakv.
// If successful, it also updates the local reference cache and triggers metadata change callbacks.
// It returns an error if the add operation fails.
func (agent *RemoteCngAgent) AddOp(ctx context.Context, newRef *metadata.RemoteClusterReference) error {
	key := newRef.Id()
	value, err := newRef.Marshal()
	if err != nil {
		return fmt.Errorf("marshal reference failed: %w", err)
	}
	agent.logger.Infof("Adding remote cluster reference %s to metakv", newRef.Name())
	return agent.persistReference(ctx, MetakvOpAdd, key, value, newRef)
}

// SetOp updates the remote cluster reference in metakv.
// If successful, it also updates the local reference cache and triggers metadata change callbacks.
// It returns an error if the set operation fails.
func (agent *RemoteCngAgent) SetOp(ctx context.Context, newRef *metadata.RemoteClusterReference) error {
	key := newRef.Id()
	value, err := newRef.Marshal()
	if err != nil {
		return fmt.Errorf("marshal reference failed: %w", err)
	}
	agent.logger.Infof("Setting remote cluster reference %s in metakv", newRef.Name())
	return agent.persistReference(ctx, MetakvOpSet, key, value, newRef)
}

// DelOp deletes the remote cluster reference from metakv.
// If successful, it also clears the local reference cache and triggers metadata change callbacks.
// It returns an error if the delete operation fails.
func (agent *RemoteCngAgent) DelOp(ctx context.Context) error {
	agent.refCache.mutex.RLock()
	key := agent.refCache.reference.Id()
	agent.refCache.mutex.RUnlock()
	agent.logger.Infof("Deleting remote cluster reference %s from metakv", key)
	return agent.persistReference(ctx, MetakvOpDel, key, nil, nil)
}

// persistReference performs the given metakv operation (add, set, delete) for the remote cluster reference.
// It manages operation state to ensure only one operation is active at a time (except for deletes, which preempt others).
// After the operation, it updates the local reference cache and triggers metadata change callbacks.
func (agent *RemoteCngAgent) persistReference(ctx context.Context, op MetakvOp, key string, value []byte, newRef *metadata.RemoteClusterReference) error {
	opCtx, gen, err := agent.metakvOpState.beginOp(ctx, op)
	if err != nil {
		return err
	}
	var opErr error
	defer func() {
		agent.metakvOpState.mutex.Lock()
		defer agent.metakvOpState.mutex.Unlock()
		if agent.metakvOpState.gen != gen {
			// Caller no longer owns the operation
			// This can happen if this "operation" was preempted by a delete operation
			return
		}
		agent.metakvOpState.endOpNolock()
		if opErr == nil {
			// Only update cache and notify if the operation was successful
			switch op {
			case MetakvOpDel:
				agent.refCache.clear()
			default:
				agent.refCache.update(newRef)
			}
			agent.callMetadataChangeCb()
		}
	}()

	if opErr = agent.runMetakvOp(opCtx, op, key, value, newRef); opErr != nil {
		opErr = fmt.Errorf("metakv op %v on key %s failed: %w", op, key, opErr)
		agent.logger.Error(opErr.Error())
		return opErr
	}
	agent.logger.Infof("metakv op %v on key %s succeeded", op, key)
	return nil
}

// runMetakvOp executes the specified metakv operation (add, set, delete) for the remote cluster reference.
func (agent *RemoteCngAgent) runMetakvOp(ctx context.Context, op MetakvOp, key string, value []byte, incomingRef *metadata.RemoteClusterReference) error {
	var err error
	// Check if the context has already been cancelled.
	// If it has, return the error immediately.
	// This is the final opportunity to detect cancellation before executing the metakv operation.
	// Any cancellation occurring after this point will not prevent the metakv operation from proceeding.
	// The caller will determine whether to update the local reference cache based on whether it still owns the operation.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	switch op {
	case MetakvOpAdd:
		err = agent.services.metakv.AddSensitiveWithCatalog(metadata_svc.RemoteClustersCatalogKey, key, value)
	case MetakvOpSet:
		err = agent.services.metakv.SetSensitive(key, value, incomingRef.Revision())
	case MetakvOpDel:
		err = agent.services.metakv.DelWithCatalog(metadata_svc.RemoteClustersCatalogKey, key, nil)
	default:
		return fmt.Errorf("invalid metakv op: %v", op)
	}

	if err != nil {
		return err
	}

	// For add and set operations, perform a read-after-write to confirm the write succeeded
	// and to get the latest revision number.
	// This is skipped for delete operations since the reference is being removed.
	if op == MetakvOpAdd || op == MetakvOpSet {
		if err := agent.readAfterWrite(key, incomingRef); err != nil {
			return fmt.Errorf("post-write read failed: %w", err)
		}
	}

	return nil
}

// readAfterWrite performs a read-after-write operation to confirm that the add/set operation succeeded
// This is required for two reasons:
// 1. To ensure read-your-own-write semantics in a distributed metakv environment
// 2. To retrieve the latest revision number assigned by metakv
func (agent *RemoteCngAgent) readAfterWrite(key string, incomingRef *metadata.RemoteClusterReference) error {
	value, rev, err := agent.services.metakv.Get(key)
	if err != nil {
		// Receiving an error from a metakv GET is highly unusual.
		// ns_server may return a non-200 response in two main cases:
		// 1. The requested key does not exist i.e. ns_server returns a 404. In this case, the underlying metakv library
		//    returns "nil" error with value and rev set to nil.
		// 2. The underlying metakv library sees a 500 status code from ns_server. Given that we do a exponential backoff
		//    retry, the chances of seeing a persistent 500 after all the retries is extremely rare. In this extremely unlikely case,
		//    set the revision value on the incomingRef to nil hoping that the revision will be updated in a future metakv callback.
		agent.logger.Warnf("setting revision to nil on reference %s due to metakv GET error: %w", incomingRef.Name(), err)
		incomingRef.SetRevision(nil)
		return nil
	}
	// A nil value indicates that the key does not exist (404 from ns_server).
	// This can happen when "this" node's metakv sees a delete operation that happened on a peer node after the previously performed
	// add/set operation.
	// In this case we log the incident and return an error to the caller.
	if value == nil {
		agent.logger.Warnf("remoteCngAgent.readAfterWrite: reference %s not found in metakv after write", incomingRef.Name())
		return metadata_svc.DeleteAlreadyIssued
	}

	// Reconstruct the reference object from the metakv value.
	refInMetaKv, err := metadata_svc.ConstructRemoteClusterReference(value, rev, true)
	if err != nil {
		// An error is returned if unmarshalling fails
		agent.logger.Errorf("agent.readAfterWrite: unmarshal failed for reference %s. err=%w", incomingRef.Name(), err)
		return base.ErrorUnmarshallFailed
	}
	// Do a sanity check to make sure there has not been any other writer who updated this reference after we've written.
	// read your own write semantics
	if !incomingRef.IsEssentiallySame(refInMetaKv) {
		// If someone did change from underneath, log and return an error.
		agent.logger.Errorf("reference %s changed in metakv after write.", incomingRef.Name())
		return base.ErrorResourceDoesNotMatch
	}
	// Loads revision minus the ActiveHostName and ActiveHttpsHostName
	incomingRef.LoadNonActivesFrom(refInMetaKv)
	return nil
}

// updateReference is the entry point for "clients" to update(add/set) the remote cluster reference.
// If updateMetaKv is true, it writes the new reference to metakv and updates the local cache, triggers callbacks on success.
// If updateMetaKv is false, it just updates the local cache and triggers callbacks.
func (agent *RemoteCngAgent) updateReference(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	agent.refCache.mutex.RLock()
	refPresent := !agent.refCache.reference.IsEmpty()
	userInitiated := updateMetaKv

	// If there are no changes, return
	if refPresent && agent.refCache.reference.IsSame(newRef) {
		agent.refCache.mutex.RUnlock()
		return nil
	}
	// If the reference has already been deleted from metakv, return an error
	if agent.refCache.refDeletedFromMetakv {
		agent.refCache.mutex.RUnlock()
		return metadata_svc.DeleteAlreadyIssued
	}

	// updateReference can be called in the following scenarios:
	// 1. User-initiated add/update via REST API  (updateMetaKv=true)
	// 2. Metakv callback to add/update operation that happened on another node (updateMetaKv=false)
	// These should take priority over any ongoing refresh operation.
	// Hence we abort any ongoing refresh operation and re-enable it after this operation is done.
	needToReenable := agent.AbortAnyOngoingRefresh()
	if needToReenable {
		defer agent.ReenableRefresh()
	}

	// For add operations:
	//   - We always trust the ID of newRef, and rev is ignored since this is a create operation.
	// For update operations, there are two cases:
	//   - User-initiated update:
	//      The "newRef" here represents a brand new reference object created at the REST layer using reference's constructor.
	//      Since its RefID is randomly generated at creation time, we overwrite it
	//      with the existing reference’s RefID and set the revision to current ref's revision.
	//   - System-initiated update:
	//      The "newRef" represents modifications made on the clone of an existing reference,
	//      so it already carries the correct RefID and revision. No special handling is required.
	if refPresent && userInitiated {
		newRef.SetId(agent.refCache.reference.Id())
		newRef.SetRevision(agent.refCache.reference.Revision())
	}

	agent.refCache.mutex.RUnlock()

	switch updateMetaKv {
	case true:
		// If updateMetaKv is true, write the incoming reference to metakv and update the local cache upon success.
		var err error
		if refPresent {
			err = agent.SetOp(context.Background(), newRef)
		} else {
			err = agent.AddOp(context.Background(), newRef)
		}
		if err != nil {
			return err
		}
	case false:
		// If updateMetaKv is false, just update the local cache and trigger callbacks.
		// This can happen during startup or when another node updates a reference.
		agent.refCache.update(newRef)
		agent.callMetadataChangeCb()
	}

	return nil
}

// UpdateReferenceFrom updates the remote agent with the new reference synchronously
func (agent *RemoteCngAgent) UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	return agent.updateReference(newRef, updateMetaKv)
}

// UpdateReferenceFromAsync updates the remote agent with the new reference asynchronously
func (agent *RemoteCngAgent) UpdateReferenceFromAsync(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	// No op
	return nil
}

// callMetadataChangeCb invokes the registered metadata change callback, if any.
func (agent *RemoteCngAgent) callMetadataChangeCb() {
	var id string

	agent.refCache.mutex.RLock()
	if agent.refCache.reference.IsEmpty() && !agent.refCache.history.IsEmpty() {
		id = agent.refCache.history.Id()
	} else {
		id = agent.refCache.reference.Id()
	}
	oldRef := agent.refCache.history.Clone()
	ref := agent.refCache.reference.Clone()
	agent.refCache.mutex.RUnlock()

	agent.mutex.RLock()
	defer agent.mutex.RUnlock()
	if agent.metadataChangeCallback != nil {
		callbackErr := agent.metadataChangeCallback(id, oldRef, ref)
		if callbackErr != nil {
			agent.logger.Error(callbackErr.Error())
		}
	}
}

// DeleteReference clears the reference from the remote agent and optionally deletes it from metakv.
// Can be called in two scenarios:
//  1. User-initiated deletion via REST API, in which case we need to delete the reference from metakv
//  2. RemoteClusterServiceCallback triggered by another node's deletion, in which case there is no need to delete
//     from metakv again as it's already been deleted
func (agent *RemoteCngAgent) DeleteReference(delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	agent.refCache.mutex.RLock()
	clonedRef := agent.refCache.reference.Clone()
	agent.refCache.mutex.RUnlock()

	if delFromMetaKv {
		err := agent.DelOp(context.Background())
		if err != nil {
			return nil, err
		}
		return clonedRef, nil
	}
	agent.refCache.clear()
	// call callbacks to notify the delete op
	agent.callMetadataChangeCb()
	return clonedRef, nil
}

// registerConnErr records a connection error on the remote reference
func (agent *RemoteCngAgent) registerConnErr(ce metadata.ConnErr) {
	agent.refCache.mutex.RLock()
	if !agent.refCache.reference.IsEmpty() {
		agent.refCache.reference.InsertConnError(ce)
	}
	agent.refCache.mutex.RUnlock()
}

// clearConnErrs clears all recorded connection errors on the remote reference
func (agent *RemoteCngAgent) clearConnErrs() {
	agent.refCache.mutex.RLock()
	if !agent.refCache.reference.IsEmpty() {
		agent.refCache.reference.ClearConnErrs()
	}
	agent.refCache.mutex.RUnlock()
}

// getCredentials returns the credentials for the remote cluster
func (agent *RemoteCngAgent) getCredentials() *base.Credentials {
	agent.refCache.mutex.RLock()
	defer agent.refCache.mutex.RUnlock()
	return agent.refCache.reference.Credentials.Clone()
}

// GetGrpcOpts returns the gRPC options for the remote cluster
func (agent *RemoteCngAgent) GetGrpcOpts() *base.GrpcOptions {
	for !agent.InitDone() {
		time.Sleep(100 * time.Millisecond)
	}
	agent.refCache.mutex.RLock()
	// Its ok to ignore the error since the validation is already done at the REST layer
	connStr, _ := agent.refCache.reference.MyConnectionStr()
	// Its ok to ignore the error since the connection string is already validated at the REST layer
	grpcOpts, _ := base.NewGrpcOptionsSecure(connStr, agent.getCredentials, base.DeepCopyByteArray(agent.refCache.reference.Certificates()))
	agent.refCache.mutex.RUnlock()
	return grpcOpts
}
