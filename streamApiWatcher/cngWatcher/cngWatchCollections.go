package cngWatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/pkg/errors"
)

// OnMessage is called when a new message is received from the stream.
// It validates the message, converts it to CollectionsManifest, and caches the result.
// Invalid messages are logged and ignored.
func (cw *CollectionsWatcher) OnMessage(msg *internal_xdcr_v1.WatchCollectionsResponse) {
	if msg == nil {
		// this should never happen
		cw.logger.Errorf("CollectionsWatcher: OnMessage() called with nil msg for bucket %v, ignoring msg", cw.bucketName)
		return
	}

	// Load and validate the manifest from the WatchCollectionsResponse
	manifest := &metadata.CollectionsManifest{}
	if err := manifest.LoadFromWatchCollectionsResp(msg); err != nil {
		cw.logger.Errorf("CollectionsWatcher: OnMessage() received invalid msg for bucket %v: %v, ignoring msg", cw.bucketName, err)
		return
	}

	cw.cache.mutex.Lock()
	defer cw.cache.mutex.Unlock()

	cw.cache.currVal = manifest

	// When the first valid message is received, we close the initDone channel
	cw.cache.once.Do(func() {
		close(cw.cache.initDone)
	})
}

// OnError is called if an error occurs during streaming
func (cw *CollectionsWatcher) OnError(err error) {
	cw.opState.mutex.RLock()
	defer cw.opState.mutex.RUnlock()

	if !cw.opState.active {
		// this should never happen
		cw.logger.Errorf("CollectionsWatcher: OnError() called for bucket %v when the operation is not active", cw.bucketName)
		return
	}

	if cw.opState.errorCh != nil {
		cw.opState.errorCh <- err
	}
}

// OnComplete is called when the stream completes successfully
func (cw *CollectionsWatcher) OnComplete() {
	cw.opState.mutex.RLock()
	defer cw.opState.mutex.RUnlock()

	if !cw.opState.active {
		// this should never happen
		cw.logger.Errorf("CollectionsWatcher: OnComplete() called for bucket %v when the operation is not active", cw.bucketName)
		return
	}

	cw.logger.Infof("watch collections stream for bucket %v completed", cw.bucketName)

	if cw.opState.doneCh != nil {
		// close the done channel to denote that the stream operation has completed
		cw.closeOnce.Do(func() {
			close(cw.opState.doneCh)
		})
	}
}

var ErrWatchCollectionsAlreadyActive error = errors.New("watch collections stream already active")

// beginOp begins the watch collections operation
func (opState *WatchCollectionsOpState) beginOp(ctx context.Context) (context.Context, chan struct{}, chan error, error) {
	opState.mutex.Lock()
	defer opState.mutex.Unlock()
	if opState.active {
		return nil, nil, nil, ErrWatchCollectionsAlreadyActive
	}

	opState.active = true
	opState.doneCh = make(chan struct{})
	opState.errorCh = make(chan error, 1)
	cancelContext, cancelWithCause := context.WithCancelCause(ctx)
	opState.activeOpCancelFunc = cancelWithCause

	return cancelContext, opState.doneCh, opState.errorCh, nil
}

// cancelOp cancels the watch collections operation
func (opState *WatchCollectionsOpState) cancelOp() {
	opState.mutex.RLock()
	defer opState.mutex.RUnlock()
	if !opState.active {
		return
	}
	opState.activeOpCancelFunc(base.ErrorUserInitiatedStreamRpcCancellation)
}

// endOp ends the watch collections operation
func (opState *WatchCollectionsOpState) endOp() {
	opState.mutex.Lock()
	defer opState.mutex.Unlock()
	if !opState.active {
		return
	}

	// cancel the context to free up resources if not already done
	opState.activeOpCancelFunc(nil)

	opState.active = false
	opState.activeOpCancelFunc = nil
	opState.doneCh = nil
	opState.errorCh = nil
}

// getStreamRequest constructs the gRPC request for the WatchCollections operation
func (cw *CollectionsWatcher) getStreamRequest(ctx context.Context, bucketName string) *base.GrpcRequest[*internal_xdcr_v1.WatchCollectionsRequest] {
	return &base.GrpcRequest[*internal_xdcr_v1.WatchCollectionsRequest]{
		Context: ctx,
		Request: &internal_xdcr_v1.WatchCollectionsRequest{
			BucketName: bucketName,
		},
	}
}

// run is the main loop for the CollectionsWatcher
// It starts the stream, handles errors, and retries the stream operation
func (cw *CollectionsWatcher) run() {
	defer func() {
		cw.logger.Infof("collectionsWatcher's run loop for bucket %v ended", cw.bucketName)
		cw.waitGrp.Done()
	}()

	var currentWait time.Duration = cw.waitTime
	for {
		ctx, doneCh, errorCh, err := cw.opState.beginOp(context.Background())
		if err != nil {
			cw.logger.Errorf("failed to begin watch collections operation for bucket %v: %v", cw.bucketName, err)
			return
		}

		streamReq := cw.getStreamRequest(ctx, cw.bucketName)
		go cw.utils.CngWatchCollections(cw.cngConn.Client(), streamReq, cw)

		select {
		case <-cw.finCh:
			// watcher is stopped, end the stream
			cw.opState.cancelOp()
			// wait for the stream to gracefully terminate
			select {
			case <-doneCh:
				// stream terminated successfully
			case err := <-errorCh:
				// stream terminated with an error
				cw.logger.Errorf("error occurred while terminating the watch collections stream for bucket %v: %v", cw.bucketName, err)
			}
			// end the operation
			cw.opState.endOp()
			return
		case err := <-errorCh:
			// log the error and end the operation
			cw.logger.Errorf("error occurred during watch collections stream for bucket %v: %v", cw.bucketName, err)

			if !cw.retry {
				// do not retry if retry is false
				cw.opState.endOp()
				return
			}

			// retry the operation
			timer := time.NewTimer(currentWait)
			select {
			case <-cw.finCh:
				// watcher stopped, end the operation
				cw.opState.endOp()
				return
			case <-timer.C:
				// increment the wait time and retry the operation
				currentWait = minDuration(currentWait*time.Duration(cw.backoffFactor), cw.maxWaitTime)
				cw.opState.endOp()
				continue
			}
		}
	}
}

// Start starts the CollectionsWatcher
func (cw *CollectionsWatcher) Start() {
	cw.logger.Infof("Starting CollectionsWatcher for bucket %v", cw.bucketName)
	var err error
	// Darshan TODO: Ideally we should use the global connection pool instead of creating a new connection here
	// This TODO is a placeholder until we have the conn pool checked in
	cw.cngConn, err = base.NewCngConn(cw.getGrpcOpts())
	if err != nil {
		cw.logger.Errorf("failed to create CNG connection for bucket %v: %v", cw.bucketName, err)
		return
	}
	cw.waitGrp.Add(1)
	go cw.run()
}

// Stop stops the CollectionsWatcher
func (cw *CollectionsWatcher) Stop() {
	close(cw.finCh)
	cw.waitGrp.Wait()
	cw.cngConn.Close()
}

// GetResult retrieves the latest manifest from the CollectionsWatcher.
// It blocks until the manifest is available or the context is done.
// If ctx is done before the manifest is available, it returns a default manifest and ctx.Err().
func (cw *CollectionsWatcher) GetResult(ctx context.Context) (*metadata.CollectionsManifest, error) {
	select {
	case <-cw.cache.initDone:
		return cw.getCurrentManifest(), nil
	case <-ctx.Done():
		// recheck initDone here to avoid a race condition
		// If both the channels are ready, then go does a pseudo random selection between the two channels.
		// This is to avoid the non-deterministic behavior when both channels are ready.
		select {
		case <-cw.cache.initDone:
			return cw.getCurrentManifest(), nil
		default:
			return nil, fmt.Errorf("manifest not received: context canceled or timed out: %w", ctx.Err())
		}
	}
}

// getCurrentManifest safely returns the current manifest.
func (cw *CollectionsWatcher) getCurrentManifest() *metadata.CollectionsManifest {
	cw.cache.mutex.RLock()
	defer cw.cache.mutex.RUnlock()
	return cw.cache.currVal
}

// minDuration returns the smaller of two durations
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
