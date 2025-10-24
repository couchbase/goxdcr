package cngWatcher

import (
	"context"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/pkg/errors"
)

// OnMessage is called when a new message is received from the stream
func (wch *WatchCollectionsHandler) OnMessage(msg *internal_xdcr_v1.WatchCollectionsResponse) {
	wch.cache.mutex.Lock()
	defer wch.cache.mutex.Unlock()

	wch.cache.currVal = msg

	// When the first message is received, we close the initDone channel
	wch.cache.once.Do(func() {
		close(wch.cache.initDone)
	})
}

// OnError is called if an error occurs during streaming
func (wch *WatchCollectionsHandler) OnError(err error) {
	wch.errorCh <- err

	// Close initDone to unblock any waiting GetResult() calls
	wch.cache.once.Do(func() {
		close(wch.cache.initDone)
	})

	close(wch.errorCh)
}

// OnComplete is called when the stream completes successfully
func (wch *WatchCollectionsHandler) OnComplete() {
	close(wch.doneCh)
}

// GetResult returns the result of the handler
func (wch *WatchCollectionsHandler) GetResult() *internal_xdcr_v1.WatchCollectionsResponse {
	<-wch.cache.initDone
	wch.cache.mutex.RLock()
	defer wch.cache.mutex.RUnlock()
	return wch.cache.currVal
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
	cancelContext, cancelWithCause := context.WithCancelCause(ctx)
	opState.activeOpCancelFunc = cancelWithCause
	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	opState.handler = NewWatchCollectionsHandler(doneCh, errorCh)

	return cancelContext, doneCh, errorCh, nil
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
	opState.handler = nil
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
		cw.opState.mutex.RLock()
		go cw.utils.CngWatchCollections(cw.cngConn.Client(), streamReq, cw.opState.handler)
		cw.opState.mutex.RUnlock()
		cw.rpcInitDoneOnce.Do(func() {
			close(cw.rpcInitDone)
		})

		select {
		case <-cw.finCh:
			// watcher is stopped, end the stream
			cw.opState.cancelOp()
			// wait for the stream to gracefully terminate
			<-doneCh
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
// It returns nil if the watcher is inactive — for example, after a failed stream closes and before a new one starts,
// or if the cache was never initialized and the stream failed.
// When the watcher is active, it returns the latest manifest.
func (cw *CollectionsWatcher) GetResult() *metadata.CollectionsManifest {
	<-cw.rpcInitDone

	cw.opState.mutex.RLock()
	defer cw.opState.mutex.RUnlock()
	if !cw.opState.active {
		return nil
	}

	resp := cw.opState.handler.GetResult()
	if resp == nil {
		return nil
	}
	manifest := &metadata.CollectionsManifest{}
	manifest.LoadFromWatchCollectionsResp(resp)
	return manifest
}

// minDuration returns the smaller of two durations
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
