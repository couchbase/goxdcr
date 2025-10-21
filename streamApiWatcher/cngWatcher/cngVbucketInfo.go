package cngWatcher

import (
	"fmt"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
)

// OnMessage is called when a new message is received from the stream
func (vbh *VbucketInfoHandler) OnMessage(msg *internal_xdcr_v1.GetVbucketInfoResponse) {
	vbh.cache.mutex.Lock()
	defer vbh.cache.mutex.Unlock()

	for _, vbInfo := range msg.GetVbuckets() {
		vbh.cache.currVal[vbInfo.GetVbucketId()] = vbInfo
	}
}

// OnError is called if an error occurs during streaming
func (vbh *VbucketInfoHandler) OnError(err error) {
	vbh.errorCh <- err
	close(vbh.errorCh)
}

// OnComplete is called when the stream completes successfully
func (vbh *VbucketInfoHandler) OnComplete() {
	close(vbh.cache.initDone)

	close(vbh.doneCh)
}

// GetResult returns the result of the handler
func (vbh *VbucketInfoHandler) GetResult() (VBucketInfoResponse, error) {
	select {
	case <-vbh.cache.initDone:
		vbh.cache.mutex.RLock()
		defer vbh.cache.mutex.RUnlock()
		return vbh.cache.currVal, nil
	case err := <-vbh.errorCh:
		return nil, fmt.Errorf("error occured while fetching vbucket info: %w", err)
	}
}
