package cngWatcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/stretchr/testify/assert"
)

// Test VbucketInfoHandler OnMessage functionality
func TestVbucketInfoHandler_OnMessage(t *testing.T) {
	assert := assert.New(t)

	// Create handler
	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewVbucketInfoHandler(doneCh, errorCh)
	var maxCas uint64 = 200
	// Create test message
	vbInfo1 := &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
		VbucketId: 0,
		Uuid:      12345,
		MaxCas:    &maxCas,
	}
	vbInfo2 := &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
		VbucketId: 1,
		Uuid:      12346,
		MaxCas:    &maxCas,
	}

	msg := &internal_xdcr_v1.GetVbucketInfoResponse{
		Vbuckets: []*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
			vbInfo1, vbInfo2,
		},
	}

	// Call OnMessage
	handler.OnMessage(msg)
	// Call OnComplete to simulate completion
	handler.OnComplete()

	// Get result and verify
	result, err := handler.GetResult()
	assert.NoError(err)
	assert.NotNil(result)
	assert.Equal(2, len(result))
	assert.Equal(vbInfo1, result[0])
	assert.Equal(vbInfo2, result[1])
}

// Test VbucketInfoHandler OnError functionality
func TestVbucketInfoHandler_OnError(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewVbucketInfoHandler(doneCh, errorCh)

	testErr := fmt.Errorf("test error")

	// Call OnError in goroutine
	go handler.OnError(testErr)

	// Check that error is received
	select {
	case err := <-errorCh:
		assert.Equal(testErr, err)
	case <-time.After(5 * time.Second):
		assert.Fail("Expected error but timed out")
	}

	// Check that error channel is closed
	select {
	case _, open := <-errorCh:
		assert.False(open, "Error channel should be closed")
	case <-time.After(1 * time.Second):
		assert.Fail("Error channel should be closed")
	}
}

// Test VbucketInfoHandler OnComplete functionality
func TestVbucketInfoHandler_OnComplete(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewVbucketInfoHandler(doneCh, errorCh)

	// Call OnComplete in goroutine
	go handler.OnComplete()

	// Check that done channel is closed
	select {
	case <-doneCh:
		// Success - channel is closed
	case <-time.After(5 * time.Second):
		assert.Fail("Expected done channel to be closed but timed out")
	}
}

// Test VbucketInfoHandler GetResult blocking behavior
func TestVbucketInfoHandler_GetResultBlocking(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewVbucketInfoHandler(doneCh, errorCh)

	// GetResult should block until first message is received
	done := make(chan bool)
	go func() {
		result, err := handler.GetResult()
		assert.NoError(err)
		assert.NotNil(result)
		done <- true
	}()

	// Wait a bit to ensure GetResult is blocking
	select {
	case <-done:
		assert.Fail("GetResult should be blocking")
	case <-time.After(100 * time.Millisecond):
		// Good - GetResult is blocking
	}

	// Send a message to unblock GetResult
	vbInfo := &internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{
		VbucketId: 0,
		Uuid:      12345,
		MaxCas:    func() *uint64 { v := uint64(200); return &v }(),
	}
	msg := &internal_xdcr_v1.GetVbucketInfoResponse{
		Vbuckets: []*internal_xdcr_v1.GetVbucketInfoResponse_VbucketState{vbInfo},
	}

	handler.OnMessage(msg)
	handler.OnComplete()

	// Now GetResult should complete
	select {
	case <-done:
		result, err := handler.GetResult()
		assert.NoError(err)
		assert.NotNil(result)
		assert.Equal(vbInfo, result[0])
	case <-time.After(5 * time.Second):
		assert.Fail("GetResult should have completed")
	}
}

// Test VbucketInfoHandler GetResult error behavior
func TestVbucketInfoHandler_GetResultError(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewVbucketInfoHandler(doneCh, errorCh)

	// Send an error to the error channel
	go handler.OnError(fmt.Errorf("test error"))

	// GetResult should return an error
	result, err := handler.GetResult()
	assert.Error(err)
	assert.Equal("error occured while fetching vbucket info: test error", err.Error())
	assert.Nil(result)
}
