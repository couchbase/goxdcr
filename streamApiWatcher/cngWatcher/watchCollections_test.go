package cngWatcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	utilsMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	defaultWaitTime   = 5 * time.Second
	maxWaitTime       = 30 * time.Second
	backoffFactor     = 2
	bucketName        = "streamTestBucket"
	testTargetConnStr = "localhost:18098"
)

// Test helper to create mock utils for unit tests that don't need real CNG
func setupMockUtils() *utilsMock.CngUtils {
	mockUtils := &utilsMock.CngUtils{}

	return mockUtils
}

// Setup GrpcOptions for testing
func getTestGrpcOptions() *base.GrpcOptions {
	return &base.GrpcOptions{
		ConnStr: testTargetConnStr,
		GetCredentials: func() *base.Credentials {
			return &base.Credentials{
				UserName_: "testuser",
				Password_: "testpass",
			}
		},
		UseTLS: false,
	}
}

// Test WatchCollectionsHandler OnMessage functionality
func TestWatchCollectionsHandler_OnMessage(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewWatchCollectionsHandler(doneCh, errorCh)

	// Create test message
	msg := &internal_xdcr_v1.WatchCollectionsResponse{
		ManifestUid: 123,
		Scopes: []*internal_xdcr_v1.WatchCollectionsResponse_Scope{
			{
				ScopeId:   456,
				ScopeName: "_default",
				Collections: []*internal_xdcr_v1.WatchCollectionsResponse_Collection{
					{
						CollectionId:   789,
						CollectionName: "_default",
					},
				},
			},
		},
	}

	// Call OnMessage
	handler.OnMessage(msg)

	// Get result and verify
	result := handler.GetResult()
	assert.NotNil(result)
	assert.Equal(msg, result)
}

// Test WatchCollectionsHandler OnError functionality
func TestWatchCollectionsHandler_OnError(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewWatchCollectionsHandler(doneCh, errorCh)

	testErr := fmt.Errorf("watch collections error")

	// Call OnError in goroutine
	go handler.OnError(testErr)

	// Check that error is received
	select {
	case err := <-errorCh:
		assert.Equal(testErr, err)
	case <-time.After(5 * time.Second):
		assert.Fail("Expected error but timed out")
	}
}

// Test WatchCollectionsHandler OnComplete functionality
func TestWatchCollectionsHandler_OnComplete(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewWatchCollectionsHandler(doneCh, errorCh)

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

// Test WatchCollectionsHandler first message closes initDone
func TestWatchCollectionsHandler_FirstMessageInitDone(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewWatchCollectionsHandler(doneCh, errorCh)

	// GetResult should block until first message
	done := make(chan bool)
	go func() {
		result := handler.GetResult()
		assert.NotNil(result)
		done <- true
	}()

	// Ensure GetResult is blocking
	select {
	case <-done:
		assert.Fail("GetResult should be blocking")
	case <-time.After(100 * time.Millisecond):
		// Good
	}

	// Send first message
	msg1 := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 1}
	handler.OnMessage(msg1)

	// GetResult should now complete
	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		assert.Fail("GetResult should have completed")
	}

	// Send second message - should update cache
	msg2 := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 2}
	handler.OnMessage(msg2)

	// GetResult should return latest message immediately
	result := handler.GetResult()
	assert.Equal(msg2, result)
}

// Test CollectionsWatcher construction
func TestCollectionsWatcher_NewCollectionsWatcher(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("testCollectionsWatcher", log.DefaultLoggerContext)

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		defaultWaitTime,
		backoffFactor,
		maxWaitTime,
		logger,
	)

	assert.NotNil(watcher)
}

// Test CollectionsWatcher Start/Stop functionality with mock utils
func TestCollectionsWatcher_StartStopMock(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("testCollectionsWatcher", log.DefaultLoggerContext)

	// Mock the CngWatchCollections method
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		// Simulate stream behavior
		handler := args.Get(2).(*WatchCollectionsHandler)

		// Send a test message
		go func() {
			time.Sleep(100 * time.Millisecond)
			msg := &internal_xdcr_v1.WatchCollectionsResponse{
				ManifestUid: 123,
			}
			handler.OnMessage(msg)
			time.Sleep(100 * time.Millisecond)
			handler.OnComplete()
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		100*time.Millisecond, // Short wait time for test
		2,
		1*time.Second,
		logger,
	)

	// Start watcher
	watcher.Start()

	// Wait a bit for it to process
	time.Sleep(500 * time.Millisecond)

	// Check that we can get a result
	result := watcher.GetResult()
	if result != nil {
		assert.Equal(uint64(123), result.Uid())
	}

	// Stop watcher
	watcher.Stop()

	mockUtils.AssertExpectations(t)
}

// Test error handling and retry logic
func TestCollectionsWatcher_ErrorHandling(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("errorTest", log.DefaultLoggerContext)

	callCount := 0
	// Mock the CngWatchCollections method to fail first, then succeed
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)
		callCount++

		if callCount == 1 {
			// First call fails
			go func() {
				time.Sleep(50 * time.Millisecond)
				handler.OnError(fmt.Errorf("simulated connection error"))
			}()
		} else {
			// Second call succeeds
			go func() {
				time.Sleep(50 * time.Millisecond)
				msg := &internal_xdcr_v1.WatchCollectionsResponse{
					ManifestUid: 456,
				}
				handler.OnMessage(msg)
				handler.OnComplete()
			}()
		}
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		50*time.Millisecond, // Very short wait for quick retry
		2,
		1*time.Second,
		logger,
	)

	// Start watcher
	watcher.Start()

	// Wait for retry logic to kick in
	time.Sleep(500 * time.Millisecond)

	// Should eventually get a successful result
	result := watcher.GetResult()
	if result != nil {
		assert.Equal(uint64(456), result.Uid())
	}

	// Stop watcher
	watcher.Stop()

	// Should have been called twice (initial + retry)
	assert.Equal(2, callCount)
	mockUtils.AssertExpectations(t)
}

// Test user initiated cancellation
func TestCollectionsWatcher_UserInitiatedCancellation(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("userCancellationTest", log.DefaultLoggerContext)

	// Mock the CngWatchCollections method to run successfully but not complete immediately
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)

		// Send a message and keep the stream running
		go func() {
			time.Sleep(50 * time.Millisecond)
			msg := &internal_xdcr_v1.WatchCollectionsResponse{
				ManifestUid: 789,
			}
			handler.OnMessage(msg)
			// Call on complete to simulate completion after 1000ms
			time.Sleep(5 * time.Second)
			handler.OnComplete()
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		100*time.Millisecond,
		2,
		1*time.Second,
		logger,
	)

	// Start watcher
	watcher.Start()

	// Wait a bit for it to start and process message
	time.Sleep(200 * time.Millisecond)

	// Verify we can get a result while it's running
	result := watcher.GetResult()
	assert.NotNil(result)
	assert.Equal(uint64(789), result.Uid())

	// Stop watcher (user iniated cancellation)

	watcher.Stop()
	assert.Equal(watcher.opState.active, false)

	mockUtils.AssertExpectations(t)
}

// Test cancellation when waiting on a retry
func TestCollectionsWatcher_CancellationDuringRetryWait(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("retryCancellationTest", log.DefaultLoggerContext)

	// Mock the CngWatchCollections method to always fail
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)

		// Always fail immediately to trigger retry logic
		go func() {
			time.Sleep(10 * time.Millisecond)
			handler.OnError(fmt.Errorf("simulated persistent connection error"))
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		500*time.Millisecond, // Longer wait time so we can cancel during retry wait
		2,
		2*time.Second,
		logger,
	)

	// Start watcher
	watcher.Start()

	// Wait for the first failure and retry to be scheduled
	time.Sleep(100 * time.Millisecond)

	// Stop watcher while it's waiting for retry (cancellation during retry wait)
	stopStart := time.Now()
	watcher.Stop()
	stopDuration := time.Since(stopStart)

	// Verify it stopped in reasonable time despite being in retry wait
	assert.Less(stopDuration, 200*time.Millisecond, "Stop should complete quickly even during retry wait")

	// Should have been called at least once (the initial failure)
	mockUtils.AssertExpectations(t)
}
