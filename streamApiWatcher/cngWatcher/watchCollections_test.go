package cngWatcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
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
		true,
		logger,
	).(*CollectionsWatcher)

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
		true,
		logger,
	).(*CollectionsWatcher)

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
		true,
		logger,
	).(*CollectionsWatcher)

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
		true,
		logger,
	).(*CollectionsWatcher)

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
		true,
		logger,
	).(*CollectionsWatcher)

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

// Test race condition: GetResult() called when watcher is inactive (after stream fails, before retry)
func TestCollectionsWatcher_GetResult_RaceCondition_InactiveWatcher(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("raceConditionTest", log.DefaultLoggerContext)

	failureCount := 0
	// Mock to fail initially, then succeed but not immediately
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)
		failureCount++

		if failureCount == 1 {
			// First call fails immediately - this will cause watcher to become inactive
			go func() {
				time.Sleep(10 * time.Millisecond)
				handler.OnError(fmt.Errorf("initial connection error"))
			}()
		} else {
			// Second call succeeds after delay
			go func() {
				time.Sleep(200 * time.Millisecond) // Delay to create race condition window
				msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 999}
				handler.OnMessage(msg)
				time.Sleep(100 * time.Millisecond)
				handler.OnComplete()
			}()
		}
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		100*time.Millisecond, // Wait time for retry
		2,
		1*time.Second,
		true,
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Wait for the first failure to complete and watcher to become inactive
	time.Sleep(50 * time.Millisecond)

	// During this window, the watcher should be inactive (after failure, before retry connects)
	// GetResult should return nil because watcher is inactive
	result := watcher.GetResult()
	assert.Nil(result, "GetResult should return nil when watcher is inactive between stream failure and retry")

	// Wait for retry to succeed
	time.Sleep(300 * time.Millisecond)

	// Now GetResult should return the manifest
	result = watcher.GetResult()
	if result != nil {
		assert.Equal(uint64(999), result.Uid())
	}

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

// Test race condition: GetResult() when cache is never initialized (stream fails before first message)
func TestCollectionsWatcher_GetResult_RaceCondition_CacheNeverInitialized(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("cacheNotInitTest", log.DefaultLoggerContext)

	// Mock to always fail before sending any message
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)
		// Fail immediately without sending any message (cache never initialized)
		go func() {
			time.Sleep(10 * time.Millisecond)
			handler.OnError(fmt.Errorf("connection error before message"))
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		2*time.Second, // Long retry wait to keep watcher inactive
		2,
		5*time.Second,
		true,
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Wait for failure to be processed
	time.Sleep(100 * time.Millisecond)

	// GetResult should return nil because:
	// 1. The cache was never initialized (no message was ever received)
	// 2. The stream failed
	result := watcher.GetResult()
	assert.Nil(result, "GetResult should return nil when cache was never initialized due to early stream failure")

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

// Test concurrent GetResult() calls during stream lifecycle changes
func TestCollectionsWatcher_GetResult_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("concurrentTest", log.DefaultLoggerContext)

	callCount := 0
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)
		callCount++

		go func() {
			time.Sleep(50 * time.Millisecond)
			if callCount <= 2 {
				// First two calls fail
				handler.OnError(fmt.Errorf("connection error %d", callCount))
			} else {
				// Third call succeeds
				msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 777}
				handler.OnMessage(msg)
				time.Sleep(200 * time.Millisecond)
				handler.OnComplete()
			}
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		50*time.Millisecond,
		2,
		1*time.Second,
		true,
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Launch multiple concurrent GetResult() calls during stream lifecycle changes
	var wg sync.WaitGroup
	results := make([]*metadata.CollectionsManifest, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Add some randomness to timing
			time.Sleep(time.Duration(idx*50) * time.Millisecond)
			results[idx] = watcher.GetResult()
		}(i)
	}

	wg.Wait()

	// Some calls might return nil (during inactive periods), others might return manifest
	// The important thing is that there are no race conditions or panics
	var successfulResults int
	for _, result := range results {
		if result != nil {
			successfulResults++
			assert.Equal(uint64(777), result.Uid())
		}
	}

	// At least some results should be successful once the stream stabilizes
	assert.True(successfulResults > 0, "At least some concurrent GetResult calls should succeed")

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

// Test beginOp error condition - stream already active
func TestWatchCollectionsOpState_BeginOp_AlreadyActive(t *testing.T) {
	assert := assert.New(t)

	opState := &WatchCollectionsOpState{}

	// First beginOp should succeed
	ctx1, doneCh1, errorCh1, err1 := opState.beginOp(context.Background())
	assert.NoError(err1)
	assert.NotNil(ctx1)
	assert.NotNil(doneCh1)
	assert.NotNil(errorCh1)
	assert.True(opState.active)

	// Second beginOp should fail with ErrWatchCollectionsAlreadyActive
	ctx2, doneCh2, errorCh2, err2 := opState.beginOp(context.Background())
	assert.Error(err2)
	assert.Equal(ErrWatchCollectionsAlreadyActive, err2)
	assert.Nil(ctx2)
	assert.Nil(doneCh2)
	assert.Nil(errorCh2)

	// Clean up
	opState.endOp()
}

// Test cancelOp when not active
func TestWatchCollectionsOpState_CancelOp_NotActive(t *testing.T) {
	opState := &WatchCollectionsOpState{}

	// cancelOp on inactive state should not panic
	opState.cancelOp()

	// Should still be inactive
	assert.False(t, opState.active)
}

// Test endOp when not active
func TestWatchCollectionsOpState_EndOp_NotActive(t *testing.T) {
	opState := &WatchCollectionsOpState{}

	// endOp on inactive state should not panic
	opState.endOp()

	// Should still be inactive
	assert.False(t, opState.active)
}

// Test handler GetResult returning nil in CollectionsWatcher.GetResult()
func TestCollectionsWatcher_GetResult_HandlerReturnsNil(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("handlerNilTest", log.DefaultLoggerContext)

	// Create a custom handler that will return nil from GetResult
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)

		// Trigger initDone without setting currVal (simulate error scenario)
		go func() {
			time.Sleep(10 * time.Millisecond)
			handler.OnError(fmt.Errorf("error after init"))
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		500*time.Millisecond,
		2,
		1*time.Second,
		true,
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Wait for stream to be established and fail
	time.Sleep(100 * time.Millisecond)

	// The handler's GetResult() returns nil, so watcher's GetResult() should return nil
	result := watcher.GetResult()
	assert.Nil(result, "GetResult should return nil when handler returns nil response")

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

// Test retry behavior - watcher should not retry after first error
func TestCollectionsWatcher_Retry_False_NoRetryOnError(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("retryFalseTest", log.DefaultLoggerContext)

	callCount := 0
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)
		callCount++

		// Always fail to test retry behavior
		go func() {
			time.Sleep(10 * time.Millisecond)
			handler.OnError(fmt.Errorf("retry false test error"))
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		50*time.Millisecond, // Short retry time
		2,
		1*time.Second,
		false, // retry = false
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Wait longer than retry time to ensure no retry happens
	time.Sleep(200 * time.Millisecond)

	// Should have been called only once (no retry due to oneTime=true)
	assert.Equal(1, callCount, "oneTime watcher should not retry after error")

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

// Test concurrent Start/Stop operations
func TestCollectionsWatcher_ConcurrentStartStop(t *testing.T) {
	mockUtils := setupMockUtils()
	logger := log.NewLogger("concurrentStartStopTest", log.DefaultLoggerContext)

	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)

		go func() {
			time.Sleep(100 * time.Millisecond)
			msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 555}
			handler.OnMessage(msg)
			time.Sleep(200 * time.Microsecond)
			handler.OnComplete()
		}()
	})

	watcher := NewCollectionsWatcher(
		bucketName,
		getTestGrpcOptions,
		mockUtils,
		50*time.Millisecond,
		2,
		1*time.Second,
		true,
		logger,
	).(*CollectionsWatcher)

	// Launch multiple Start operations concurrently
	// Only one should actually start the watcher
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			watcher.Start()
		}()
	}

	time.Sleep(50 * time.Millisecond)
	watcher.Stop()

	wg.Wait()

	// Should not panic and should handle concurrent operations gracefully
	mockUtils.AssertExpectations(t)
}

// Test minDuration helper function
func TestMinDuration(t *testing.T) {
	assert := assert.New(t)

	// Test cases for minDuration
	assert.Equal(1*time.Second, minDuration(1*time.Second, 2*time.Second))
	assert.Equal(1*time.Second, minDuration(2*time.Second, 1*time.Second))
	assert.Equal(1*time.Second, minDuration(1*time.Second, 1*time.Second))
	assert.Equal(0*time.Second, minDuration(0*time.Second, 1*time.Second))
}

// Test WatchCollectionsHandler OnError closes initDone
func TestWatchCollectionsHandler_OnError_ClosesInitDone(t *testing.T) {
	assert := assert.New(t)

	doneCh := make(chan struct{})
	errorCh := make(chan error, 1)
	handler := NewWatchCollectionsHandler(doneCh, errorCh)

	// GetResult should block until initDone is closed
	done := make(chan bool)
	go func() {
		result := handler.GetResult()
		// Result should be nil since no message was sent
		assert.Nil(result)
		done <- true
	}()

	// Ensure GetResult is blocking
	select {
	case <-done:
		assert.Fail("GetResult should be blocking")
	case <-time.After(50 * time.Millisecond):
		// Good
	}

	// OnError should close initDone and unblock GetResult
	go handler.OnError(fmt.Errorf("test error"))

	// GetResult should now complete
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		assert.Fail("GetResult should have completed after OnError")
	}

	// Verify error was sent
	select {
	case err := <-errorCh:
		assert.Error(err)
	default:
		assert.Fail("Error should have been sent to errorCh")
	}
}

// Test LoadFromWatchCollectionsResp path with actual manifest data
func TestCollectionsWatcher_GetResult_WithManifestData(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("manifestDataTest", log.DefaultLoggerContext)

	// Mock with realistic manifest data
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*WatchCollectionsHandler)

		go func() {
			time.Sleep(50 * time.Millisecond)
			msg := &internal_xdcr_v1.WatchCollectionsResponse{
				ManifestUid: 12345,
				Scopes: []*internal_xdcr_v1.WatchCollectionsResponse_Scope{
					{
						ScopeId:   0,
						ScopeName: "_default",
						Collections: []*internal_xdcr_v1.WatchCollectionsResponse_Collection{
							{
								CollectionId:   0,
								CollectionName: "_default",
							},
							{
								CollectionId:   8,
								CollectionName: "testCollection",
							},
						},
					},
					{
						ScopeId:   9,
						ScopeName: "testScope",
						Collections: []*internal_xdcr_v1.WatchCollectionsResponse_Collection{
							{
								CollectionId:   10,
								CollectionName: "scopedCollection",
							},
						},
					},
				},
			}
			handler.OnMessage(msg)
			time.Sleep(500 * time.Millisecond)
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
		true,
		logger,
	).(*CollectionsWatcher)

	watcher.Start()

	// Wait for message processing
	time.Sleep(200 * time.Millisecond)

	// Verify manifest is properly constructed
	result := watcher.GetResult()
	assert.NotNil(result)
	assert.Equal(uint64(12345), result.Uid())

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}
