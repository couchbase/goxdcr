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
		handler := args.Get(2).(*CollectionsWatcher)

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
		handler := args.Get(2).(*CollectionsWatcher)
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
		handler := args.Get(2).(*CollectionsWatcher)

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
		handler := args.Get(2).(*CollectionsWatcher)

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
		1*time.Second, // Longer wait time so we can cancel during retry wait
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

// Test race condition: GetResult() called when stream is in retry wait (after stream fails, before retry)
func TestCollectionsWatcher_GetResult_RaceCondition_RetryWait(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("raceConditionTest", log.DefaultLoggerContext)

	failureCount := 0
	// Mock to fail initially, then succeed but not immediately
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*CollectionsWatcher)
		failureCount++

		if failureCount == 1 {
			// First call fails immediately - this will cause stream to retry
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
	// GetResult should return default manifest because no message has been received yet
	result := watcher.GetResult()
	assert.NotNil(result, "GetResult should return default manifest when no message received yet")
	assert.Equal(uint64(0), result.Uid(), "Default manifest should have UID 0")

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
		handler := args.Get(2).(*CollectionsWatcher)
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

	// GetResult should return default manifest because:
	// 1. The cache was never initialized (no message was ever received)
	// 2. The stream failed
	result := watcher.GetResult()
	assert.NotNil(result, "GetResult should return default manifest when cache was never initialized")
	assert.Equal(uint64(0), result.Uid(), "Default manifest should have UID 0")

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
		handler := args.Get(2).(*CollectionsWatcher)
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

	// Some calls might return default manifest (during inactive periods), others might return the actual manifest
	// The important thing is that there are no race conditions or panics
	var successfulResults int
	for _, result := range results {
		assert.NotNil(result, "GetResult should always return a manifest (default or actual)")
		if result.Uid() == 777 {
			successfulResults++
		}
	}

	// At least some results should be successful once the stream stabilizes
	assert.True(successfulResults > 0, "At least some concurrent GetResult calls should return the actual manifest")

	watcher.Stop()
	mockUtils.AssertExpectations(t)
}

func TestCollectionsWatcher_GetResult_OnRetryWait(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("retryWaitTest", log.DefaultLoggerContext)

	failureCount := 0
	// Mock to fail
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*CollectionsWatcher)
		failureCount++
		time.Sleep(10 * time.Millisecond)
		handler.OnError(fmt.Errorf("initial connection error"))
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

	// Initialize the cache with a message
	msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 999}
	watcher.OnMessage(msg)

	// Start the watcher - the underlying stream will constantly fail and retry
	watcher.Start()

	// Wait for the stream to be retried before getting the result
	time.Sleep(3 * time.Second)

	// assert the stream has been retried
	assert.Greater(failureCount, 1)

	// assert the result is the message we initialized the cache with
	result := watcher.GetResult()
	assert.Equal(uint64(999), result.Uid())

	// stop the watcher
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

// Test retry behavior - watcher should not retry after first error
func TestCollectionsWatcher_Retry_False_NoRetryOnError(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("retryFalseTest", log.DefaultLoggerContext)

	callCount := 0
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*CollectionsWatcher)
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

// Test minDuration helper function
func TestMinDuration(t *testing.T) {
	assert := assert.New(t)

	// Test cases for minDuration
	assert.Equal(1*time.Second, minDuration(1*time.Second, 2*time.Second))
	assert.Equal(1*time.Second, minDuration(2*time.Second, 1*time.Second))
	assert.Equal(1*time.Second, minDuration(1*time.Second, 1*time.Second))
	assert.Equal(0*time.Second, minDuration(0*time.Second, 1*time.Second))
}

// Test LoadFromWatchCollectionsResp path with actual manifest data
func TestCollectionsWatcher_GetResult_WithManifestData(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("manifestDataTest", log.DefaultLoggerContext)

	// Mock with realistic manifest data
	mockUtils.On("CngWatchCollections", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Run(func(args mock.Arguments) {
		handler := args.Get(2).(*CollectionsWatcher)

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

// Test CollectionsWatcher OnMessage method
func TestCollectionsWatcher_OnMessage(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("testOnMessage", log.DefaultLoggerContext)

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

	// Create test message
	msg := &internal_xdcr_v1.WatchCollectionsResponse{
		ManifestUid: 123,
		Scopes: []*internal_xdcr_v1.WatchCollectionsResponse_Scope{
			{
				ScopeId:   456,
				ScopeName: "_default",
			},
		},
	}

	// Call OnMessage
	watcher.OnMessage(msg)

	// Verify the message is cached
	select {
	case <-watcher.cache.initDone:
		// initDone should be closed
	case <-time.After(1 * time.Second):
		assert.Fail("initDone should be closed after OnMessage")
	}

	watcher.cache.mutex.RLock()
	assert.Equal(msg, watcher.cache.currVal)
	watcher.cache.mutex.RUnlock()
}

// Test CollectionsWatcher OnError method
func TestCollectionsWatcher_OnError(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("testOnError", log.DefaultLoggerContext)

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

	testErr := fmt.Errorf("test error")

	// Initialize the opState with an errorCh
	watcher.opState.mutex.Lock()
	watcher.opState.active = true
	watcher.opState.errorCh = make(chan error, 1)
	errorCh := watcher.opState.errorCh
	watcher.opState.mutex.Unlock()

	// Call OnError in goroutine
	go watcher.OnError(testErr)

	// Check that error is received
	select {
	case err := <-errorCh:
		assert.Equal(testErr, err)
	case <-time.After(1 * time.Second):
		assert.Fail("Expected error but timed out")
	}
}

// Test CollectionsWatcher OnComplete method
func TestCollectionsWatcher_OnComplete(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("testOnComplete", log.DefaultLoggerContext)

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

	// Initialize the opState with a doneCh
	watcher.opState.mutex.Lock()
	watcher.opState.active = true
	watcher.opState.doneCh = make(chan struct{})
	doneCh := watcher.opState.doneCh
	watcher.opState.mutex.Unlock()

	// Call OnComplete in goroutine
	go watcher.OnComplete()

	// Check that done channel is closed
	select {
	case <-doneCh:
		// Success - channel is closed
	case <-time.After(1 * time.Second):
		assert.Fail("Expected done channel to be closed but timed out")
	}
}

// Test race condition: Concurrent OnMessage calls
func TestCollectionsWatcher_OnMessage_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("concurrentOnMessage", log.DefaultLoggerContext)

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

	// Launch multiple concurrent OnMessage calls
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := &internal_xdcr_v1.WatchCollectionsResponse{
				ManifestUid: uint32(idx),
			}
			watcher.OnMessage(msg)
		}(i)
	}

	wg.Wait()

	// Verify no race conditions occurred and cache was updated
	watcher.cache.mutex.RLock()
	assert.NotNil(watcher.cache.currVal)
	watcher.cache.mutex.RUnlock()

	// Verify initDone is closed
	select {
	case <-watcher.cache.initDone:
		// Success
	default:
		assert.Fail("initDone should be closed after concurrent OnMessage calls")
	}
}

// Test race condition: Concurrent GetResult calls
func TestCollectionsWatcher_GetResult_RaceDetection(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("getResultRace", log.DefaultLoggerContext)

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

	// Send a message to initialize the cache
	msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 555}
	watcher.OnMessage(msg)

	// Launch multiple concurrent GetResult calls
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := watcher.GetResult()
			assert.NotNil(result)
			assert.Equal(uint64(555), result.Uid())
		}()
	}

	wg.Wait()
	// If no race condition, test passes
}

// Test race condition: Concurrent GetResult and OnMessage
func TestCollectionsWatcher_GetResult_OnMessage_Concurrent(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("getResultOnMessageRace", log.DefaultLoggerContext)

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

	// Send initial message
	msg := &internal_xdcr_v1.WatchCollectionsResponse{ManifestUid: 100}
	watcher.OnMessage(msg)

	var wg sync.WaitGroup

	wg.Add(1)
	// Launch concurrent OnMessage calls
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				newMsg := &internal_xdcr_v1.WatchCollectionsResponse{
					ManifestUid: uint32(101 + idx),
				}
				watcher.OnMessage(newMsg)
			}(i)
		}
	}()

	// Launch concurrent GetResult calls
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 25; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result := watcher.GetResult()
				assert.NotNil(result)
			}()
		}
	}()
	wg.Wait()

	// Verify no race condition occurred and we got a valid manifest
	// Due to concurrent execution, we can't predict which message will be last
	// but it should be one of the messages we sent (UID between 100 and 124)
	result := watcher.GetResult()
	assert.NotNil(result)
	uid := result.Uid()
	assert.Greater(uid, uint64(100), "UID should be at least 100")
	assert.LessOrEqual(uid, uint64(124), "UID should be at most 124")

	// If no race condition, test passes
}

// Test race condition: OpState concurrent access
func TestWatchCollectionsOpState_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	opState := &WatchCollectionsOpState{}

	var wg sync.WaitGroup

	// First, start an operation
	ctx, doneCh, errorCh, err := opState.beginOp(context.Background())
	assert.NoError(err)
	assert.NotNil(ctx)
	assert.NotNil(doneCh)
	assert.NotNil(errorCh)

	// Launch concurrent cancelOp calls
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opState.cancelOp()
		}()
	}

	// Launch concurrent status reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opState.mutex.RLock()
			_ = opState.active
			opState.mutex.RUnlock()
		}()
	}

	wg.Wait()

	// Clean up
	opState.endOp()

	// Verify state is clean
	assert.False(opState.active)
}

// Test GetResult returns default manifest immediately without blocking
func TestCollectionsWatcher_GetResult_DefaultManifest(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("defaultManifestTest", log.DefaultLoggerContext)

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

	// GetResult should return default manifest immediately without blocking
	done := make(chan bool)
	go func() {
		result := watcher.GetResult()
		assert.NotNil(result)
		assert.Equal(uint64(0), result.Uid())
		done <- true
	}()

	// Should complete quickly without blocking
	select {
	case <-done:
		// Success - returned default manifest immediately
	case <-time.After(100 * time.Millisecond):
		assert.Fail("GetResult should return default manifest immediately")
	}
}

// Test that multiple GetResult calls without messages return default manifest
func TestCollectionsWatcher_GetResult_MultipleCallsDefaultManifest(t *testing.T) {
	assert := assert.New(t)

	mockUtils := setupMockUtils()
	logger := log.NewLogger("multipleDefaultManifest", log.DefaultLoggerContext)

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

	// Multiple calls should all return default manifest
	for i := 0; i < 5; i++ {
		result := watcher.GetResult()
		assert.NotNil(result)
		assert.Equal(uint64(0), result.Uid())
	}
}

// Test race condition: beginOp and endOp concurrent access
func TestWatchCollectionsOpState_BeginEnd_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)

	opState := &WatchCollectionsOpState{}

	// Start an operation
	ctx, doneCh, errorCh, err := opState.beginOp(context.Background())
	assert.NoError(err)
	assert.NotNil(ctx)
	assert.NotNil(doneCh)
	assert.NotNil(errorCh)

	var wg sync.WaitGroup

	// Try to begin another operation (should fail)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := opState.beginOp(context.Background())
			assert.Error(err)
			assert.Equal(ErrWatchCollectionsAlreadyActive, err)
		}()
	}

	wg.Wait()

	// End the operation
	opState.endOp()
	assert.False(opState.active)

	// Now beginOp should succeed
	ctx2, doneCh2, errorCh2, err2 := opState.beginOp(context.Background())
	assert.NoError(err2)
	assert.NotNil(ctx2)
	assert.NotNil(doneCh2)
	assert.NotNil(errorCh2)

	opState.endOp()
}

// Test context cancellation propagation
func TestCollectionsWatcher_ContextCancellation(t *testing.T) {
	assert := assert.New(t)

	opState := &WatchCollectionsOpState{}

	// Begin operation
	ctx, doneCh, errorCh, err := opState.beginOp(context.Background())
	assert.NoError(err)
	assert.NotNil(ctx)
	assert.NotNil(doneCh)
	assert.NotNil(errorCh)

	// Cancel the operation
	opState.cancelOp()

	// Wait a bit for cancellation to propagate
	time.Sleep(50 * time.Millisecond)

	// Context should be cancelled
	select {
	case <-ctx.Done():
		// Success - context is cancelled
		cause := context.Cause(ctx)
		assert.Equal(base.ErrorUserInitiatedStreamRpcCancellation, cause)
	default:
		assert.Fail("Context should be cancelled after cancelOp")
	}

	// Clean up
	opState.endOp()
}
