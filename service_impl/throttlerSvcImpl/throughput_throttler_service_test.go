package throttlerSvcImpl

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	"github.com/stretchr/testify/assert"
)

type testParams struct {
	Name            string
	HighLimit       int64
	LowLimit        int64
	ClogLimit       int64
	MaxReassignable int64
	NeedToCalibrate bool

	Existing *quotaVals
	Expected *quotaVals

	CanSendStatus bool
	ThrottlerReq  throttlerSvc.ThrottlerReq
}

func (tt *testParams) updateSettings(t *testing.T, th *ThroughputThrottler) {
	settings := map[string]interface{}{
		throttlerSvc.HighTokensKey:                tt.HighLimit,
		throttlerSvc.LowTokensKey:                 tt.LowLimit,
		throttlerSvc.ConflictLogTokensKey:         tt.ClogLimit,
		throttlerSvc.MaxReassignableHighTokensKey: tt.MaxReassignable,
		throttlerSvc.NeedToCalibrateKey:           tt.NeedToCalibrate,
		throttlerSvc.ConflictLogEnabledKey:        true,
	}

	assert.Equal(t, 0, len(th.UpdateSettings(settings)))
}

func (tt *testParams) setQuota(_ *testing.T, th *ThroughputThrottler) {
	if tt.Existing != nil {
		th.unused_high_tokens = tt.Existing.HighQuota
		th.throughput_quota = tt.Existing.LowQuota
		th.conflictLogQuota = tt.Existing.ClogQuota
		th.reassigned_high_tokens = tt.Existing.MaxReassignable
		th.logger.Infof("after setQuota: %#v", th)
	}
}

type quotaVals struct {
	HighQuota       int64
	LowQuota        int64
	ClogQuota       int64
	MaxReassignable int64
}

func createThrottler(_ *testing.T, tt *testParams) (th *ThroughputThrottler, settings map[string]interface{}) {
	settings = map[string]interface{}{
		throttlerSvc.HighTokensKey:                tt.HighLimit,
		throttlerSvc.LowTokensKey:                 tt.LowLimit,
		throttlerSvc.ConflictLogTokensKey:         tt.ClogLimit,
		throttlerSvc.MaxReassignableHighTokensKey: tt.MaxReassignable,
		throttlerSvc.NeedToCalibrateKey:           tt.NeedToCalibrate,
		throttlerSvc.ConflictLogEnabledKey:        true,
	}

	loggerCtx := log.DefaultLoggerContext
	loggerCtx.Log_level = log.LogLevelDebug

	th = NewThroughputThrottlerSvc(loggerCtx)
	th.SetId("unit-test")
	if tt.Existing != nil {
		th.unused_high_tokens = tt.Existing.HighQuota
		th.throughput_quota = tt.Existing.LowQuota
		th.conflictLogQuota = tt.Existing.ClogQuota
		th.reassigned_high_tokens = tt.MaxReassignable
	}
	return
}

func Test_handleClogReq(t *testing.T) {
	testData := []*testParams{
		{
			Name:          "[positive] no limit",
			LowLimit:      0,
			Existing:      &quotaVals{},
			Expected:      &quotaVals{},
			CanSendStatus: true,
		},
		{
			Name:      "[positive] enough quota",
			ClogLimit: 10,
			Existing: &quotaVals{
				ClogQuota: 10,
			},
			Expected: &quotaVals{
				ClogQuota: 7,
			},
			CanSendStatus: true,
		},
		{
			Name:          "[positive] no quota but enough reassignable tokens",
			ClogLimit:     10,
			Existing:      &quotaVals{MaxReassignable: 10},
			Expected:      &quotaVals{MaxReassignable: 7},
			CanSendStatus: true,
		},
		{
			Name:          "[negative] no quota no reassignable tokens",
			ClogLimit:     10,
			Existing:      &quotaVals{},
			Expected:      &quotaVals{},
			CanSendStatus: false,
		},
		{
			Name:      "[negative] has not enough quota and no reassignable tokens",
			ClogLimit: 10,
			Existing: &quotaVals{
				ClogQuota: 2,
			},
			Expected: &quotaVals{
				ClogQuota: 2,
			},
			CanSendStatus: false,
		},
		{
			Name:      "[negative] has no quota but no not enough reassignable tokens",
			ClogLimit: 10,
			Existing: &quotaVals{
				MaxReassignable: 2,
			},
			Expected: &quotaVals{
				MaxReassignable: 2,
			},
			CanSendStatus: false,
		},
		{
			Name:      "[positive] has exactly enough qouta",
			ClogLimit: 30,
			Existing: &quotaVals{
				ClogQuota: 3,
			},
			Expected: &quotaVals{
				ClogQuota: 0,
			},
			CanSendStatus: true,
		},
		{
			Name:      "[positive] has no quota but exactly enough reassignable tokens",
			ClogLimit: 10,
			Existing: &quotaVals{
				MaxReassignable: 3,
			},
			Expected: &quotaVals{
				MaxReassignable: 0,
			},
			CanSendStatus: true,
		},
	}

	for _, tt := range testData {
		t.Run(tt.Name, func(t *testing.T) {
			th, settings := createThrottler(t, tt)
			assert.Equal(t, 0, len(th.UpdateSettings(settings)))
			tt.setQuota(t, th)

			result := th.handleClogReq()
			assert.Equal(t, tt.CanSendStatus, result)

			assert.Equal(t, tt.Expected.HighQuota, th.unused_high_tokens)
			assert.Equal(t, tt.Expected.LowQuota, th.throughput_quota)
			assert.Equal(t, tt.Expected.ClogQuota, th.conflictLogQuota)
			assert.Equal(t, tt.Expected.MaxReassignable, th.reassigned_high_tokens)
		})
	}
}

func Test_handleLowReq(t *testing.T) {
	testData := []*testParams{
		{
			Name:     "[positive] no limit",
			LowLimit: 0,
			Existing: &quotaVals{
				LowQuota: 0,
			},
			Expected: &quotaVals{
				LowQuota: 0,
			},
			CanSendStatus: true,
		},
		{
			Name:     "[positive] enough quota",
			LowLimit: 10,
			Existing: &quotaVals{
				LowQuota: 10,
			},
			Expected: &quotaVals{
				LowQuota: 9,
			},
			CanSendStatus: true,
		},
		{
			Name:     "[positive] no quota but enough reassignable tokens",
			LowLimit: 10,
			Existing: &quotaVals{
				LowQuota:        0,
				MaxReassignable: 10,
			},
			Expected: &quotaVals{
				LowQuota:        0,
				MaxReassignable: 9,
			},
			CanSendStatus: true,
		},
		{
			Name:     "[negative] no quota no reassignable tokens",
			LowLimit: 10,
			Existing: &quotaVals{
				LowQuota:        0,
				MaxReassignable: 0,
			},
			Expected: &quotaVals{
				LowQuota:        0,
				MaxReassignable: 0,
			},
			CanSendStatus: false,
		},
	}

	for _, tt := range testData {
		t.Run(tt.Name, func(t *testing.T) {
			th, settings := createThrottler(t, tt)
			assert.Equal(t, 0, len(th.UpdateSettings(settings)))
			tt.setQuota(t, th)

			result := th.handleLowReq()
			assert.Equal(t, tt.CanSendStatus, result)

			assert.Equal(t, tt.Expected.HighQuota, th.unused_high_tokens)
			assert.Equal(t, tt.Expected.LowQuota, th.throughput_quota)
			assert.Equal(t, tt.Expected.ClogQuota, th.conflictLogQuota)
			assert.Equal(t, tt.Expected.MaxReassignable, th.reassigned_high_tokens)
		})
	}
}

func Test_updateSettings(t *testing.T) {
	testData := []*testParams{
		{
			Name:     "low tokens replenish",
			LowLimit: 100,
			Existing: &quotaVals{
				LowQuota: 20,
			},
			Expected: &quotaVals{
				LowQuota: 10,
			},
		},
		{
			Name:      "high tokens replenish",
			HighLimit: 100,
			Expected: &quotaVals{
				HighQuota: 10,
			},
		},
		{
			Name:      "clog tokens replenish",
			ClogLimit: 100,
			Expected: &quotaVals{
				ClogQuota: 10,
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.Name, func(t *testing.T) {
			th, settings := createThrottler(t, tt)
			assert.Equal(t, 0, len(th.UpdateSettings(settings)))

			assert.Equal(t, tt.Expected.HighQuota, th.unused_high_tokens)
			assert.Equal(t, tt.Expected.LowQuota, th.throughput_quota)
			assert.Equal(t, tt.Expected.ClogQuota, th.conflictLogQuota)
			th.logStatsOnce("unit-test")
		})
	}
}

func Test_updateOnce(t *testing.T) {
	testData := []*testParams{
		{
			Name:     "low tokens replenish",
			LowLimit: 100,
			Expected: &quotaVals{
				LowQuota: 10,
			},
		},
		{
			Name:      "high tokens replenish",
			HighLimit: 100,
			Expected: &quotaVals{
				HighQuota: 10,
			},
		},
		{
			Name:      "clog tokens replenish",
			ClogLimit: 100,
			Expected: &quotaVals{
				ClogQuota: 20,
			},
		},
		{
			Name:      "low,high token replenish",
			LowLimit:  118,
			HighLimit: 219,
			Expected: &quotaVals{
				LowQuota:  12,
				HighQuota: 22,
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.Name, func(t *testing.T) {
			th, settings := createThrottler(t, tt)
			assert.Equal(t, 0, len(th.UpdateSettings(settings)))
			tt.setQuota(t, th)

			th.updateOnce()

			assert.Equal(t, tt.Expected.HighQuota, th.unused_high_tokens)
			assert.Equal(t, tt.Expected.LowQuota, th.throughput_quota)
			assert.Equal(t, tt.Expected.ClogQuota, th.conflictLogQuota)
			assert.Equal(t, tt.Expected.MaxReassignable, th.reassigned_high_tokens)

			th.logStatsOnce("unit-test")
		})
	}
}

func Test_CanSend(t *testing.T) {
	testData := []*testParams{
		{
			Name:      "[negative] clog throttler req",
			ClogLimit: 100,
			Existing: &quotaVals{
				ClogQuota:       0,
				MaxReassignable: 0,
			},
			ThrottlerReq: throttlerSvc.ThrottlerReqCLog,
		},
		{
			Name:      "[positive] clog throttler req - from common buckets",
			ClogLimit: 100,
			Existing: &quotaVals{
				ClogQuota:       0,
				MaxReassignable: 10,
			},
			CanSendStatus: true,
			ThrottlerReq:  throttlerSvc.ThrottlerReqCLog,
		},
		{
			Name:      "[positive] clog throttler req - from clog bucket",
			ClogLimit: 100,
			Existing: &quotaVals{
				ClogQuota:       10,
				MaxReassignable: 0,
			},
			CanSendStatus: true,
			ThrottlerReq:  throttlerSvc.ThrottlerReqCLog,
		},
	}

	for _, tt := range testData {
		t.Run(tt.Name, func(t *testing.T) {
			th, settings := createThrottler(t, tt)
			assert.Equal(t, 0, len(th.UpdateSettings(settings)))
			tt.setQuota(t, th)
			assert.Equal(t, tt.CanSendStatus, th.CanSend(tt.ThrottlerReq))
			th.logStatsOnce("unit-test ")
		})
	}
}

func TestLowTokensThrottling(t *testing.T) {
	// Test ensures that the system can recover when the overall throughput is completely driven by low priority pipelines
	const (
		itemCount        = 100
		initialLowTokens = int64(1)
		numOfSlots       = 10
		numConsumers     = 3
		tickInterval     = 1 * time.Second
	)

	itemsQueue := make(chan throttlerSvc.ThrottlerReq, itemCount)
	throttler := NewThroughputThrottlerSvc(&log.LoggerContext{})

	var (
		prevLowTokens      = initialLowTokens
		prevItemsProcessed int64
		itemsProcessed     int64
		onceStarted        bool
		wg                 sync.WaitGroup
	)

	// Producer
	go func() {
		defer close(itemsQueue)
		for i := 0; i < itemCount; i++ {
			itemsQueue <- throttlerSvc.ThrottlerReqLowRepl
		}
	}()

	// Consumer
	consumer := func() {
		defer wg.Done()
		for item := range itemsQueue {
			for {
				if throttler.CanSend(item) {
					atomic.AddInt64(&itemsProcessed, 1)
					break
				}
				throttler.Wait()
			}
		}
	}

	// Setup and start throttler + consumers
	startThrottler := func() {
		throttler.UpdateSettings(map[string]interface{}{
			throttlerSvc.LowTokensKey:  initialLowTokens,
			throttlerSvc.HighTokensKey: int64(0),
		})
		throttler.Start()

		prevLowTokens = initialLowTokens
		prevItemsProcessed = 0

		for i := 0; i < numConsumers; i++ {
			wg.Add(1)
			go consumer()
		}
	}

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

testLoop:
	for {
		select {
		case <-ticker.C:
			if !onceStarted {
				startThrottler()
				onceStarted = true
				continue
			}

			currQueueLen := len(itemsQueue)
			if currQueueLen == 0 {
				break testLoop
			}

			totalProcessed := atomic.LoadInt64(&itemsProcessed)
			batchProcessed := totalProcessed - prevItemsProcessed

			if prevLowTokens < numOfSlots {
				// Allowing off-by-one error due to a possible race condition
				// It is possible that a consumer might be executing CanSend function concurrently when here
				// As a result the the itemsProcessed does not reflect the exact expected count(prevLowTokens)
				// Hence allow a delta of 1.
				assert.InDelta(t, float64(numOfSlots), float64(batchProcessed), 1)
			} else {
				assert.InDelta(t, float64(prevLowTokens), float64(batchProcessed), 1)
			}

			// grow by 10%
			newLowTokens := batchProcessed + (batchProcessed * 10 / 100)
			throttler.UpdateSettings(map[string]interface{}{
				throttlerSvc.LowTokensKey: newLowTokens,
			})
			prevLowTokens = newLowTokens
			prevItemsProcessed = totalProcessed
		}
	}

	wg.Wait()

	// Final assertion: All items should be processed
	assert.Equal(t, int64(itemCount), atomic.LoadInt64(&itemsProcessed), "Not all items were processed")
}
