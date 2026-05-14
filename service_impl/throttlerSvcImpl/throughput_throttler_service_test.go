package throttlerSvcImpl

import (
	"sync/atomic"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
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

// TestLowTokensThrottling verifies the corner case fixed in MB-70816:
// when throughput_limit is below NumberOfSlotsForThroughputThrottling,
// the throttler must still inject one token per slot tick (capped at
// numOfSlots), so consumers can sustain numOfSlots items per second even
// with a sub-1-per-slot limit. It also verifies that as the limit grows
// past numOfSlots, exactly `limit` items are released per logical second.
//
// The test drives the throttler with a logical clock — every call to
// updateOnce() advances exactly one slot. There are no goroutines, no
// real-time tickers, and no time.Sleep, so the outcome is independent of
// CPU contention or scheduler latency.
func TestLowTokensThrottling(t *testing.T) {
	const itemCount = 100
	numOfSlots := int64(base.NumberOfSlotsForThroughputThrottling)
	initialLowTokens := int64(1)

	throttler := NewThroughputThrottlerSvc(&log.LoggerContext{})
	throttler.SetId("unit-test")
	throttler.UpdateSettings(map[string]interface{}{
		throttlerSvc.LowTokensKey:  initialLowTokens,
		throttlerSvc.HighTokensKey: int64(0),
	})

	// 1) Cap check: with limit < numOfSlots and no consumer, ticking past
	//    numOfSlots must not let the quota exceed numOfSlots.
	for i := int64(0); i < numOfSlots*3; i++ {
		throttler.updateOnce()
	}
	assert.Equal(t, numOfSlots, atomic.LoadInt64(&throttler.throughput_quota),
		"quota must saturate at numOfSlots when limit < numOfSlots")

	// Drain the accumulated quota so the next phase starts at zero.
	for throttler.CanSend(throttlerSvc.ThrottlerReqLowRepl) {
	}
	assert.Equal(t, int64(0), atomic.LoadInt64(&throttler.throughput_quota))

	// 2) End-to-end throughput: simulate a producer of itemCount items and a
	//    consumer that drains the quota after every slot tick. Each iteration
	//    of the outer loop is one logical second (numOfSlots ticks). After
	//    each second, grow the limit by 10% — mirrors the dynamic-rate growth
	//    the original test exercised.
	var itemsProcessed int64
	currentLimit := initialLowTokens

	for atomic.LoadInt64(&itemsProcessed) < int64(itemCount) {
		prevProcessed := atomic.LoadInt64(&itemsProcessed)

	cycle:
		for slot := int64(0); slot < numOfSlots; slot++ {
			throttler.updateOnce()
			for throttler.CanSend(throttlerSvc.ThrottlerReqLowRepl) {
				if atomic.AddInt64(&itemsProcessed, 1) >= int64(itemCount) {
					break cycle
				}
			}
		}

		batchProcessed := atomic.LoadInt64(&itemsProcessed) - prevProcessed

		// Expected throughput for this cycle:
		//   - limit <  numOfSlots: numOfSlots (one token per slot tick).
		//   - limit >= numOfSlots: exactly `limit` (baseQuota + distributed remainder).
		// The last cycle may be partial when remaining < expected.
		expected := currentLimit
		if currentLimit < numOfSlots {
			expected = numOfSlots
		}
		if remaining := int64(itemCount) - prevProcessed; expected > remaining {
			expected = remaining
		}
		assert.Equal(t, expected, batchProcessed,
			"unexpected throughput in cycle with limit=%d (processed so far=%d)",
			currentLimit, prevProcessed)

		// Grow by 10% (integer arithmetic — matches the original test).
		newLimit := batchProcessed + (batchProcessed * 10 / 100)
		throttler.UpdateSettings(map[string]interface{}{
			throttlerSvc.LowTokensKey: newLimit,
		})
		currentLimit = newLimit
	}

	assert.Equal(t, int64(itemCount), atomic.LoadInt64(&itemsProcessed),
		"Not all items were processed")
}
