package throttlerSvcImpl

import (
	"testing"

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
		/*
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
		*/
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
