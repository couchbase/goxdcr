// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package throttlerSvcImpl

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
)

// ThroughputThrottler limits throughput of replication
type ThroughputThrottler struct {
	id string

	// throughput limit for low priority replications, as number of mutations per second
	throughput_limit int64
	// remaining quota for low priority replications, as number of mutations
	throughput_quota int64
	// tokens for high priority replications, as number of mutations per second
	high_tokens int64
	// conflictLogEnabled tells whether the said feature is enabled or not
	conflictLogEnabled *base.AtomicBooleanType
	// tokens for conflict logging
	conflictLogTokens int64
	// tokens for conflict logging per slot
	conflictLogQuota int64
	// unitConflictLogTokens is the number of tokens to consume when conflict log is request
	unitConflictLogTokens int64
	// high tokens that are not used up by high priority replications, as number of mutations.
	// it can be negative, in which case it needs to be brought back to the positive territory
	// by future unused tokens before any tokens can be reassigned to low priority replications
	unused_high_tokens int64
	// unused high tokens that have been reassigned to low priority replications, as number of mutations.
	reassigned_high_tokens int64
	// max number of base high tokens that can be reassigned to low priority replications in a time slot
	max_reassignable_tokens_per_slot_base int64
	// remainder from integer division when computing max reassignable tokens per slot.
	// distributed as +1 token to one slot at a time until the remainder is exhausted.
	max_reassignable_tokens_per_slot_remainder uint32
	// whether calibation(stop reassigning tokens to low priority replications) is needed
	needToCalibrate *base.AtomicBooleanType
	// number of times update() method is called. needed for calibration
	updateCount uint64

	// Marks the updateCount at the start of a new token cycle i.e every refresh cycle of the resource manager(1sec).
	// used to uniformly distribute the remainder from integer division
	// when splitting tokens uniformly across base.NumberOfSlotsForThroughputThrottling.
	cycleStartMarker uint64

	cond_var *sync.Cond

	finish_ch chan bool
	wait_grp  sync.WaitGroup

	once sync.Once

	logger *log.CommonLogger
}

func NewThroughputThrottlerSvc(logger_ctx *log.LoggerContext) *ThroughputThrottler {
	return &ThroughputThrottler{
		finish_ch:             make(chan bool),
		cond_var:              sync.NewCond(&sync.Mutex{}),
		conflictLogEnabled:    base.NewAtomicBooleanType(false),
		needToCalibrate:       base.NewAtomicBooleanType(true),
		unitConflictLogTokens: throttlerSvc.UnitConflictLogTokens,
		logger:                log.NewLogger("TpThrottler", logger_ctx)}
}

func (t *ThroughputThrottler) SetId(id string) {
	t.id = id
}

func (t *ThroughputThrottler) replenishClogTokens() {
	// update throughput_quota
	for {
		limit := atomic.LoadInt64(&t.conflictLogTokens)
		if limit == 0 {
			// nothing to do
			break
		}

		quota := atomic.LoadInt64(&t.conflictLogQuota)
		// increment quota
		new_quota := quota + limit/int64(base.NumberOfSlotsForThroughputThrottling)
		if new_quota > limit {
			// keep quota below limit
			new_quota = limit
		}
		if atomic.CompareAndSwapInt64(&t.conflictLogQuota, quota, new_quota) {
			break
		}
	}
}

func (throttler *ThroughputThrottler) Start() error {
	throttler.logger.Infof("%v starting...", throttler.id)

	throttler.wait_grp.Add(1)
	go throttler.update()

	throttler.wait_grp.Add(1)
	go throttler.clearReassignedTokens()

	throttler.wait_grp.Add(1)
	go throttler.logStats()

	return nil
}

func (throttler *ThroughputThrottler) Stop() error {
	throttler.logger.Infof("%v stopping...", throttler.id)
	defer throttler.logger.Infof("%v stopped...", throttler.id)

	close(throttler.finish_ch)
	throttler.wait_grp.Wait()

	return nil
}

// update throttler state after each measurement interval
func (throttler *ThroughputThrottler) update() error {
	throttler.logger.Infof("%v update routine starting", throttler.id)
	defer throttler.logger.Infof("%v update routine stopped...", throttler.id)

	defer throttler.wait_grp.Done()

	finish_ch := throttler.finish_ch

	ticker := time.NewTicker(time.Second / time.Duration(base.NumberOfSlotsForThroughputThrottling))
	defer ticker.Stop()

	for {
		select {
		case <-finish_ch:
			throttler.logger.Infof("%v received finish signal and is exitting", throttler.id)
			return nil
		case <-ticker.C:
			throttler.updateOnce()
		}
	}

	return nil
}

func (throttler *ThroughputThrottler) updateOnce() {
	var new_throughput_quota int64
	var currentUpdateCount uint64
	var cycleStartMarker uint64

	// update throughput_quota
	for {
		throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)
		if throughput_limit == 0 {
			// nothing to do
			break
		}
		// load the current update count and the cycleStartMarker
		currentUpdateCount = atomic.LoadUint64(&throttler.updateCount)
		cycleStartMarker = atomic.LoadUint64(&throttler.cycleStartMarker)

		// Load the current throughput quota
		currentQuota := atomic.LoadInt64(&throttler.throughput_quota)

		// Start with the existing quota to preserve any unused tokens
		new_throughput_quota = currentQuota

		// Compute the per-slot base quota and remainder
		baseQuota := throughput_limit / int64(base.NumberOfSlotsForThroughputThrottling)
		remainder := throughput_limit % int64(base.NumberOfSlotsForThroughputThrottling)

		// If the throughput limit is very low and the quota is depleted, inject a minimal quota to unblock processing
		if throughput_limit < int64(base.NumberOfSlotsForThroughputThrottling) && currentQuota == 0 {
			new_throughput_quota += 1
		} else {
			// Distribute the remainder tokens evenly across the initial slots in the cycle.
			// The remainder is always in the range [1, base.NumberOfSlotsForThroughputThrottling - 1].
			//
			// The resource manager (RM) computes the low token budget (throughput_limit) every second,
			// and this budget is distributed across 'base.NumberOfSlotsForThroughputThrottling' slots,
			// with a slot computed every (1 / base.NumberOfSlotsForThroughputThrottling) seconds.
			//
			// To evenly distribute the leftover tokens from integer division (the remainder),
			// we assign one extra token to each of the first 'remainder' slots in the current cycle.
			// For example, if throughput_limit is 17 and NumberOfSlotsForThroughputThrottling is 10,
			// then baseQuota is 1 and remainder is 7. The first 7 slots will each receive 1 extra token.
			if uint32(remainder) > uint32(currentUpdateCount-cycleStartMarker) {
				baseQuota += 1
			}
			new_throughput_quota += baseQuota
		}

		if new_throughput_quota > throughput_limit {
			// keep quota below limit
			new_throughput_quota = throughput_limit
		}
		if atomic.CompareAndSwapInt64(&throttler.throughput_quota, currentQuota, new_throughput_quota) {
			break
		}
	}

	throttler.replenishClogTokens()

	// process unused_high_tokens
	high_tokens := atomic.LoadInt64(&throttler.high_tokens)
	new_high_tokens := high_tokens / int64(base.NumberOfSlotsForThroughputThrottling)
	high_remainder := high_tokens % int64(base.NumberOfSlotsForThroughputThrottling)
	// distribute the remainder if it isn't exhausted yet
	if uint32(high_remainder) > uint32(currentUpdateCount-cycleStartMarker) {
		new_high_tokens = new_high_tokens + 1
	}

	// reassign tokes to low priority replications when
	// 1. calibration is not enabled
	// or 2. calibration is enabled, and current time slot is not picked for calibration
	if !throttler.needToCalibrate.Get() || int(math.Mod(float64(throttler.updateCount), float64(base.IntervalForThrottlerCalibration))) != 0 {
		max_reassignable_tokens_per_slot := atomic.LoadInt64(&throttler.max_reassignable_tokens_per_slot_base)
		max_reassignable_tokens_per_slot_remainder := atomic.LoadUint32(&throttler.max_reassignable_tokens_per_slot_remainder)
		// distribute the remainder if it isn't exhausted yet
		if max_reassignable_tokens_per_slot_remainder > uint32(currentUpdateCount-cycleStartMarker) {
			max_reassignable_tokens_per_slot += 1
		}

		// tokens that can be reassigned to low priority replications
		// = min(unused_high_tokens, max_reassignable_high_tokens)
		// Conceptually:
		// ---------------
		// = min(highTokens - actuallyUsedTokens, highTokens - meanHighThroughput)
		// = highTokens - max(actuallyUsedTokens, meanHighThroughput).

		// reassignable_high_tokens is the smaller of unused token and max_reassignable_tokens_per_slot
		reassignable_high_tokens := atomic.LoadInt64(&throttler.unused_high_tokens)
		throttler.logger.Debugf("thpt throttler update loop 1 in max_reassignable_tokens_per_slot=%d, unused_high_tokens=%d",
			max_reassignable_tokens_per_slot, reassignable_high_tokens)
		if reassignable_high_tokens > max_reassignable_tokens_per_slot {
			reassignable_high_tokens = max_reassignable_tokens_per_slot
		}

		// reassign high tokens, which could be negative, to low priority replications
		atomic.AddInt64(&throttler.reassigned_high_tokens, reassignable_high_tokens)
		throttler.logger.Debugf("thpt throttler update loop 2 in max_reassignable_tokens_per_slot=%d, reassigned_high_tokens=%d",
			max_reassignable_tokens_per_slot, throttler.reassigned_high_tokens)
	}

	// assign more high tokens for the next time slot
	// note that CompareAndSwap is not used here because
	// 1. it is not easy to handle failure to CompareAndSwap since the operation on reassigned_high_tokens above is not idempotent
	// 2. unused_high_tokens being a little off due to race condition is not a critial problem
	atomic.StoreInt64(&throttler.unused_high_tokens, new_high_tokens)

	atomic.AddUint64(&throttler.updateCount, 1)

	throttler.broadcast()
}

// clear reassigned high tokens periodically to get a clean slate
func (throttler *ThroughputThrottler) clearReassignedTokens() error {
	throttler.logger.Infof("%v clearReassignedTokens starting... clearing every %v", throttler.id, base.ThroughputThrottlerClearTokensInterval)
	defer throttler.logger.Infof("%v clearReassignedTokens stopped...", throttler.id)

	defer throttler.wait_grp.Done()

	finish_ch := throttler.finish_ch

	ticker := time.NewTicker(base.ThroughputThrottlerClearTokensInterval)
	defer ticker.Stop()

	for {
		select {
		case <-finish_ch:
			return nil
		case <-ticker.C:
			atomic.StoreInt64(&throttler.reassigned_high_tokens, 0)
		}
	}

	return nil
}

// consumeQuota attempts to take 'n' tokens
// If it returns retry = true then caller should call the function again
// 'result' should only be used when retry == false
func (t *ThroughputThrottler) consumeQuota(limit, quota *int64, n int64) (retry, result bool) {
	l := atomic.LoadInt64(limit)
	if l == 0 {
		result = true
		return
	}

	q := atomic.LoadInt64(quota)
	if q-n <= 0 {
		return
	}

	if atomic.CompareAndSwapInt64(quota, q, q-n) {
		result = true
	} else {
		retry = true
	}

	return
}

// consumeReassignedTokens attempts to take 'n' tokens
// If it returns retry = true then caller should call the function again
// 'result' should only be used when retry == false
func (t *ThroughputThrottler) consumeReassignedTokens(n int64) (retry, result bool) {
	// if there is no quota left, check if we can get allowance from reassigned high tokens
	reassigned_high_tokens := atomic.LoadInt64(&t.reassigned_high_tokens)
	if reassigned_high_tokens-n > 0 {
		if atomic.CompareAndSwapInt64(&t.reassigned_high_tokens, reassigned_high_tokens, reassigned_high_tokens-n) {
			result = true
		} else {
			retry = true
		}
	}

	return
}

func (t *ThroughputThrottler) handleHighReq() bool {
	atomic.AddInt64(&t.unused_high_tokens, -1)
	return true
}

func (t *ThroughputThrottler) handleLowReq() bool {
	// for low priority replications, we need to check whether quota/token is available
	for {
		retry, result := t.consumeQuota(&t.throughput_limit, &t.throughput_quota, 1)
		if retry {
			continue
		}

		if result {
			return true
		}

		retry, result = t.consumeReassignedTokens(1)
		if !retry {
			return result
		}
	}
}

func (t *ThroughputThrottler) handleClogReq() bool {
	// for low priority replications, we need to check whether quota/token is available
	for {
		retry, result := t.consumeQuota(&t.conflictLogTokens, &t.conflictLogQuota, t.unitConflictLogTokens)
		if retry {
			continue
		}

		if result {
			return true
		}

		retry, result = t.consumeReassignedTokens(t.unitConflictLogTokens)
		if !retry {
			return result
		}
	}
}

func (t *ThroughputThrottler) canSendWithClog(req throttlerSvc.ThrottlerReq) bool {
	switch req {
	case throttlerSvc.ThrottlerReqHighRepl:
		return t.handleHighReq()
	case throttlerSvc.ThrottlerReqLowRepl:
		return t.handleLowReq()
	case throttlerSvc.ThrottlerReqCLog:
		return t.handleClogReq()
	default:
		return false
	}
}

func (throttler *ThroughputThrottler) canSendWithoutClog(req throttlerSvc.ThrottlerReq) bool {
	if req == throttlerSvc.ThrottlerReqHighRepl {
		// for high priority replications, only bookkeeping is needed
		atomic.AddInt64(&throttler.unused_high_tokens, -1)
		return true
	}

	// Allow clog requests when clog is disabled. This is because of inherent race between
	// enabling the feature and throttler knowing about it.
	if req == throttlerSvc.ThrottlerReqCLog {
		return true
	}

	// for low priority replications, we need to check whether quota/token is available
	for {
		throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)
		if throughput_limit == 0 {
			// if throughput limit is 0, throughput throttling is not enabled
			return true
		}

		// first try to get allowance from throughput quota
		throughput_quota := atomic.LoadInt64(&throttler.throughput_quota)
		if throughput_quota > 0 {
			if atomic.CompareAndSwapInt64(&throttler.throughput_quota, throughput_quota, throughput_quota-1) {
				return true
			} else {
				// throttler.throughput_quota has been updated by some one else. retry the entire op
				continue
			}
		}

		// if there is no quota left, check if we can get allowance from reassigned high tokens
		reassigned_high_tokens := atomic.LoadInt64(&throttler.reassigned_high_tokens)
		if reassigned_high_tokens > 0 {
			if atomic.CompareAndSwapInt64(&throttler.reassigned_high_tokens, reassigned_high_tokens, reassigned_high_tokens-1) {
				return true
			} else {
				// throttler.reassigned_high_tokens has been updated by some one else. retry the entire op
				continue
			}
		}

		// no quota/token available. cannot send
		return false
	}
}

// output:
// true - if the mutation can be sent
// false - if the mutation cannot be sent
func (throttler *ThroughputThrottler) CanSend(req throttlerSvc.ThrottlerReq) bool {
	clogEnabled := throttler.conflictLogEnabled.Get()

	if clogEnabled {
		return throttler.canSendWithClog(req)
	}

	return throttler.canSendWithoutClog(req)
}

// blocks till the next measurement interval, when throughput usage allowance may become available
func (throttler *ThroughputThrottler) Wait() {
	throttler.cond_var.L.Lock()
	defer throttler.cond_var.L.Unlock()
	throttler.cond_var.Wait()
}

// braodcast availability of new throughput allowance
func (throttler *ThroughputThrottler) broadcast() {
	throttler.cond_var.L.Lock()
	defer throttler.cond_var.L.Unlock()
	throttler.cond_var.Broadcast()
}

func (throttler *ThroughputThrottler) Id() string {
	return throttler.id
}

func (throttler *ThroughputThrottler) UpdateSettings(settings map[string]interface{}) map[string]error {
	errMap := make(map[string]error)
	highTokens, ok := settings[throttlerSvc.HighTokensKey]
	if ok {
		err := throttler.setHighTokens(highTokens.(int64))
		if err != nil {
			errMap[throttlerSvc.HighTokensKey] = err
		}
	}

	maxReassignableHighTokens, ok := settings[throttlerSvc.MaxReassignableHighTokensKey]
	if ok {
		err := throttler.setMaxReassignableHighTokens(maxReassignableHighTokens.(int64))
		if err != nil {
			errMap[throttlerSvc.MaxReassignableHighTokensKey] = err
		}
	}

	lowTokens, ok := settings[throttlerSvc.LowTokensKey]
	if ok {
		err := throttler.setLowTokens(lowTokens.(int64))
		if err != nil {
			errMap[throttlerSvc.LowTokensKey] = err
		}
	}

	clogTokens, ok := settings[throttlerSvc.ConflictLogTokensKey]
	if ok {
		err := throttler.setConflictLogTokens(clogTokens.(int64))
		if err != nil {
			errMap[throttlerSvc.ConflictLogTokensKey] = err
		}
	}

	enabled, ok := settings[throttlerSvc.ConflictLogEnabledKey]
	if ok {
		throttler.conflictLogEnabled.Set(enabled.(bool))
	}

	needToCalibrate, ok := settings[throttlerSvc.NeedToCalibrateKey]
	if ok {
		throttler.needToCalibrate.Set(needToCalibrate.(bool))
	}

	// capture the current value of updateCount into cycleStartMarker.
	// this is used to ensure that the remainder from integer division while computing quota is
	// evenly distributed across the base.NumberOfSlotsForThroughputThrottling.
	atomic.StoreUint64(&throttler.cycleStartMarker, atomic.LoadUint64(&throttler.updateCount))

	throttler.logger.Tracef("throughput throttler update settings: %#v", throttler)

	return errMap
}

func (throttler *ThroughputThrottler) setHighTokens(tokens int64) error {
	if tokens < 0 {
		return fmt.Errorf("High tokens cannot be negative. tokens=%v\n", tokens)
	}

	atomic.StoreInt64(&throttler.high_tokens, tokens)

	throttler.once.Do(func() {
		// initialize unused_high_tokens the first time tokens is set
		atomic.StoreInt64(&throttler.unused_high_tokens, tokens/int64(base.NumberOfSlotsForThroughputThrottling))
	})

	return nil
}

func (throttler *ThroughputThrottler) setMaxReassignableHighTokens(tokens int64) error {
	if tokens < 0 {
		return fmt.Errorf("Max reassignable tokens cannot be negative. tokens=%v\n", tokens)
	}

	atomic.StoreInt64(&throttler.max_reassignable_tokens_per_slot_base, tokens/int64(base.NumberOfSlotsForThroughputThrottling))
	atomic.StoreUint32(&throttler.max_reassignable_tokens_per_slot_remainder, uint32(tokens%int64(base.NumberOfSlotsForThroughputThrottling)))

	return nil
}

func (throttler *ThroughputThrottler) setConflictLogTokens(tokens int64) error {
	if tokens < 0 {
		return fmt.Errorf("Conflict log tokens cannot be negative, tokens=%d", tokens)
	}

	atomic.StoreInt64(&throttler.conflictLogTokens, tokens)
	atomic.StoreInt64(&throttler.conflictLogQuota, tokens/int64(base.NumberOfSlotsForThroughputThrottling))

	return nil
}

func (throttler *ThroughputThrottler) setLowTokens(tokens int64) error {
	if tokens < 0 {
		return fmt.Errorf("Throughput limit cannot be negative. limit=%v\n", tokens)
	}

	atomic.StoreInt64(&throttler.throughput_limit, tokens)

	// reset quota accordingly
	newQuota := tokens / int64(base.NumberOfSlotsForThroughputThrottling)
	for {
		oldQuota := atomic.LoadInt64(&throttler.throughput_quota)
		if oldQuota <= newQuota {
			// ok to leave old quota if it is lower than new quota
			break
		} else {
			// this is possible when old tokens > tokens
			// set new quota to honor the newer, smaller, tokens
			if atomic.CompareAndSwapInt64(&throttler.throughput_quota, oldQuota, newQuota) {
				break
			}
		}
	}

	return nil
}

func (throttler *ThroughputThrottler) logStats() {
	throttler.logger.Infof("%v logStats started", throttler.id)
	defer throttler.logger.Infof("%v logStats exited", throttler.id)

	defer throttler.wait_grp.Done()

	logStats_ticker := time.NewTicker(base.ThroughputThrottlerLogInterval)
	defer logStats_ticker.Stop()

	for {
		select {
		case <-throttler.finish_ch:
			return
		case <-logStats_ticker.C:
			throttler.logStatsOnce("")
		}
	}
}

func (throttler *ThroughputThrottler) logStatsOnce(prefixMsg string) {
	throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)

	throughput_quota := atomic.LoadInt64(&throttler.throughput_quota)
	high_tokens := atomic.LoadInt64(&throttler.high_tokens)
	clogTokens := atomic.LoadInt64(&throttler.conflictLogTokens)
	clogQuota := atomic.LoadInt64(&throttler.conflictLogQuota)
	unused_high_tokens := atomic.LoadInt64(&throttler.unused_high_tokens)
	reassigned_high_tokens := atomic.LoadInt64(&throttler.reassigned_high_tokens)
	max_reassignable_tokens_per_slot_base := atomic.LoadInt64(&throttler.max_reassignable_tokens_per_slot_base)
	max_reassignable_tokens_per_slot_remainder := atomic.LoadUint32(&throttler.max_reassignable_tokens_per_slot_remainder)
	needToCalibrate := throttler.needToCalibrate.Get()
	clogEnabled := throttler.conflictLogEnabled.Get()

	msg := ""
	if throughput_quota < 0 {
		msg = "went over the limit. "
	}

	throttler.logger.Infof("%s%v %sthroughput_limit=%v, throughput_quota=%v, high_tokens=%v, clogEnabled:%v, clog_tokens=%v, clog_quota=%v, unused_tokens=%v, reassigned_tokens=%v, reassignable_tokens_base=%v,reassignable_tokens_remainder=%v needToCalibrate=%v\n",
		prefixMsg, throttler.id, msg, throughput_limit, throughput_quota, high_tokens, clogEnabled, clogTokens, clogQuota, unused_high_tokens, reassigned_high_tokens, max_reassignable_tokens_per_slot_base, max_reassignable_tokens_per_slot_remainder, needToCalibrate)
}
