// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

//ThroughputThrottler limits throughput of replication
type ThroughputThrottler struct {
	id string

	// throughput limit for low priority replications, as number of mutations per second
	throughput_limit int64
	// remaining quota for low priority replications, as number of mutations
	throughput_quota int64
	// tokens for high priority replications, as number of mutations per second
	high_tokens int64
	// high tokens that are not used up by high priority replications, as number of mutations.
	// it can be negative, in which case it needs to be brought back to the positive territory
	// by future unused tokens before any tokens can be reassigned to low priority replications
	unused_high_tokens int64
	// unused high tokens that have been reassigned to low priority replications, as number of mutations.
	reassigned_high_tokens int64
	// max number of high tokens that can be reassigned to low priority replications in a time slot
	max_reassignable_tokens_per_slot int64
	// whether calibation(stop reassigning tokens to low priority replications) is needed
	needToCalibrate *base.AtomicBooleanType
	// number of times update() method is called. needed for calibration
	updateCount uint64

	cond_var *sync.Cond

	finish_ch chan bool
	wait_grp  sync.WaitGroup

	once sync.Once

	logger *log.CommonLogger
}

func NewThroughputThrottlerSvc(logger_ctx *log.LoggerContext) *ThroughputThrottler {
	return &ThroughputThrottler{
		finish_ch:       make(chan bool),
		cond_var:        sync.NewCond(&sync.Mutex{}),
		needToCalibrate: base.NewAtomicBooleanType(true),
		logger:          log.NewLogger("TpThrottler", logger_ctx)}
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

	// update throughput_quota
	for {
		throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)
		if throughput_limit == 0 {
			// nothing to do
			break
		}

		throughput_quota := atomic.LoadInt64(&throttler.throughput_quota)
		// increment quota
		new_throughput_quota = throughput_quota + throughput_limit/int64(base.NumberOfSlotsForThroughputThrottling)
		if new_throughput_quota > throughput_limit {
			// keep quota below limit
			new_throughput_quota = throughput_limit
		}
		if atomic.CompareAndSwapInt64(&throttler.throughput_quota, throughput_quota, new_throughput_quota) {
			break
		}
	}

	// process unused_high_tokens
	high_tokens := atomic.LoadInt64(&throttler.high_tokens)
	new_high_tokens := high_tokens / int64(base.NumberOfSlotsForThroughputThrottling)

	// reassign tokes to low priority replications when
	// 1. calibration is not enabled
	// or 2. calibration is enabled, and current time slot is not picked for calibration
	if !throttler.needToCalibrate.Get() || int(math.Mod(float64(throttler.updateCount), float64(base.IntervalForThrottlerCalibration))) != 0 {
		max_reassignable_tokens_per_slot := atomic.LoadInt64(&throttler.max_reassignable_tokens_per_slot)

		// tokens that can be reassigned to low priority replications
		// = min(unused_high_tokens, max_reassignable_high_tokens)
		// Conceptually:
		// ---------------
		// = min(highTokens - actuallyUsedTokens, highTokens - meanHighThroughput)
		// = highTokens - max(actuallyUsedTokens, meanHighThroughput).

		// reassignable_high_tokens is the smaller of unused token and max_reassignable_tokens_per_slot
		reassignable_high_tokens := atomic.LoadInt64(&throttler.unused_high_tokens)
		if reassignable_high_tokens > max_reassignable_tokens_per_slot {
			reassignable_high_tokens = max_reassignable_tokens_per_slot
		}

		// reassign high tokens, which could be negative, to low priority replications
		atomic.AddInt64(&throttler.reassigned_high_tokens, reassignable_high_tokens)
	}

	// assign more high tokens for the next time slot
	// note that CompareAndSwap is not used here because
	// 1. it is not easy to handle failure to CompareAndSwap since the operation on reassigned_high_tokens above is not idempotent
	// 2. unused_high_tokens being a little off due to race condition is not a critial problem
	atomic.StoreInt64(&throttler.unused_high_tokens, new_high_tokens)

	throttler.updateCount++

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

// output:
// true - if the mutation can be sent
// false - if the mutation cannot be sent
func (throttler *ThroughputThrottler) CanSend(isHighPriorityReplication bool) bool {
	if isHighPriorityReplication {
		// for high priority replications, only bookkeeping is needed
		atomic.AddInt64(&throttler.unused_high_tokens, -1)
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
	highTokens, ok := settings[service_def.HighTokensKey]
	if ok {
		err := throttler.setHighTokens(highTokens.(int64))
		if err != nil {
			errMap[service_def.HighTokensKey] = err
		}
	}

	maxReassignableHighTokens, ok := settings[service_def.MaxReassignableHighTokensKey]
	if ok {
		err := throttler.setMaxReassignableHighTokens(maxReassignableHighTokens.(int64))
		if err != nil {
			errMap[service_def.MaxReassignableHighTokensKey] = err
		}
	}

	lowTokens, ok := settings[service_def.LowTokensKey]
	if ok {
		err := throttler.setLowTokens(lowTokens.(int64))
		if err != nil {
			errMap[service_def.LowTokensKey] = err
		}
	}

	needToCalibrate, ok := settings[service_def.NeedToCalibrateKey]
	if ok {
		throttler.needToCalibrate.Set(needToCalibrate.(bool))
	}

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

	atomic.StoreInt64(&throttler.max_reassignable_tokens_per_slot, tokens/int64(base.NumberOfSlotsForThroughputThrottling))

	return nil
}

func (throttler *ThroughputThrottler) setLowTokens(tokens int64) error {
	if tokens < 0 {
		return fmt.Errorf("Throughput limit cannot be negative. limit=%v\n", tokens)
	}

	atomic.StoreInt64(&throttler.throughput_limit, tokens)

	if tokens == 0 {
		return nil
	}

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
			throttler.logStatsOnce()
		}
	}
}

func (throttler *ThroughputThrottler) logStatsOnce() {
	throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)
	if throughput_limit == 0 {
		return
	}
	throughput_quota := atomic.LoadInt64(&throttler.throughput_quota)
	high_tokens := atomic.LoadInt64(&throttler.high_tokens)
	unused_high_tokens := atomic.LoadInt64(&throttler.unused_high_tokens)
	reassigned_high_tokens := atomic.LoadInt64(&throttler.reassigned_high_tokens)
	max_reassignable_tokens_per_slot := atomic.LoadInt64(&throttler.max_reassignable_tokens_per_slot)
	needToCalibrate := throttler.needToCalibrate.Get()
	if throughput_quota < 0 {
		throttler.logger.Errorf("%v went over the limit. throughput_limit=%v, throughput_quota=%v, high_tokens=%v, unused_tokens=%v, reassignable_tokens=%v, reassigned_tokens=%v, needToCalibrate=%v\n", throttler.id, throughput_limit, throughput_quota, high_tokens, unused_high_tokens, reassigned_high_tokens, max_reassignable_tokens_per_slot, needToCalibrate)
	} else {
		throttler.logger.Infof("%v throughput_limit=%v, throughput_quota=%v, high_tokens=%v, unused_tokens=%v, reassignable_tokens=%v, reassigned_tokens=%v, needToCalibrate=%v\n", throttler.id, throughput_limit, throughput_quota, high_tokens, unused_high_tokens, reassigned_high_tokens, max_reassignable_tokens_per_slot, needToCalibrate)
	}
}
