// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"sync"
	"sync/atomic"
	"time"
)

//ThroughputThrottler limits throughput of replication
type ThroughputThrottler struct {
	id string

	// throughput limit as number of mutations per second
	throughput_limit int64
	// remaining quota for throughput as number of mutations
	throughput_quota int64

	cond_var *sync.Cond

	finish_ch chan bool
	wait_grp  sync.WaitGroup

	logger *log.CommonLogger
}

func NewThroughputThrottlerSvc(logger_ctx *log.LoggerContext) *ThroughputThrottler {
	return &ThroughputThrottler{
		finish_ch: make(chan bool),
		cond_var:  sync.NewCond(&sync.Mutex{}),
		logger:    log.NewLogger("TpThrottler", logger_ctx)}
}

func (throttler *ThroughputThrottler) Start() error {
	throttler.logger.Infof("%v starting...", throttler.id)

	throttler.wait_grp.Add(1)
	go throttler.update()

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

	throttler.broadcast()
}

// output:
// true - if the mutation can be sent
// false - if the mutation cannot be sent
func (throttler *ThroughputThrottler) CanSend() bool {
	for {
		throughput_limit := atomic.LoadInt64(&throttler.throughput_limit)
		if throughput_limit == 0 {
			// if throughput limit is 0, throughput throttling is not enabled
			return true
		}

		throughput_quota := atomic.LoadInt64(&throttler.throughput_quota)
		if throughput_quota <= 0 {
			return false
		}

		if atomic.CompareAndSwapInt64(&throttler.throughput_quota, throughput_quota, throughput_quota-1) {
			return true
		} else {
			// throttler.throughput_quota has been updated by some one else. retry the entire op
			continue
		}
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

func (throttler *ThroughputThrottler) SetThroughputLimit(limit int64) error {
	if limit < 0 {
		err := fmt.Errorf("Throughput limit cannot be negative. Limit=%v\n", limit)
		throttler.logger.Warnf("%v %v", throttler.id, err.Error())
		return err
	}

	atomic.StoreInt64(&throttler.throughput_limit, limit)
	throttler.logger.Infof("%v updated throughput limit to %v\n", throttler.id, limit)

	// reset quota accordingly
	newQuota := limit / int64(base.NumberOfSlotsForThroughputThrottling)
	for {
		oldQuota := atomic.LoadInt64(&throttler.throughput_quota)
		if oldQuota <= newQuota {
			// ok to leave old quota if it is lower than new quota
			break
		} else {
			// this is possible when oldLimit > limit
			// set new quota to honor the newer, smaller, limit
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
	if throughput_quota < 0 {
		throttler.logger.Errorf("%v went over the limit. throughput_limit=%v, throughput_quota=%v", throttler.id, throughput_limit, throughput_quota)
	} else {
		throttler.logger.Infof("%v throughput_limit=%v, throughput_quota=%v", throttler.id, throughput_limit, throughput_quota)
	}
}
