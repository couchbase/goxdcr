// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_svc

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"sync/atomic"
	"time"
)

// factors affecting bandwidth limit of the current node, which could be updated through UpdateSettings() call
var OVERALL_BANDWIDTH_LIMIT = "overall_bandwidth_limit"
var NUMBER_OF_SOURCE_NODES = "number_of_source_nodes"

//BandwidthThrottler limits bandwidth usage of replication
type BandwidthThrottler struct {
	id string

	xdcr_topology_svc service_def.XDCRCompTopologySvc

	number_of_source_nodes  uint32
	overall_bandwidth_limit int64

	// bandwidth limit for the current node in byte/sec
	bandwidth_limit int64
	// bandwidth usage in the current 1 second interval
	bandwidth_usage int64

	bandwidth_usage_history     []int64
	bandwidth_usage_history_sum int64

	cond_var *sync.Cond

	finish_ch chan bool
	wait_grp  sync.WaitGroup

	logger *log.CommonLogger
}

func NewBandwidthThrottlerSvc(xdcr_topology_svc service_def.XDCRCompTopologySvc,
	logger_ctx *log.LoggerContext) *BandwidthThrottler {
	return &BandwidthThrottler{
		xdcr_topology_svc: xdcr_topology_svc,
		finish_ch:         make(chan bool),
		cond_var:          sync.NewCond(&sync.Mutex{}),
		logger:            log.NewLogger("BwThrottler", logger_ctx)}
}

func (throttler *BandwidthThrottler) Attach(pipeline common.Pipeline) error {
	throttler.id = pipeline_utils.GetElementIdFromName(pipeline, base.BANDWIDTH_THROTTLER_SVC)

	number_of_source_nodes, err := throttler.xdcr_topology_svc.NumberOfKVNodes()
	if err != nil {
		return err
	}
	throttler.number_of_source_nodes = uint32(number_of_source_nodes)
	throttler.overall_bandwidth_limit = int64(pipeline.Specification().Settings.BandwidthLimit)
	throttler.logger.Infof("%v set overall bandwidth limit to %v and number of source nodes to %v\n", throttler.id, throttler.overall_bandwidth_limit, throttler.number_of_source_nodes)

	throttler.setBandwidthLimit()

	throttler.initBandwidthUsageHistory()

	throttler.logger.Infof("%v attached to pipeline", throttler.id)
	return nil
}

func (throttler *BandwidthThrottler) initBandwidthUsageHistory() {
	// bandwidth usage of the current time slot is captured in bandwidth_usage, and not in bandwidth_usage_history.
	// that is why length of history is (number of slots - 1)
	throttler.bandwidth_usage_history = make([]int64, base.NumberOfSlotsForBandwidthThrottling-1)

	// initialize slots in bandwidth history with usage = bandwidth_limit/numberOfSlots to smooth out initial traffic
	usage_per_slot := throttler.bandwidth_limit / int64(base.NumberOfSlotsForBandwidthThrottling)
	for i := 0; i < base.NumberOfSlotsForBandwidthThrottling-1; i++ {
		throttler.bandwidth_usage_history[i] = usage_per_slot
	}
	usage_sum := usage_per_slot * int64(base.NumberOfSlotsForBandwidthThrottling-1)
	throttler.bandwidth_usage_history_sum = usage_sum
	throttler.bandwidth_usage = usage_sum
}

func (throttler *BandwidthThrottler) Start(settings map[string]interface{}) error {
	throttler.logger.Infof("%v starting...", throttler.id)

	throttler.wait_grp.Add(1)
	go throttler.update()

	return nil
}

func (throttler *BandwidthThrottler) Stop() error {
	throttler.logger.Infof("%v stopping...", throttler.id)
	defer throttler.logger.Infof("%v stopped...", throttler.id)

	close(throttler.finish_ch)
	throttler.wait_grp.Wait()

	return nil
}

// update throttler state after each measurement interval
func (throttler *BandwidthThrottler) update() error {
	throttler.logger.Infof("%v reset routine starting", throttler.id)
	defer throttler.logger.Infof("%v reset routine stopped...", throttler.id)

	defer throttler.wait_grp.Done()

	finish_ch := throttler.finish_ch

	// measurement interval is time.Second/base.NumberOfSlotsForBandwidthThrottling
	ticker := time.NewTicker(time.Second / time.Duration(base.NumberOfSlotsForBandwidthThrottling))
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

func (throttler *BandwidthThrottler) updateOnce() {
	length := len(throttler.bandwidth_usage_history)
	// track the oldest entry in history before it gets rotated out
	usage_oldest_slot := throttler.bandwidth_usage_history[0]
	for i := 1; i < length; i++ {
		// rotate entries in bandwidth_usage_history to the left - the first/oldest entry gets rotated out
		throttler.bandwidth_usage_history[i-1] = throttler.bandwidth_usage_history[i]
	}

	// decrement bandwidth usage by usage_oldest_slot since it is no longer in the current 1 second moving window
	new_bandwith_usage := atomic.AddInt64(&throttler.bandwidth_usage, 0-int64(usage_oldest_slot))
	throttler.bandwidth_usage_history_sum -= usage_oldest_slot
	// compute usage in the current time slot
	usage_current_slot := new_bandwith_usage - throttler.bandwidth_usage_history_sum
	// move the current time slot into history
	throttler.bandwidth_usage_history[length-1] = usage_current_slot
	throttler.bandwidth_usage_history_sum += usage_current_slot

	// wake up all routines blocked by throttler.Wait()
	throttler.broadcast()
}

// input:
// 1. numberOfBytes - the total number of bytes that caller wants to send
// 2. minNumberOfBytes - the minimum number of bytes that caller wants to send, if numberOfBytes cannot be send
// output:
// 1. bytesCanSend - the number of bytes that caller can proceed to send.
//    it can take one of three values: numberOfBytes, minNumberOfBytes, 0
// 2. bytesAllowed - the number of bytes remaining in bandwidth allowance that caller can ATTEMPT to send
//    caller cannot just send this number of bytes, though. It has to call Throttle() again
func (throttler *BandwidthThrottler) Throttle(numberOfBytes int, minNumberOfBytes int) (bytesCanSend int, bytesAllowed int64) {
	bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
	if bandwidth_limit == 0 {
		// if bandwidth limit is 0, bandwdith throttling is not enabled
		return numberOfBytes, 0
	}

	for {
		bandwidth_usage := atomic.LoadInt64(&throttler.bandwidth_usage)
		// first check if numberOfBytes can be sent
		new_bandwidth_usage := bandwidth_usage + int64(numberOfBytes)
		if new_bandwidth_usage <= bandwidth_limit {
			// numberOfBytes can be sent
			if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage, bandwidth_usage, new_bandwidth_usage) {
				return numberOfBytes, 0
			} else {
				// throttler.bandwidth_usage has been updated by some one else. retry the entire op
				continue
			}
		} else {
			// if numberOfBytes cannot be sent, check if minNumberOfBytes can be sent
			new_bandwidth_usage = bandwidth_usage + int64(minNumberOfBytes)
			if new_bandwidth_usage <= bandwidth_limit {
				// minNumberOfBytes can be sent
				if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage, bandwidth_usage, new_bandwidth_usage) {
					return minNumberOfBytes, bandwidth_limit - new_bandwidth_usage
				} else {
					// throttler.bandwidth_usage has been updated by some one else. retry the entire op
					continue
				}
			} else {
				// even minNumberOfBytes can not be sent
				return 0, bandwidth_limit - bandwidth_usage
			}
		}
	}
}

// blocks till the next measurement interval, when bandwidth usage allowance may become available
func (throttler *BandwidthThrottler) Wait() {
	throttler.cond_var.L.Lock()
	defer throttler.cond_var.L.Unlock()
	throttler.cond_var.Wait()
}

// braodcast availability of new bandwidth allowance
func (throttler *BandwidthThrottler) broadcast() {
	throttler.cond_var.L.Lock()
	defer throttler.cond_var.L.Unlock()
	throttler.cond_var.Broadcast()
}

func (throttler *BandwidthThrottler) Id() string {
	return throttler.id
}

func (throttler *BandwidthThrottler) UpdateSettings(settings map[string]interface{}) error {
	throttler.logger.Debugf("%v Updating settings. settings=%v\n", throttler.id, settings)
	overall_bandwidth_limit := settings[OVERALL_BANDWIDTH_LIMIT]
	if overall_bandwidth_limit != nil {
		atomic.StoreInt64(&throttler.overall_bandwidth_limit, int64(overall_bandwidth_limit.(int)))
		throttler.logger.Infof("%v updated overall bandwidth limit to %v\n", throttler.id, overall_bandwidth_limit)
	}
	number_of_source_nodes := settings[NUMBER_OF_SOURCE_NODES]
	if number_of_source_nodes != nil {
		atomic.StoreUint32(&throttler.number_of_source_nodes, uint32(number_of_source_nodes.(int)))
		throttler.logger.Infof("%v updated number of source node to %v\n", throttler.id, number_of_source_nodes)
	}

	if overall_bandwidth_limit != nil || number_of_source_nodes != nil {
		throttler.setBandwidthLimit()
	}

	return nil
}

func (throttler *BandwidthThrottler) setBandwidthLimit() {
	number_of_source_nodes := atomic.LoadUint32(&throttler.number_of_source_nodes)
	overall_bandwidth_limit := atomic.LoadInt64(&throttler.overall_bandwidth_limit)
	bandwidth_limit := overall_bandwidth_limit * 1024 * 1024 / int64(number_of_source_nodes)
	atomic.StoreInt64(&throttler.bandwidth_limit, bandwidth_limit)
	throttler.logger.Infof("%v updated bandwidth limit to %v\n", throttler.id, bandwidth_limit)
}

func (throttler *BandwidthThrottler) StatusSummary() string {
	bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
	bandwidth_usage := atomic.LoadInt64(&throttler.bandwidth_usage)
	if bandwidth_usage > bandwidth_limit {
		throttler.logger.Errorf("%v went over the limit. bandwidth_limit=%v, bandwidth_usage=%v", throttler.id, bandwidth_limit, bandwidth_usage)
	}
	return fmt.Sprintf("%v bandwidth_limit=%v, bandwidth_usage=%v", throttler.id, bandwidth_limit, bandwidth_usage)
}
