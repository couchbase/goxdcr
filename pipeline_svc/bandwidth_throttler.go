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
	"errors"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"sync/atomic"
	"time"
)

// factors affecting bandwidth limit of the current node, which could be updated through UpdateSettings() call
var OVERALL_BANDWIDTH_LIMIT = "overall_bandwidth_limit"
var NUMBER_OF_SOURCE_NODES = "number_of_source_nodes"
var errorZeroSrcNode = errors.New("Error: Bandwidth Throttler's number of source nodes cannot be 0")

//BandwidthThrottler limits bandwidth usage of replication
type BandwidthThrottler struct {
	id string

	xdcr_topology_svc service_def.XDCRCompTopologySvc

	number_of_source_nodes  uint32
	overall_bandwidth_limit int64

	// bandwidth limit for the current node in bytes per second
	bandwidth_limit int64
	// remaining quota for bandwidth usage in bytes
	bandwidth_usage_quota int64

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
	} else if number_of_source_nodes == 0 {
		return errorZeroSrcNode
	}

	throttler.number_of_source_nodes = uint32(number_of_source_nodes)
	throttler.overall_bandwidth_limit = int64(pipeline.Specification().Settings.BandwidthLimit)
	throttler.logger.Infof("%v set overall bandwidth limit to %v and number of source nodes to %v\n", throttler.id, throttler.overall_bandwidth_limit, throttler.number_of_source_nodes)

	bandwidth_limit, err := throttler.setBandwidthLimit()
	if err != nil {
		return err
	}

	throttler.initBandwidthUsageQuota(bandwidth_limit)

	throttler.logger.Infof("%v attached to pipeline", throttler.id)
	return nil
}

func (throttler *BandwidthThrottler) initBandwidthUsageQuota(bandwidth_limit int64) {
	// set quota to limit/numberOfSlots to ensure a slow and smooth start
	atomic.StoreInt64(&throttler.bandwidth_usage_quota, bandwidth_limit/int64(base.NumberOfSlotsForBandwidthThrottling))
}

func (throttler *BandwidthThrottler) Start(settings metadata.ReplicationSettingsMap) error {
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
	var new_bandwidth_usage_quota int64
	for {
		bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
		bandwidth_usage_quota := atomic.LoadInt64(&throttler.bandwidth_usage_quota)
		// increment quota
		new_bandwidth_usage_quota = bandwidth_usage_quota + bandwidth_limit/int64(base.NumberOfSlotsForBandwidthThrottling)
		if new_bandwidth_usage_quota > bandwidth_limit {
			// keep quota below limit
			new_bandwidth_usage_quota = bandwidth_limit
		}
		if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage_quota, bandwidth_usage_quota, new_bandwidth_usage_quota) {
			break
		}
	}

	// if new quota is larger than 0, wake up all routines blocked by throttler.Wait()
	if new_bandwidth_usage_quota > 0 {
		throttler.broadcast()
	}
}

// input:
// 1. numberOfBytes - the total number of bytes that caller wants to send
// 2. minNumberOfBytes - the minimum number of bytes that caller wants to send, if numberOfBytes cannot be send
// 3. numberOfBytesOfFirstItem - the number of bytes of the first item in the caller's list

// numberOfBytesOfFirstItem is needed to prevent stuckness and starvation.
// 1. stuckness: if a document has size > bandwidth_limit, it will never get sent
//    if we do not let bandwidth_usage_quota fall below 0
// 2. starvation: if a document has size > bandwidth_limit/numberOfSlots, it may never get sent
//    if we do not let bandwidth_usage_quota fall below 0, since mutations in other vbuckets
//    could keep using up bandwidth and never let bandwidth_usage_quota exceed bandwidth_limit/numberOfSlots
// In order to prevent these scenarios, we always send the first document if it has size > bandwidth_limit/numberOfSlots,
// even if bandwidth_usage_quota falls below 0 as a result.
// When this happens, we will wait for bandwidth_usage_quota to climb back to the positive territory before we send any more mutations.

// output:
// 1. bytesCanSend - the number of bytes that caller can proceed to send.
//    it can take one of four values: numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem, 0
// 2. bytesAllowed - the number of bytes remaining in bandwidth allowance that caller can ATTEMPT to send
//    caller cannot just send this number of bytes, though. It has to call Throttle() again
func (throttler *BandwidthThrottler) Throttle(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem int64) (bytesCanSend int64, bytesAllowed int64) {
	bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
	if bandwidth_limit == 0 {
		// if bandwidth limit is 0, bandwdith throttling is not enabled
		return numberOfBytes, 0
	}

	for {
		bandwidth_usage_quota := atomic.LoadInt64(&throttler.bandwidth_usage_quota)
		if bandwidth_usage_quota <= 0 {
			return 0, 0
		}
		// first check if numberOfBytes can be sent
		if bandwidth_usage_quota >= numberOfBytes {
			// numberOfBytes can be sent
			if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage_quota, bandwidth_usage_quota, bandwidth_usage_quota-numberOfBytes) {
				return numberOfBytes, 0
			} else {
				// throttler.bandwidth_usage_quota has been updated by some one else. retry the entire op
				continue
			}
			// if numberOfBytes cannot be sent, check if minNumberOfBytes can be sent
		} else if bandwidth_usage_quota >= minNumberOfBytes {
			// minNumberOfBytes can be sent
			if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage_quota, bandwidth_usage_quota, bandwidth_usage_quota-minNumberOfBytes) {
				return minNumberOfBytes, bandwidth_usage_quota - minNumberOfBytes
			} else {
				// throttler.bandwidth_usage_quota has been updated by some one else. retry the entire op
				continue
			}
		} else {
			// if even minNumberOfBytes cannot be sent, check if first item should be sent
			bandwidth_usage_per_slot := bandwidth_limit / int64(base.NumberOfSlotsForBandwidthThrottling)
			if bandwidth_usage_per_slot < numberOfBytesOfFirstItem {
				// If first item is big, sends it to avoid stuckness and starvation,
				// even if bandwidth_usage_quota may drop below 0 afterward
				new_bandwidth_usage_quota := bandwidth_usage_quota - numberOfBytesOfFirstItem
				if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage_quota, bandwidth_usage_quota, new_bandwidth_usage_quota) {
					if new_bandwidth_usage_quota > 0 {
						return numberOfBytesOfFirstItem, new_bandwidth_usage_quota
					} else {
						return numberOfBytesOfFirstItem, 0
					}
				} else {
					// throttler.bandwidth_usage_quota has been updated by some one else. retry the entire op
					continue
				}
			}
			// when we get there, nothing can be sent
			return 0, bandwidth_usage_quota
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

func (throttler *BandwidthThrottler) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	if throttler.logger.GetLogLevel() >= log.LogLevelDebug {
		throttler.logger.Debugf("%v Updating settings. settings=%v\n", throttler.id, settings.CloneAndRedact())
	}
	overall_bandwidth_limit := settings[OVERALL_BANDWIDTH_LIMIT]
	if overall_bandwidth_limit != nil {
		atomic.StoreInt64(&throttler.overall_bandwidth_limit, int64(overall_bandwidth_limit.(int)))
		throttler.logger.Infof("%v updated overall bandwidth limit to %v\n", throttler.id, overall_bandwidth_limit)
	}
	number_of_source_nodes := settings[NUMBER_OF_SOURCE_NODES]
	if number_of_source_nodes != nil {
		if uint32(number_of_source_nodes.(int)) > 0 {
			atomic.StoreUint32(&throttler.number_of_source_nodes, uint32(number_of_source_nodes.(int)))
			throttler.logger.Infof("%v updated number of source node to %v\n", throttler.id, number_of_source_nodes)
		} else {
			return errorZeroSrcNode
		}
	}

	if overall_bandwidth_limit != nil || number_of_source_nodes != nil {
		bandwidth_limit, err := throttler.setBandwidthLimit()
		if err != nil {
			throttler.logger.Errorf(err.Error())
			return err
		}

		throttler.adjustBandwidthUsageQuota(bandwidth_limit)
	}

	return nil
}

func (throttler *BandwidthThrottler) setBandwidthLimit() (int64, error) {
	number_of_source_nodes := atomic.LoadUint32(&throttler.number_of_source_nodes)
	if number_of_source_nodes == 0 {
		return 0, errorZeroSrcNode
	}
	overall_bandwidth_limit := atomic.LoadInt64(&throttler.overall_bandwidth_limit)
	bandwidth_limit := overall_bandwidth_limit * 1024 * 1024 / int64(number_of_source_nodes)
	atomic.StoreInt64(&throttler.bandwidth_limit, bandwidth_limit)
	throttler.logger.Infof("%v updated bandwidth limit to %v\n", throttler.id, bandwidth_limit)

	return bandwidth_limit, nil
}

// adjust quota when new limit is set
func (throttler *BandwidthThrottler) adjustBandwidthUsageQuota(bandwidth_limit int64) {
	for {
		bandwidth_usage_quota := atomic.LoadInt64(&throttler.bandwidth_usage_quota)
		if bandwidth_usage_quota <= bandwidth_limit {
			// nothing to do
			return
		}

		// adjust quota to ensure that it is within limit
		if atomic.CompareAndSwapInt64(&throttler.bandwidth_usage_quota, bandwidth_usage_quota, bandwidth_limit) {
			return
		}
	}
}

func (throttler *BandwidthThrottler) PrintStatusSummary() {
	bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
	if bandwidth_limit == 0 {
		return
	}
	bandwidth_usage_quota := atomic.LoadInt64(&throttler.bandwidth_usage_quota)
	if bandwidth_usage_quota < 0 {
		throttler.logger.Warnf("%v went over the limit. Need cool down before more mutations can be sent. bandwidth_limit=%v, bandwidth_usage_quota=%v", throttler.id, bandwidth_limit, bandwidth_usage_quota)
	} else {
		throttler.logger.Infof("%v bandwidth_limit=%v, bandwidth_usage_quota=%v", throttler.id, bandwidth_limit, bandwidth_usage_quota)
	}
}
