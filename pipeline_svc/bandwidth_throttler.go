// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline_utils"
	"github.com/couchbase/goxdcr/v8/service_def"
)

// factors affecting bandwidth limit of the current node, which could be updated through UpdateSettings() call
var OVERALL_BANDWIDTH_LIMIT = "overall_bandwidth_limit"
var NUMBER_OF_SOURCE_NODES = "number_of_source_nodes"

// BandwidthThrottler limits bandwidth usage of replication
type BandwidthThrottler struct {
	id string

	xdcr_topology_svc service_def.XDCRCompTopologySvc

	number_of_source_nodes  uint32
	overall_bandwidth_limit int64

	// bandwidth limit for the current node in bytes per second
	bandwidth_limit      int64
	bandwidth_limit_lock sync.Mutex
	// remaining quota for bandwidth usage in bytes
	bandwidth_usage_quota int64

	cond_var *sync.Cond

	finish_ch chan bool
	wait_grp  sync.WaitGroup

	logger *log.CommonLogger

	attachmentMtx sync.Mutex
	pipelines     []common.Pipeline
}

func NewBandwidthThrottlerSvc(xdcr_topology_svc service_def.XDCRCompTopologySvc,
	logger_ctx *log.LoggerContext) *BandwidthThrottler {
	return &BandwidthThrottler{
		xdcr_topology_svc:    xdcr_topology_svc,
		finish_ch:            make(chan bool),
		cond_var:             sync.NewCond(&sync.Mutex{}),
		bandwidth_limit_lock: sync.Mutex{},
		logger:               log.NewLogger("BwThrottler", logger_ctx)}
}

// Throttler could be attached to multiple pipelines. The first one will always
// be the main pipeline
func (throttler *BandwidthThrottler) Attach(pipeline common.Pipeline) error {
	throttler.attachmentMtx.Lock()
	defer throttler.attachmentMtx.Unlock()

	if len(throttler.pipelines) == 0 {

		throttler.id = pipeline_utils.GetElementIdFromName(pipeline, base.BANDWIDTH_THROTTLER_SVC)

		number_of_source_nodes, err := throttler.xdcr_topology_svc.NumberOfKVNodes()
		if err != nil {
			return err
		}

		err = throttler.updateBandwidthLimitSettings(number_of_source_nodes, true, /*update_number_of_source_nodes*/
			pipeline.Specification().GetReplicationSpec().Settings.BandwidthLimit, true /*update_overall_bandwidth_limit*/)
		if err != nil {
			return err
		}

		throttler.initBandwidthUsageQuota()

		throttler.logger.Infof("%v attached to main pipeline", throttler.id)
	} else {
		throttler.logger.Infof("%v attached to secondary pipeline", throttler.id)
	}

	throttler.pipelines = append(throttler.pipelines, pipeline)
	return nil
}

func (throttler *BandwidthThrottler) initBandwidthUsageQuota() {
	bandwidth_limit := atomic.LoadInt64(&throttler.bandwidth_limit)
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
			// At this point, we wake up all the writers waiting on the throttler one last time.
			throttler.broadcast()
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
//  1. bytesCanSend - the number of bytes that caller can proceed to send.
//     it can take one of four values: numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem, 0
//  2. bytesAllowed - the number of bytes remaining in bandwidth allowance that caller can ATTEMPT to send
//     caller cannot just send this number of bytes, though. It has to call Throttle() again
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

func (throttler *BandwidthThrottler) isClosed() (ok bool) {
	select {
	case <-throttler.finish_ch:
		ok = true
	default:
	}
	return
}

// blocks till the next measurement interval, when bandwidth usage allowance may become available
func (throttler *BandwidthThrottler) Wait() (err error) {
	// Calling cond_var.Wait() after cond_var.Broadcast() has been called results into a panic
	// When the throttler is stopped, it closes the fin channel first before it cond_var.Broadcast().
	// So, checking for close() here prevents the panic
	if throttler.isClosed() {
		err = fmt.Errorf("throttler is closed")
		return
	}
	throttler.cond_var.L.Lock()
	defer throttler.cond_var.L.Unlock()
	throttler.cond_var.Wait()

	return
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

	var ok bool
	var overall_bandwidth_limit int
	update_overall_bandwidth_limit := false
	var number_of_source_nodes int
	update_number_of_source_nodes := false

	overall_bandwidth_limit_obj := settings[OVERALL_BANDWIDTH_LIMIT]
	if overall_bandwidth_limit_obj != nil {
		update_overall_bandwidth_limit = true
		overall_bandwidth_limit, ok = overall_bandwidth_limit_obj.(int)
		if !ok {
			return fmt.Errorf("%v overall bandwidth limit, %v, has invalid type.", throttler.id, overall_bandwidth_limit_obj)
		}
	}

	number_of_source_nodes_obj := settings[NUMBER_OF_SOURCE_NODES]
	if number_of_source_nodes_obj != nil {
		update_number_of_source_nodes = true
		number_of_source_nodes, ok = number_of_source_nodes_obj.(int)
		if !ok {
			return fmt.Errorf("%v number of source nodes, %v, has invalid type.", throttler.id, number_of_source_nodes_obj)
		}
	}

	if update_overall_bandwidth_limit || update_number_of_source_nodes {
		// update only when necessary
		return throttler.updateBandwidthLimitSettings(number_of_source_nodes, update_number_of_source_nodes,
			overall_bandwidth_limit, update_overall_bandwidth_limit)
	}

	return nil
}

// update bandwidth limit related settings
// note: the two bool inputs indicate whether the corresponding setting needs to be updated
//
//	if a bool takes the value of false, the corresponding input setting is ignored
func (throttler *BandwidthThrottler) updateBandwidthLimitSettings(number_of_source_nodes int,
	update_number_of_source_nodes bool, overall_bandwidth_limit int, update_overall_bandwidth_limit bool) error {

	// use the lock to prevent concurrent and interleaved updates to bandwith limit settings
	// without the lock, the following scenario is possible:
	// 1. go routine 1 updates number_of_source_nodes to n1
	// 2. go routine 2 updates number_of_source_nodes to n2
	// 3. go routine 2 updates overall_bandwidth_limit to o2
	// 4. go routine 1 updates overall_bandwidth_limit to o1
	// 5. the end result of bandwidth_limit is o1/n2, which does not make sense
	throttler.bandwidth_limit_lock.Lock()
	defer throttler.bandwidth_limit_lock.Unlock()

	var err error
	var number_of_source_nodes_after_update uint32
	var overall_bandwidth_limit_after_update int64
	number_of_source_nodes_updated := false
	overall_bandwidth_limit_updated := false

	if update_number_of_source_nodes {
		err = throttler.validateNumberOfSourceNodes(number_of_source_nodes)
		if err != nil {
			return err
		}

		number_of_source_nodes_after_update = uint32(number_of_source_nodes)
		number_of_source_nodes_original := atomic.LoadUint32(&throttler.number_of_source_nodes)
		if number_of_source_nodes_original != number_of_source_nodes_after_update {
			atomic.StoreUint32(&throttler.number_of_source_nodes, number_of_source_nodes_after_update)
			number_of_source_nodes_updated = true
			throttler.logger.Infof("%v updated number of source nodes from %v to %v\n", throttler.id, number_of_source_nodes_original, number_of_source_nodes_after_update)
		} else {
			throttler.logger.Infof("%v skipped update of number of source nodes because it already has the value of %v\n", throttler.id, number_of_source_nodes_original)
		}
	} else {
		number_of_source_nodes_after_update = atomic.LoadUint32(&throttler.number_of_source_nodes)
	}

	if update_overall_bandwidth_limit {
		err = throttler.validateOverallBandwidthLimit(overall_bandwidth_limit)
		if err != nil {
			return err
		}

		overall_bandwidth_limit_after_update = int64(overall_bandwidth_limit)
		overall_bandwidth_limit_original := atomic.LoadInt64(&throttler.overall_bandwidth_limit)
		if overall_bandwidth_limit_original != overall_bandwidth_limit_after_update {
			atomic.StoreInt64(&throttler.overall_bandwidth_limit, overall_bandwidth_limit_after_update)
			overall_bandwidth_limit_updated = true
			throttler.logger.Infof("%v updated overall bandwidth limit from %v to %v\n", throttler.id, overall_bandwidth_limit_original, overall_bandwidth_limit_after_update)
		} else {
			throttler.logger.Infof("%v skipped update of overall bandwidth limit because it already has the value of %v\n", throttler.id, overall_bandwidth_limit_original)
		}
	} else {
		overall_bandwidth_limit_after_update = atomic.LoadInt64(&throttler.overall_bandwidth_limit)
	}

	if number_of_source_nodes_updated || overall_bandwidth_limit_updated {
		bandwidth_limit_original := atomic.LoadInt64(&throttler.bandwidth_limit)
		bandwidth_limit_after_update := overall_bandwidth_limit_after_update * 1024 * 1024 / int64(number_of_source_nodes_after_update)
		if bandwidth_limit_original != bandwidth_limit_after_update {
			atomic.StoreInt64(&throttler.bandwidth_limit, bandwidth_limit_after_update)
			throttler.logger.Infof("%v updated bandwidth limit from %v to %v\n", throttler.id, bandwidth_limit_original, bandwidth_limit_after_update)
			throttler.adjustBandwidthUsageQuota(bandwidth_limit_after_update)
		} else {
			throttler.logger.Infof("%v skipped update of bandwidth limit because it already has the value of %v\n", throttler.id, bandwidth_limit_original)
		}
	} else {
		throttler.logger.Infof("%v skipped update of bandwidth limit because there have been no real changes.\n", throttler.id)
	}
	return nil
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

func (throttler *BandwidthThrottler) validateNumberOfSourceNodes(number_of_source_nodes int) error {
	if number_of_source_nodes == 0 {
		return fmt.Errorf("%v number of source nodes is 0", throttler.id)
	} else if number_of_source_nodes < 0 {
		return fmt.Errorf("%v number of source nodes is negative, %v", throttler.id, number_of_source_nodes)
	}
	return nil
}

func (throttler *BandwidthThrottler) validateOverallBandwidthLimit(overall_bandwidth_limit int) error {
	if overall_bandwidth_limit < 0 {
		return fmt.Errorf("%v overall bandwidth limit is negative, %v", throttler.id, overall_bandwidth_limit)
	}
	return nil
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

func (throttler *BandwidthThrottler) IsSharable() bool {
	// Replications can specify max bandwidth to use - and the said bandwidth
	// should be shared among multiple pipelines within the same replication
	return true
}

func (throttler *BandwidthThrottler) Detach(pipeline common.Pipeline) error {
	throttler.attachmentMtx.Lock()
	defer throttler.attachmentMtx.Unlock()

	var idxToDel int = -1

	for i, attachedP := range throttler.pipelines {
		if pipeline.FullTopic() == attachedP.FullTopic() {
			throttler.logger.Infof("Detaching %v %v", attachedP.Type(), attachedP.Topic())
			idxToDel = i
			break
		}
	}

	if idxToDel == -1 {
		return base.ErrorNotFound
	}

	throttler.pipelines = append(throttler.pipelines[:idxToDel], throttler.pipelines[idxToDel+1:]...)
	return nil
}
