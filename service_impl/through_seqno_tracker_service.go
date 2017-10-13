// Copyright (c) 2013 Couchbase, Inc.
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
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	pipeline_pkg "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/simple_utils"
	"sync"
	"time"
)

type ThroughSeqnoTrackerSvc struct {
	// map of vbs that the tracker tracks.
	vb_map map[uint16]bool

	// through_seqno seen by outnozzles based on the docs that are actually sent to target
	// This is a map that is considered the "baseline" of sequence numbers. Whenever a batch operation
	// that figures out timestamps, it updates this map. And whenever a caller asks this service for
	// the "highest" seqno that is considered synchronized between source and target, the tracker service
	// starts with the number listed in this map and builds on top. (See GetThroughSeqnos)
	through_seqno_map map[uint16]*base.SeqnoWithLock

	// stores for each vb a sorted list of the seqnos that have been sent to and confirmed by target
	vb_sent_seqno_list_map map[uint16]*SortedSeqnoListWithLock

	// Note: lists in the following two maps are treated in the same way in through_seqno computation
	// they are maintained as two seperate lists because insertions into the lists are simpler
	// and quicker this way - each insertion is simply an append to the end of the list

	// stores for each vb a sorted list of seqnos that have been filtered out
	vb_filtered_seqno_list_map map[uint16]*SortedSeqnoListWithLock
	// stores for each vb a sorted list of seqnos that have failed conflict resolution on source
	vb_failed_cr_seqno_list_map map[uint16]*SortedSeqnoListWithLock

	// gap_seqno_list_1[i] stores the start seqno of the ith gap range
	// gap_seqno_list_2[i] stores the end seqno of  the ith gap range
	// for example, if we receive seqno 5 and then 10 from dcp, the gap range is [6,9]
	// 6 will be appended to the end of gap_seqno_list_1, and 10 appended to gap_seqno_list_2
	// for a given vb, gap_seqno_list_1 and gap_seqno_list_2 always have the same length
	vb_gap_seqno_list_map map[uint16]*DualSortedSeqnoListWithLock

	// tracks the last seen seqno streamed out by dcp, so that we can tell the gap between the last seen seqno
	// and the current seen seqno
	vb_last_seen_seqno_map map[uint16]*base.SeqnoWithLock

	id     string
	rep_id string

	logger *log.CommonLogger
}

type SortedSeqnoListWithLock struct {
	seqno_list []uint64
	lock       *sync.RWMutex
}

func newSortedSeqnoListWithLock() *SortedSeqnoListWithLock {
	return &SortedSeqnoListWithLock{make([]uint64, 0), &sync.RWMutex{}}
}

// when needToSort is true, sort the internal seqno_list before returning it
// sorting is needed only when seqno_list is not already sorted, which is the case only for sent_seqno_list
// in other words, needToSort should be set to true only when operating on sent_seqno_list
func (list_obj *SortedSeqnoListWithLock) getSortedSeqnoList(needToSort bool) []uint64 {
	if needToSort {
		list_obj.lock.Lock()
		defer list_obj.lock.Unlock()

		simple_utils.SortUint64List(list_obj.seqno_list)
		return simple_utils.DeepCopyUint64Array(list_obj.seqno_list)
	} else {
		list_obj.lock.RLock()
		defer list_obj.lock.RUnlock()

		return simple_utils.DeepCopyUint64Array(list_obj.seqno_list)
	}
}

func (list_obj *SortedSeqnoListWithLock) getLengthOfSeqnoList() int {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	return len(list_obj.seqno_list)
}

// append seqno to the end of seqno_list
func (list_obj *SortedSeqnoListWithLock) appendSeqno(seqno uint64, logger *log.CommonLogger) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list = append(list_obj.seqno_list, seqno)
}

// truncate all seqnos that are no larger than passed in through_seqno
func (list_obj *SortedSeqnoListWithLock) truncateSeqnos(through_seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	seqno_list := list_obj.seqno_list
	index, found := simple_utils.SearchUint64List(seqno_list, through_seqno)
	if found {
		list_obj.seqno_list = seqno_list[index+1:]
	} else if index > 0 {
		list_obj.seqno_list = seqno_list[index:]
	}
}

// struct containing two seqno lists that need to be accessed and locked together
type DualSortedSeqnoListWithLock struct {
	seqno_list_1 []uint64
	seqno_list_2 []uint64
	lock         *sync.RWMutex
}

func newDualSortedSeqnoListWithLock() *DualSortedSeqnoListWithLock {
	return &DualSortedSeqnoListWithLock{make([]uint64, 0), make([]uint64, 0), &sync.RWMutex{}}
}

func (list_obj *DualSortedSeqnoListWithLock) getSortedSeqnoLists() ([]uint64, []uint64) {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	if len(list_obj.seqno_list_1) != len(list_obj.seqno_list_2) {
		panic(fmt.Sprintf("lengths of gap_seqno_lists do not match. gap_seqno_list_1=%v, gap_seqno_list_2=%v", list_obj.seqno_list_1, list_obj.seqno_list_2))
	}

	return simple_utils.DeepCopyUint64Array(list_obj.seqno_list_1), simple_utils.DeepCopyUint64Array(list_obj.seqno_list_2)
}

func (list_obj *DualSortedSeqnoListWithLock) getLengthOfSeqnoLists() int {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	return len(list_obj.seqno_list_1)
}

// append seqnos to the end of seqno_lists
func (list_obj *DualSortedSeqnoListWithLock) appendSeqnos(seqno_1 uint64, seqno_2 uint64, logger *log.CommonLogger) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list_1 = append(list_obj.seqno_list_1, seqno_1)
	list_obj.seqno_list_2 = append(list_obj.seqno_list_2, seqno_2)
}

// truncate all seqnos that are no larger than passed in through_seqno
func (list_obj *DualSortedSeqnoListWithLock) truncateSeqnos(through_seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()

	list_obj.seqno_list_1 = truncateGapSeqnoList(through_seqno, list_obj.seqno_list_1)
	list_obj.seqno_list_2 = truncateGapSeqnoList(through_seqno, list_obj.seqno_list_2)
}

func truncateGapSeqnoList(through_seqno uint64, seqno_list []uint64) []uint64 {
	index, found := simple_utils.SearchUint64List(seqno_list, through_seqno)
	if found {
		panic("through_seqno cannot be in gap_seqno_list")
	} else if index > 0 {
		return seqno_list[index:]
	}

	return seqno_list
}

func NewThroughSeqnoTrackerSvc(logger_ctx *log.LoggerContext) *ThroughSeqnoTrackerSvc {
	logger := log.NewLogger("ThrSeqTrackSvc", logger_ctx)
	tsTracker := &ThroughSeqnoTrackerSvc{
		logger:                      logger,
		vb_map:                      make(map[uint16]bool),
		through_seqno_map:           make(map[uint16]*base.SeqnoWithLock),
		vb_last_seen_seqno_map:      make(map[uint16]*base.SeqnoWithLock),
		vb_sent_seqno_list_map:      make(map[uint16]*SortedSeqnoListWithLock),
		vb_filtered_seqno_list_map:  make(map[uint16]*SortedSeqnoListWithLock),
		vb_failed_cr_seqno_list_map: make(map[uint16]*SortedSeqnoListWithLock),
		vb_gap_seqno_list_map:       make(map[uint16]*DualSortedSeqnoListWithLock),
	}
	return tsTracker
}

func (tsTracker *ThroughSeqnoTrackerSvc) initialize(pipeline common.Pipeline) {
	tsTracker.rep_id = pipeline.Topic()
	tsTracker.id = pipeline.Topic() + "_" + base.ThroughSeqnoTracker
	for _, vbno := range pipeline_utils.GetSourceVBListPerPipeline(pipeline) {
		tsTracker.vb_map[vbno] = true

		tsTracker.through_seqno_map[vbno] = base.NewSeqnoWithLock()
		tsTracker.vb_last_seen_seqno_map[vbno] = base.NewSeqnoWithLock()

		tsTracker.vb_sent_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_filtered_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_failed_cr_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_gap_seqno_list_map[vbno] = newDualSortedSeqnoListWithLock()
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) Attach(pipeline common.Pipeline) error {
	tsTracker.logger.Infof("Attach through seqno tracker with pipeline %v\n", pipeline.InstanceId())

	tsTracker.initialize(pipeline)

	asyncListenerMap := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)

	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataSentEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataFailedCREventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataFilteredEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataReceivedEventListener, tsTracker)
	return nil
}

func (tsTracker *ThroughSeqnoTrackerSvc) ProcessEvent(event *common.Event) error {
	if !tsTracker.isPipelineRunning() {
		tsTracker.logger.Tracef("Pipeline %s is no longer running, skip ProcessEvent for %v\n", tsTracker.rep_id, event)
	}

	if event.EventType == common.DataSent {
		vbno := event.OtherInfos.(parts.DataSentEventAdditional).VBucket
		seqno := event.OtherInfos.(parts.DataSentEventAdditional).Seqno
		tsTracker.addSentSeqno(vbno, seqno)
	} else if event.EventType == common.DataFiltered {
		upr_event := event.Data.(*mcc.UprEvent)
		seqno := upr_event.Seqno
		vbno := upr_event.VBucket
		tsTracker.addFilteredSeqno(vbno, seqno)
	} else if event.EventType == common.DataFailedCRSource {
		seqno := event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).Seqno
		vbno := event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).VBucket
		tsTracker.addFailedCRSeqno(vbno, seqno)
	} else if event.EventType == common.DataReceived {
		upr_event := event.Data.(*mcc.UprEvent)
		seqno := upr_event.Seqno
		vbno := upr_event.VBucket
		// Sets last sequence number, and should the sequence number skip due to gap, this will take care of it
		tsTracker.processGapSeqnos(vbno, seqno)
	} else {
		panic(fmt.Sprintf("Incorrect event type, %v, received by %v", event.EventType, tsTracker.id))
	}

	return nil

}

func (tsTracker *ThroughSeqnoTrackerSvc) addSentSeqno(vbno uint16, sent_seqno uint64) {
	tsTracker.validateVbno(vbno, "addSentSeqno")
	tsTracker.vb_sent_seqno_list_map[vbno].appendSeqno(sent_seqno, tsTracker.logger)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFilteredSeqno(vbno uint16, filtered_seqno uint64) {
	tsTracker.validateVbno(vbno, "addFilteredSeqno")
	tsTracker.vb_filtered_seqno_list_map[vbno].appendSeqno(filtered_seqno, tsTracker.logger)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFailedCRSeqno(vbno uint16, failed_cr_seqno uint64) {
	tsTracker.validateVbno(vbno, "addFailedCRSeqno")

	tsTracker.vb_failed_cr_seqno_list_map[vbno].appendSeqno(failed_cr_seqno, tsTracker.logger)
}

func (tsTracker *ThroughSeqnoTrackerSvc) processGapSeqnos(vbno uint16, current_seqno uint64) {
	tsTracker.validateVbno(vbno, "processGapSeqnos")

	last_seen_seqno_obj := tsTracker.vb_last_seen_seqno_map[vbno]

	last_seen_seqno_obj.Lock()
	defer last_seen_seqno_obj.Unlock()
	last_seen_seqno := last_seen_seqno_obj.GetSeqnoWithoutLock()
	if last_seen_seqno == 0 {
		// this covers the case where the replication resumes from checkpoint docs
		last_seen_seqno = tsTracker.through_seqno_map[vbno].GetSeqno()
	}
	last_seen_seqno_obj.SetSeqnoWithoutLock(current_seqno)

	if last_seen_seqno < current_seqno-1 {
		// If the current sequence number is not consecutive, then this means we have hit a gap. Store it in gap list.
		tsTracker.vb_gap_seqno_list_map[vbno].appendSeqnos(last_seen_seqno+1, current_seqno-1, tsTracker.logger)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) truncateSeqnoLists(vbno uint16, through_seqno uint64) {
	tsTracker.vb_sent_seqno_list_map[vbno].truncateSeqnos(through_seqno)
	tsTracker.vb_filtered_seqno_list_map[vbno].truncateSeqnos(through_seqno)
	tsTracker.vb_failed_cr_seqno_list_map[vbno].truncateSeqnos(through_seqno)
	tsTracker.vb_gap_seqno_list_map[vbno].truncateSeqnos(through_seqno)
}

/**
 * It's possible that even though the last sequence number actually sent is N, the next few numbers
 * are actually handled and decided not to be sent. For example, N+1 may have been decided to be filtered
 * out, and N+2 has failed CR, and N+3 to N+5 has been deduplicated as part of the DCP snapshot and has
 * been accounted for in the gap seq number dual-list. In this example, the through seqno would be N+5.
 *
 * As a service, its job is to determine the "highest" number, and it could be either
 * actually sent, or filtered, or some other "evented", such that any transaction below this number is
 * considered "synchronized" between the source and destination.
 *
 * This method, given a vbucket number, returns that number.
 *
 * Note that the method is called "GetThroughSeqno" but also note that a through seqno may not necessarily
 * mean that a mutation is physically sent over to the target. Do not confuse this with through_seqno_map,
 * which only keeps track of physically sent numbers.
 */
func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqno(vbno uint16) uint64 {
	tsTracker.validateVbno(vbno, "GetThroughSeqno")

	// lock through_seqno_map[vbno] throughout the computation to ensure that
	// two GetThroughSeqno() routines won't interleave, which would cause issues
	// since we truncate seqno lists in accordance with through_seqno
	through_seqno_obj := tsTracker.through_seqno_map[vbno]
	through_seqno_obj.Lock()
	defer through_seqno_obj.Unlock()

	last_through_seqno := through_seqno_obj.GetSeqnoWithoutLock()
	sent_seqno_list := tsTracker.vb_sent_seqno_list_map[vbno].getSortedSeqnoList(true)
	max_sent_seqno := maxSeqno(sent_seqno_list)
	filtered_seqno_list := tsTracker.vb_filtered_seqno_list_map[vbno].getSortedSeqnoList(false)
	max_filtered_seqno := maxSeqno(filtered_seqno_list)
	failed_cr_seqno_list := tsTracker.vb_failed_cr_seqno_list_map[vbno].getSortedSeqnoList(false)
	max_failed_cr_seqno := maxSeqno(failed_cr_seqno_list)
	gap_seqno_list_1, gap_seqno_list_2 := tsTracker.vb_gap_seqno_list_map[vbno].getSortedSeqnoLists()
	max_end_gap_seqno := maxSeqno(gap_seqno_list_2)

	// Goal of algorithm:
	// Find the right through_seqno for stats and checkpointing, with the constraint that through_seqno cannot be
	// a gap seqno, since we do not want to use gap seqnos for checkpointing

	// Starting from last_through_seqno, find the largest N such that last_through_seqno+1, last_through_seqno+2,
	// .., last_through_seqno+N all exist in filtered_seqno_list, failed_cr_seqno_list, sent_seqno_list, or a gap range,
	// and that last_through_seqno+N itself is not in a gap range
	// return last_through_seqno+N as the current through_seqno. Note that N could be 0.

	through_seqno := last_through_seqno

	iter_seqno := last_through_seqno
	var last_sent_index int = -1
	var last_filtered_index int = -1
	var last_failed_cr_index int = -1
	var found_seqno_type int = -1

	const (
		SeqnoTypeSent     int = 1
		SeqnoTypeFiltered int = 2
		SeqnoTypeFailedCR int = 3
	)

	for {
		iter_seqno++
		if iter_seqno <= max_sent_seqno {
			sent_index, sent_found := simple_utils.SearchUint64List(sent_seqno_list, iter_seqno)
			if sent_found {
				last_sent_index = sent_index
				found_seqno_type = SeqnoTypeSent
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_filtered_seqno {
			filtered_index, filtered_found := simple_utils.SearchUint64List(filtered_seqno_list, iter_seqno)
			if filtered_found {
				last_filtered_index = filtered_index
				found_seqno_type = SeqnoTypeFiltered
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_failed_cr_seqno {
			failed_cr_index, failed_cr_found := simple_utils.SearchUint64List(failed_cr_seqno_list, iter_seqno)
			if failed_cr_found {
				last_failed_cr_index = failed_cr_index
				found_seqno_type = SeqnoTypeFailedCR
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_end_gap_seqno {
			gap_found, end_gap_seqno := isSeqnoGapSeqno(gap_seqno_list_1, gap_seqno_list_2, iter_seqno)
			if gap_found {
				// if iter_seqno is in a gap range, skip to the end of the gap range
				iter_seqno = end_gap_seqno
				continue
			}
		}

		// stop if cannot find seqno in any of the lists
		break
	}

	if last_sent_index >= 0 || last_filtered_index >= 0 || last_failed_cr_index >= 0 {
		if found_seqno_type == SeqnoTypeSent {
			through_seqno = sent_seqno_list[last_sent_index]
		} else if found_seqno_type == SeqnoTypeFiltered {
			through_seqno = filtered_seqno_list[last_filtered_index]
		} else if found_seqno_type == SeqnoTypeFailedCR {
			through_seqno = failed_cr_seqno_list[last_failed_cr_index]
		} else {
			panic(fmt.Sprintf("unexpected found_seqno_type, %v", found_seqno_type))
		}

		through_seqno_obj.SetSeqnoWithoutLock(through_seqno)

		// truncate no longer needed entries from seqno lists to reduce memory/cpu overhead for future computations
		go tsTracker.truncateSeqnoLists(vbno, through_seqno)
	}
	return through_seqno
}

// if seqno is a gap seqno, return (true, seqno of end of gap range)
// otherwise, return (false, 0)
func isSeqnoGapSeqno(gap_seqno_list_1, gap_seqno_list_2 []uint64, seqno uint64) (bool, uint64) {
	if len(gap_seqno_list_1) == 0 {
		return false, 0
	}
	index, is_start_gap_seqno := simple_utils.SearchUint64List(gap_seqno_list_1, seqno)
	if is_start_gap_seqno {
		return true, gap_seqno_list_2[index]
	}

	// gap_range_index is the index of the largest start_gap_seqno that is smaller than seqno
	gap_range_index := index - 1
	if gap_range_index < 0 {
		return false, 0
	}

	if gap_seqno_list_2[gap_range_index] >= seqno {
		// seqno is between gap_seqno_list_1[gap_range_index] and gap_seqno_list_2[gap_range_index]
		// and hence is a gap seqno
		return true, gap_seqno_list_2[gap_range_index]
	}

	return false, 0

}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqnos() map[uint16]uint64 {
	start_time := time.Now()
	defer func() {
		time_taken := time.Since(start_time)
		if time_taken > base.ThresholdForThroughSeqnoComputation {
			tsTracker.logger.Warnf("%v GetThroughSeqnos completed after %v\n", tsTracker.id, time_taken)
		}
	}()

	result_map := make(map[uint16]uint64)

	listOfVbs := tsTracker.getVbList()
	vb_per_worker := 20
	start_index := 0

	wait_grp := &sync.WaitGroup{}
	executor_id := 0
	result_map_map := make(map[int]map[uint16]uint64)
	for {
		end_index := start_index + vb_per_worker
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_executor := listOfVbs[start_index:end_index]
		result_map_map[executor_id] = make(map[uint16]uint64)
		wait_grp.Add(1)
		go tsTracker.getThroughSeqnos(executor_id, vbs_for_executor, result_map_map[executor_id], wait_grp)
		start_index = end_index
		executor_id++
		if start_index >= len(listOfVbs) {
			break
		}
	}

	wait_grp.Wait()

	for _, exec_result_map := range result_map_map {
		for vbno, seqno := range exec_result_map {
			result_map[vbno] = seqno
		}
	}

	return result_map
}

func (tsTracker *ThroughSeqnoTrackerSvc) getThroughSeqnos(executor_id int, listOfVbs []uint16, result_map map[uint16]uint64, wait_grp *sync.WaitGroup) {
	if result_map == nil {
		panic("through_seqno_map is nil")
	}
	tsTracker.logger.Tracef("%v getThroughSeqnos executor %v is working on vbuckets %v", tsTracker.id, executor_id, listOfVbs)
	if wait_grp == nil {
		panic("wait_grp can't be nil")
	}
	defer wait_grp.Done()

	for _, vbno := range listOfVbs {
		result_map[vbno] = tsTracker.GetThroughSeqno(vbno)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) SetStartSeqno(vbno uint16, seqno uint64) {
	tsTracker.validateVbno(vbno, "setStartSeqno")
	obj, _ := tsTracker.through_seqno_map[vbno]
	obj.SetSeqno(seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) validateVbno(vbno uint16, caller string) {
	if _, ok := tsTracker.vb_map[vbno]; !ok {
		panic(fmt.Sprintf("method %v in tracker service for pipeline %v received invalid vbno. vbno=%v; valid vbnos=%v",
			caller, tsTracker.id, vbno, tsTracker.vb_map))
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) getVbList() []uint16 {
	vb_list := make([]uint16, 0)
	for vbno, _ := range tsTracker.vb_map {
		vb_list = append(vb_list, vbno)
	}
	return vb_list
}

func (tsTracker *ThroughSeqnoTrackerSvc) Id() string {
	return tsTracker.id
}

func (tsTracker *ThroughSeqnoTrackerSvc) isPipelineRunning() bool {
	rep_status, _ := pipeline_manager.ReplicationStatus(tsTracker.rep_id)
	if rep_status != nil {
		pipeline := rep_status.Pipeline()
		if pipeline != nil && pipeline_utils.IsPipelineRunning(pipeline.State()) {
			return true
		}
	}
	return false
}

func maxSeqno(seqno_list []uint64) uint64 {
	length := len(seqno_list)
	if length > 0 {
		return seqno_list[length-1]
	} else {
		return 0
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) PrintStatusSummary() {
	start_time := time.Now()
	var count, sum_sent, max_sent, sum_filtered, max_filtered, sum_failed_cr, max_failed_cr, sum_gap, max_gap int
	for _, vbno := range tsTracker.getVbList() {
		length_sent_seqno_list := tsTracker.vb_sent_seqno_list_map[vbno].getLengthOfSeqnoList()
		length_filtered_seqno_list := tsTracker.vb_filtered_seqno_list_map[vbno].getLengthOfSeqnoList()
		length_failed_cr_seqno_list := tsTracker.vb_failed_cr_seqno_list_map[vbno].getLengthOfSeqnoList()
		length_gap_seqno_list := tsTracker.vb_gap_seqno_list_map[vbno].getLengthOfSeqnoLists()

		count++
		sum_sent += length_sent_seqno_list
		if max_sent < length_sent_seqno_list {
			max_sent = length_sent_seqno_list
		}
		sum_filtered += length_filtered_seqno_list
		if max_filtered < length_filtered_seqno_list {
			max_filtered = length_filtered_seqno_list
		}
		sum_failed_cr += length_failed_cr_seqno_list
		if max_failed_cr < length_failed_cr_seqno_list {
			max_failed_cr = length_failed_cr_seqno_list
		}
		sum_gap += length_gap_seqno_list
		if max_gap < length_gap_seqno_list {
			max_gap = length_gap_seqno_list
		}
	}

	tsTracker.logger.Infof("%v time_spent=%v num_vb=%v max_sent=%v avg_sent=%v max_filtered=%v avg_filtered=%v max_failed_cr=%v avg_failed_cr=%v max_gap=%v avg_gap=%v\n",
		tsTracker.id, time.Since(start_time), count, max_sent, sum_sent/count, max_filtered, sum_filtered/count, max_failed_cr, sum_failed_cr/count,
		max_gap, sum_gap/count)
}
