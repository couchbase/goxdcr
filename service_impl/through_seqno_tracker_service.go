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
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/simple_utils"
	"sort"
	"sync"
)

type ThroughSeqnoTrackerSvc struct {
	// map of vbs that the tracker tracks.
	vb_map map[uint16]bool

	// through_seqno seen by outnozzles based on the docs that are actually sent to target
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

	// stores for each vb a sorted list of gap seqnos that have not been streamed out by dcp
	vb_gap_seqno_list_map map[uint16]*SortedSeqnoListWithLock

	// tracks the last seen seqno streamed out by dcp, so that we can tell the gap between the last seen seqno
	// and the current seen seqno
	vb_last_seen_seqno_map map[uint16]*base.SeqnoWithLock

	id string

	logger *log.CommonLogger
}

type SortedSeqnoListWithLock struct {
	seqno_list []int
	lock       *sync.RWMutex
}

func newSortedSeqnoListWithLock() *SortedSeqnoListWithLock {
	return &SortedSeqnoListWithLock{make([]int, 0), &sync.RWMutex{}}
}

// when needToSort is true, sort the internal seqno_list before returning it
// sorting is needed only when seqno_list is not already sorted, which is the case only for sent_seqno_list
// in other words, needToSort should be set to true only when operating on sent_seqno_list
func (list_obj *SortedSeqnoListWithLock) getSortedSeqnoList(needToSort bool) []int {
	if needToSort {
		list_obj.lock.Lock()
		defer list_obj.lock.Unlock()

		sort.Ints(list_obj.seqno_list)
		return simple_utils.DeepCopyIntArray(list_obj.seqno_list)
	} else {
		list_obj.lock.RLock()
		defer list_obj.lock.RUnlock()

		return simple_utils.DeepCopyIntArray(list_obj.seqno_list)
	}
}

// append seqno to the end of seqno_list
func (list_obj *SortedSeqnoListWithLock) appendSeqno(seqno uint64, logger *log.CommonLogger) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list = append(list_obj.seqno_list, int(seqno))
	logger.Debugf("after adding seqno %v, seqno_list is %v\n", seqno, list_obj.seqno_list)
}

func (list_obj *SortedSeqnoListWithLock) appendSeqnos(seqno_list []uint64, logger *log.CommonLogger) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	for _, seqno := range seqno_list {
		list_obj.seqno_list = append(list_obj.seqno_list, int(seqno))
	}
	logger.Debugf("after adding seqno list %v, new seqno list is %v\n", seqno_list, list_obj.seqno_list)
}

// truncate all seqnos that are no larger than passed in through_seqno
func (list_obj *SortedSeqnoListWithLock) truncateSeqnos(through_seqno uint64, validateSeqnoNotInList bool) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	seqno_list := list_obj.seqno_list
	index, found := search(seqno_list, through_seqno)
	if found {
		if validateSeqnoNotInList {
			// this applies to gap_seqno_list only
			panic("through_seqno cannot be in seqno_list")
		} else {
			list_obj.seqno_list = seqno_list[index+1:]
		}
	} else if index > 0 {
		list_obj.seqno_list = seqno_list[index:]
	}
}

func NewThroughSeqnoTrackerSvc(logger_ctx *log.LoggerContext) *ThroughSeqnoTrackerSvc {
	logger := log.NewLogger("ThroughSeqnoTrackerSvc", logger_ctx)
	tsTracker := &ThroughSeqnoTrackerSvc{
		logger:                      logger,
		vb_map:                      make(map[uint16]bool),
		through_seqno_map:           make(map[uint16]*base.SeqnoWithLock),
		vb_last_seen_seqno_map:      make(map[uint16]*base.SeqnoWithLock),
		vb_sent_seqno_list_map:      make(map[uint16]*SortedSeqnoListWithLock),
		vb_filtered_seqno_list_map:  make(map[uint16]*SortedSeqnoListWithLock),
		vb_failed_cr_seqno_list_map: make(map[uint16]*SortedSeqnoListWithLock),
		vb_gap_seqno_list_map:       make(map[uint16]*SortedSeqnoListWithLock),
	}
	return tsTracker
}

func (tsTracker *ThroughSeqnoTrackerSvc) initialize(pipeline common.Pipeline) {
	tsTracker.id = pipeline.Topic() + "_" + base.ThroughSeqnoTracker
	for _, vbno := range pipeline_utils.GetSourceVBListPerPipeline(pipeline) {
		tsTracker.vb_map[vbno] = true

		tsTracker.through_seqno_map[vbno] = base.NewSeqnoWithLock()
		tsTracker.vb_last_seen_seqno_map[vbno] = base.NewSeqnoWithLock()

		tsTracker.vb_sent_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_filtered_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_failed_cr_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
		tsTracker.vb_gap_seqno_list_map[vbno] = newSortedSeqnoListWithLock()
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
		tsTracker.processGapSeqnos(vbno, seqno)
	} else {
		panic(fmt.Sprintf("Incorrect event type, %v, received by %v", event.EventType, tsTracker.id))
	}

	return nil

}

func (tsTracker *ThroughSeqnoTrackerSvc) addSentSeqno(vbno uint16, sent_seqno uint64) {
	tsTracker.validateVbno(vbno, "addSentSeqno")
	tsTracker.logger.Debugf("%v adding sent seqno %v for vb %v.\n", tsTracker.id, sent_seqno, vbno)
	tsTracker.vb_sent_seqno_list_map[vbno].appendSeqno(sent_seqno, tsTracker.logger)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFilteredSeqno(vbno uint16, filtered_seqno uint64) {
	tsTracker.validateVbno(vbno, "addFilteredSeqno")
	tsTracker.logger.Debugf("%v adding filtered seqno %v for vb %v.", tsTracker.id, filtered_seqno, vbno)
	tsTracker.vb_filtered_seqno_list_map[vbno].appendSeqno(filtered_seqno, tsTracker.logger)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFailedCRSeqno(vbno uint16, failed_cr_seqno uint64) {
	tsTracker.validateVbno(vbno, "addFailedCRSeqno")

	tsTracker.logger.Debugf("%v adding failed cr seqno %v for vb %v.", tsTracker.id, failed_cr_seqno, vbno)
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

	tsTracker.logger.Debugf("%v processing gap seqnos for seqno %v for vbno %v. last_seen_seqno=%v\n", tsTracker.id, current_seqno, vbno, last_seen_seqno)

	if last_seen_seqno < current_seqno-1 {
		gap_seqno_list := make([]uint64, 0)
		for i := last_seen_seqno + 1; i < current_seqno; i++ {
			gap_seqno_list = append(gap_seqno_list, i)
		}

		tsTracker.vb_gap_seqno_list_map[vbno].appendSeqnos(gap_seqno_list, tsTracker.logger)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) truncateSeqnoLists(vbno uint16, through_seqno uint64) {
	tsTracker.vb_sent_seqno_list_map[vbno].truncateSeqnos(through_seqno, false)
	tsTracker.vb_filtered_seqno_list_map[vbno].truncateSeqnos(through_seqno, false)
	tsTracker.vb_failed_cr_seqno_list_map[vbno].truncateSeqnos(through_seqno, false)
	tsTracker.vb_gap_seqno_list_map[vbno].truncateSeqnos(through_seqno, true)
}

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
	gap_seqno_list := tsTracker.vb_gap_seqno_list_map[vbno].getSortedSeqnoList(false)
	max_gap_seqno := maxSeqno(gap_seqno_list)

	tsTracker.logger.Debugf("%v, vbno=%v, last_through_seqno=%v\n sent_seqno_list=%v\n filtered_seqno_list=%v\n failed_cr_seqno_list=%v\n gap_seqno_list=%v\n", tsTracker.id, vbno, last_through_seqno, sent_seqno_list, filtered_seqno_list, failed_cr_seqno_list, gap_seqno_list)

	// Goal of algorithm:
	// Find the right through_seqno for stats and checkpointing, with the constraint that through_seqno cannot be
	// in gap_seqno_list, since we do not want to use seqnos in gap_seqno_list for checkpointing

	// Starting from last_through_seqno, find the largest N such that last_through_seqno+1, last_through_seqno+2,
	// .., last_through_seqno+N all exist in filtered_seqno_list, failed_cr_seqno_list, sent_seqno_list, or gap_seqno_list,
	// and that last_through_seqno+N is not in gap_seqno_list
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
		iter_seqno = iter_seqno + 1
		if iter_seqno <= max_sent_seqno {
			sent_index, sent_found := search(sent_seqno_list, iter_seqno)
			if sent_found {
				last_sent_index = sent_index
				found_seqno_type = SeqnoTypeSent
				continue
			}
		}

		if iter_seqno <= max_filtered_seqno {
			filtered_index, filtered_found := search(filtered_seqno_list, iter_seqno)
			if filtered_found {
				last_filtered_index = filtered_index
				found_seqno_type = SeqnoTypeFiltered
				continue
			}
		}

		if iter_seqno <= max_failed_cr_seqno {
			failed_cr_index, failed_cr_found := search(failed_cr_seqno_list, iter_seqno)
			if failed_cr_found {
				last_failed_cr_index = failed_cr_index
				found_seqno_type = SeqnoTypeFailedCR
				continue
			}
		}

		if iter_seqno <= max_gap_seqno {
			_, gap_found := search(gap_seqno_list, iter_seqno)
			if gap_found {
				continue
			}
		}

		// stop if cannot find seqno in any of the lists
		break
	}

	if last_sent_index >= 0 || last_filtered_index >= 0 || last_failed_cr_index >= 0 {
		if found_seqno_type == SeqnoTypeSent {
			through_seqno = uint64(sent_seqno_list[last_sent_index])
		} else if found_seqno_type == SeqnoTypeFiltered {
			through_seqno = uint64(filtered_seqno_list[last_filtered_index])
		} else if found_seqno_type == SeqnoTypeFailedCR {
			through_seqno = uint64(failed_cr_seqno_list[last_failed_cr_index])
		} else {
			panic(fmt.Sprintf("unexpected found_seqno_type, %v", found_seqno_type))
		}

		tsTracker.logger.Infof("%v, vbno=%v, setting new through_seqno=%v\n", tsTracker.id, vbno, through_seqno)

		through_seqno_obj.SetSeqnoWithoutLock(through_seqno)

		// truncate no longer needed entries from seqno lists to reduce memory/cpu overhead for future computations
		go tsTracker.truncateSeqnoLists(vbno, through_seqno)
	}

	tsTracker.logger.Debugf("%v, vbno=%v, through_seqno=%v\n", tsTracker.id, vbno, through_seqno)
	return through_seqno
}

func search(seqno_list []int, seqno uint64) (int, bool) {
	index := sort.Search(len(seqno_list), func(i int) bool {
		return seqno_list[i] >= int(seqno)
	})
	if index < len(seqno_list) && seqno_list[index] == int(seqno) {
		return index, true
	} else {
		return index, false
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqnos() map[uint16]uint64 {
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
	tsTracker.logger.Debugf("%v getThroughSeqnos executor %v is working on vbuckets %v", tsTracker.id, executor_id, listOfVbs)
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

func maxSeqno(seqno_list []int) uint64 {
	length := len(seqno_list)
	if length > 0 {
		return uint64(seqno_list[length-1])
	} else {
		return 0
	}
}
