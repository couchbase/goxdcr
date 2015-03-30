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
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"sort"
	"sync"
)

type ThroughSeqnoTrackerSvc struct {
	// list of vbs that the tracker tracks
	vb_list []uint16

	// through_seqno seen by outnozzles based on the docs that are actually sent to target
	through_seqno_map       map[uint16]uint64
	through_seqno_map_locks map[uint16]*sync.RWMutex

	// some docs may not be sent to target, e.g., because they were filtered out or failed conflict resolution on source,
	// such docs still need to be included for through_seqno computation
	// this map stores a sorted list of such seqnos for each vb to faciliate through_seqno computation
	vb_notsent_seqno_list_map   map[uint16][]int
	vb_notsent_seqno_list_locks map[uint16]*sync.RWMutex

	logger *log.CommonLogger
}

func NewThroughSeqnoTrackerSvc(logger_ctx *log.LoggerContext) *ThroughSeqnoTrackerSvc {
	logger := log.NewLogger("ThroughSeqnoTrackerSvc", logger_ctx)
	tsTracker := &ThroughSeqnoTrackerSvc{
		logger:                      logger,
		through_seqno_map:           make(map[uint16]uint64),
		through_seqno_map_locks:     make(map[uint16]*sync.RWMutex),
		vb_notsent_seqno_list_map:   make(map[uint16][]int),
		vb_notsent_seqno_list_locks: make(map[uint16]*sync.RWMutex)}
	return tsTracker
}

func (tsTracker *ThroughSeqnoTrackerSvc) initialize(pipeline common.Pipeline) {
	tsTracker.vb_list = pipeline_utils.GetSourceVBListPerPipeline(pipeline)
	for _, vbno := range tsTracker.vb_list {
		tsTracker.through_seqno_map_locks[vbno] = &sync.RWMutex{}

		tsTracker.vb_notsent_seqno_list_map[vbno] = make([]int, 0)
		tsTracker.vb_notsent_seqno_list_locks[vbno] = &sync.RWMutex{}
		
		tsTracker.through_seqno_map[vbno] = 0
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) Attach(pipeline common.Pipeline) error {
	tsTracker.logger.Infof("Attach through seqno tracker with pipeline %v\n", pipeline.InstanceId())

	tsTracker.initialize(pipeline)

	outNozzle_parts := pipeline.Targets()
	for _, part := range outNozzle_parts {
		part.RegisterComponentEventListener(common.DataSent, tsTracker)
		part.RegisterComponentEventListener(common.DataFailedCRSource, tsTracker)
	}

	dcp_parts := pipeline.Sources()
	for _, dcp := range dcp_parts {
		//get connector, which is a router
		router := dcp.Connector().(*parts.Router)
		router.RegisterComponentEventListener(common.DataFiltered, tsTracker)
	}

	return nil
}

func (tsTracker *ThroughSeqnoTrackerSvc) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataSent {
		through_seqno, ok := otherInfos[parts.EVENT_ADDI_HISEQNO].(uint64)
		if ok {
			vbno := item.(*gomemcached.MCRequest).VBucket
			tsTracker.through_seqno_map_locks[vbno].Lock()
			defer tsTracker.through_seqno_map_locks[vbno].Unlock()
			tsTracker.through_seqno_map[vbno] = through_seqno
			tsTracker.logger.Debugf("tsTracker.through_seqno_map[%v].Seqno =%v\n", vbno, through_seqno)
		}
	} else if eventType == common.DataFiltered {
		seqno := item.(*mcc.UprEvent).Seqno
		vbno := item.(*mcc.UprEvent).VBucket
		tsTracker.addNotSentSeqno(vbno, seqno)
	} else if eventType == common.DataFailedCRSource {
		seqno, ok := otherInfos[parts.EVENT_ADDI_SEQNO].(uint64)
		if ok {
			vbno := item.(*gomemcached.MCRequest).VBucket
			tsTracker.addNotSentSeqno(vbno, seqno)
		}
	}

}

func (tsTracker *ThroughSeqnoTrackerSvc) addNotSentSeqno(vbno uint16, notsent_seqno uint64) {
	tsTracker.vb_notsent_seqno_list_locks[vbno].Lock()
	defer tsTracker.vb_notsent_seqno_list_locks[vbno].Unlock()
	tsTracker.vb_notsent_seqno_list_map[vbno] = append(tsTracker.vb_notsent_seqno_list_map[vbno], int(notsent_seqno))
	tsTracker.logger.Debugf("tsTracker.vb_notsent_seqno_list_map[%v]=%v\n", vbno, tsTracker.vb_notsent_seqno_list_map[vbno])
}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqno(vbno uint16) uint64 {
	tsTracker.through_seqno_map_locks[vbno].Lock()
	defer tsTracker.through_seqno_map_locks[vbno].Unlock()
	tsTracker.vb_notsent_seqno_list_locks[vbno].Lock()
	defer tsTracker.vb_notsent_seqno_list_locks[vbno].Unlock()

	seqno := tsTracker.through_seqno_map[vbno]
	seqno_list := tsTracker.vb_notsent_seqno_list_map[vbno]

	// algorithm:
	// if seqno+1 can be found in seqno_list, find the biggest N such that seqno+1, seqno+2, .., seqno+N all exist in seqno_list
	// set seqno+N as the high seqno
	// if seqno+1 cannot be found in seqno_list, return seqno  as the high seqno
	var through_seqno uint64
	index := sort.Search(len(seqno_list), func(i int) bool {
		return seqno_list[i] >= int(seqno+1)
	})
	if index < len(seqno_list) && seqno_list[index] == int(seqno+1) {
		// found seqno + 1

		// look for largest N such as seqno+1...seqno+N exist in seqno_list
		endIndex := index
		for ; endIndex < len(seqno_list)-1; endIndex++ {
			if seqno_list[endIndex+1] != seqno_list[endIndex]+1 {
				break
			}
		}

		// seqno+N = seqno_list[endIndex] by now
		through_seqno = uint64(seqno_list[endIndex])
		tsTracker.through_seqno_map[vbno] = through_seqno
		// truncate all entries up to and including through_seqno to expedite future searches
		tsTracker.vb_notsent_seqno_list_map[vbno] = seqno_list[endIndex+1:]
	} else {
		// did not find seqno + 1

		through_seqno = seqno
		if index > 0 {
			// truncate all entries smaller than through_seqno to expedite future searches
			tsTracker.vb_notsent_seqno_list_map[vbno] = seqno_list[index:]
		}
	}

	return through_seqno
}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqnos() map[uint16]uint64 {
	result_map := make(map[uint16]uint64)

	listOfVbs := tsTracker.vb_list
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
	tsTracker.logger.Debugf("getThroughSeqnos executor %v is working on vbuckets %v", executor_id, listOfVbs)
	if wait_grp == nil {
		panic("wait_grp can't be nil")
	}
	defer wait_grp.Done()

	for _, vbno := range listOfVbs {
		result_map[vbno] = tsTracker.GetThroughSeqno(vbno)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) SetStartSeqnos(start_seqno_map map[uint16]uint64) {
	for vbno, _ := range start_seqno_map {
		tsTracker.setStartSeqno(vbno, start_seqno_map[vbno])
	}

	tsTracker.logger.Infof("through_seqno_map in through seqno tracker has been set to %v\n", tsTracker.through_seqno_map)
}

func (tsTracker *ThroughSeqnoTrackerSvc) setStartSeqno(vbno uint16, seqno uint64) {
	tsTracker.through_seqno_map_locks[vbno].Lock()
	defer tsTracker.through_seqno_map_locks[vbno].Unlock()

	tsTracker.through_seqno_map[vbno] = seqno
}
