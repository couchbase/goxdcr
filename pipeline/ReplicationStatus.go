// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/simple_utils"
	"sync"
	"time"
)

type ReplicationState int

const (
	Pending     ReplicationState = iota
	Replicating ReplicationState = iota
	Paused      ReplicationState = iota
)

var OVERVIEW_METRICS_KEY = "Overview"

func (rep_state ReplicationState) String() string {
	if rep_state == Pending {
		return base.Pending
	} else if rep_state == Replicating {
		return base.Replicating
	} else if rep_state == Paused {
		return base.Paused
	} else {
		panic("Invalid rep_state")
	}
}

type PipelineError struct {
	Timestamp time.Time `json:"time"`
	ErrMsg    string    `json:"errMsg"`
}

type PipelineErrorArray []PipelineError

var PipelineErrorArray_Max_Size int = 20

type ReplicationSpecGetter func(specId string) (*metadata.ReplicationSpecification, error)

func (errArray PipelineErrorArray) String() string {
	bytes, err := json.Marshal(errArray)
	if err != nil {
		fmt.Printf(" Failed to marshal PipelineErrorArray. err=%v\n", err)
		return ""
	}

	return string(bytes)
}

type ReplicationStatus struct {
	pipeline         common.Pipeline
	err_list         PipelineErrorArray
	progress         string
	logger           *log.CommonLogger
	specId           string
	spec_getter      ReplicationSpecGetter
	pipeline_updater interface{}
	obj_pool         *base.MCRequestPool
	Lock             *sync.RWMutex
	// tracks the list of vbs managed by the replication.
	// useful when replication is paused, when it can be compared with the current vb_list to determine
	// whether topology change has occured on source
	vb_list []uint16
}

func NewReplicationStatus(specId string, spec_getter ReplicationSpecGetter, logger *log.CommonLogger) *ReplicationStatus {
	rep_status := &ReplicationStatus{specId: specId,
		pipeline:    nil,
		logger:      logger,
		err_list:    PipelineErrorArray{},
		spec_getter: spec_getter,
		Lock:        &sync.RWMutex{},
		obj_pool:    base.NewMCRequestPool(specId, logger),
		progress:    ""}

	rep_status.Publish(false)
	return rep_status
}

func (rs *ReplicationStatus) SetPipeline(pipeline common.Pipeline) {
	rs.Lock.Lock()
	defer rs.Lock.Unlock()
	rs.pipeline = pipeline
	if pipeline != nil {
		rs.vb_list = pipeline_utils.GetSourceVBListPerPipeline(pipeline)
		simple_utils.SortUint16List(rs.vb_list)
	}

	rs.Publish(false)
}

func (rs *ReplicationStatus) Spec() *metadata.ReplicationSpecification {
	spec, err := rs.spec_getter(rs.specId)
	if err != nil {
		rs.logger.Errorf("Invalid replication status %v, failed to retrieve spec. err=%v", rs.specId, err)

	} else if spec == nil {
		//it is possible that spec is nil. When replication specification is deleted,
		//ReplicationSpecVal.spec is set to nil, but it is not removed from cached to keep
		//replication status there so that we have a place to retrieve pipeline and proper action
		//can be taken, like stop the pipline etc.
		rs.logger.Infof("Spec=nil for replication status %v, which means replication specification has been deleted.", rs.specId)
	}
	return spec
}

func (rs *ReplicationStatus) RepId() string {
	return rs.specId
}

func (rs *ReplicationStatus) AddError(err error) {
	// need to lock because this method could be called concurrently from pipeline_manager and updater
	rs.Lock.Lock()
	defer rs.Lock.Unlock()

	if err != nil {
		end := len(rs.err_list)
		if end > PipelineErrorArray_Max_Size-1 {
			end = PipelineErrorArray_Max_Size - 1
		}
		rs.err_list = append(rs.err_list[:end], PipelineError{})
		for i := len(rs.err_list) - 1; i > 0; i-- {
			rs.err_list[i] = rs.err_list[i-1]
		}
		errStr := err.Error()

		rs.err_list[0] = PipelineError{Timestamp: time.Now(), ErrMsg: errStr}
		rs.Publish(false)
	}
}

func (rs *ReplicationStatus) RuntimeStatus(lock bool) ReplicationState {
	if lock {
		rs.Lock.RLock()
		defer rs.Lock.RUnlock()
	}

	spec := rs.Spec()
	if rs.pipeline != nil && rs.pipeline.State() == common.Pipeline_Running {
		return Replicating
	} else if spec != nil && !spec.Settings.Active {
		return Paused
	} else {
		return Pending
	}
}

//return the corresponding expvar map as its storage
func (rs *ReplicationStatus) Storage() *expvar.Map {
	var rep_map *expvar.Map
	root_map := RootStorage()
	rep_map_var := root_map.Get(rs.specId)
	if rep_map_var == nil {
		rep_map = new(expvar.Map).Init()
		root_map.Set(rs.specId, rep_map)
	} else {
		rep_map = rep_map_var.(*expvar.Map)
	}

	return rep_map
}

func (rs *ReplicationStatus) GetStats(registryName string) *expvar.Map {
	expvar_var := rs.Storage()
	stats := expvar_var.Get(registryName)
	if stats != nil {
		statsMap, ok := stats.(*expvar.Map)
		if ok {
			return statsMap
		}
	}
	return nil
}

func (rs *ReplicationStatus) GetOverviewStats() *expvar.Map {
	return rs.GetStats(OVERVIEW_METRICS_KEY)
}

func (rs *ReplicationStatus) SetStats(registryName string, stats *expvar.Map) {
	expvar_var := rs.Storage()
	expvar_var.Set(registryName, stats)
}

func (rs *ReplicationStatus) SetOverviewStats(stats *expvar.Map) {
	rs.SetStats(OVERVIEW_METRICS_KEY, stats)
}

func (rs *ReplicationStatus) CleanupBeforeExit(statsToClear []string) {
	errList := rs.Errors()
	overviewStats := rs.GetOverviewStats()
	rs.ResetStorage()
	// preserve error list
	rs.err_list = errList
	// clear a subset of stats and preserve the rest
	if overviewStats != nil {
		zero_var := new(expvar.Int)
		zero_var.Set(0)
		for _, statsToClear := range statsToClear {
			overviewStats.Set(statsToClear, zero_var)
		}
		rs.SetOverviewStats(overviewStats)
	}

	rs.publishWithStatus(base.Pending, true)
}

func RootStorage() *expvar.Map {
	replications_root_map := expvar.Get(base.XDCR_EXPVAR_ROOT)
	if replications_root_map == nil {
		return expvar.NewMap(base.XDCR_EXPVAR_ROOT)
	}
	return replications_root_map.(*expvar.Map)
}

func (rs *ReplicationStatus) ResetStorage() {
	root_map := RootStorage()
	root_map.Set(rs.specId, nil)
}

func (rs *ReplicationStatus) Publish(lock bool) {
	rs.publishWithStatus(rs.RuntimeStatus(lock).String(), lock)
}

// there may be cases, e.g., when we are about to pause the replication, where we want to publish
// a specified status instead of the one inferred from pipeline.State()
func (rs *ReplicationStatus) publishWithStatus(status string, lock bool) {
	if lock {
		rs.Lock.RLock()
		defer rs.Lock.RUnlock()
	}

	rep_map := rs.Storage()

	//publish status
	statusVar := new(expvar.String)
	statusVar.Set(status)
	rep_map.Set("Status", statusVar)

	//publish progress
	progress := rs.progress
	progressVar := new(expvar.String)
	progressVar.Set(progress)
	rep_map.Set("Progress", progressVar)

	//publish errors
	errorVar := new(expvar.String)
	errorVar.Set(rs.err_list.String())
	rep_map.Set(base.ErrorsStatsKey, errorVar)

}

func (rs *ReplicationStatus) Pipeline() common.Pipeline {
	rs.Lock.RLock()
	defer rs.Lock.RUnlock()
	return rs.pipeline
}

func (rs *ReplicationStatus) VbList() []uint16 {
	rs.Lock.RLock()
	defer rs.Lock.RUnlock()
	return rs.vb_list
}

func (rs *ReplicationStatus) SetVbList(vb_list []uint16) {
	rs.Lock.Lock()
	defer rs.Lock.Unlock()
	rs.vb_list = vb_list
}

func (rs *ReplicationStatus) SettingsMap() map[string]interface{} {
	settings := rs.Settings()
	if settings != nil {
		return settings.ToMap()
	} else {
		//empty map
		return make(map[string]interface{})
	}
}

func (rs *ReplicationStatus) Settings() *metadata.ReplicationSettings {
	spec := rs.Spec()
	if spec != nil {
		return spec.Settings
	} else {
		return nil
	}
}

func (rs *ReplicationStatus) Errors() PipelineErrorArray {
	rs.Lock.RLock()
	defer rs.Lock.RUnlock()
	return rs.err_list
}

func (rs *ReplicationStatus) ClearErrors() {
	rs.Lock.Lock()
	defer rs.Lock.Unlock()
	rs.err_list = PipelineErrorArray{}
}

func (rs *ReplicationStatus) RecordProgress(progress string) {
	rs.Lock.Lock()
	defer rs.Lock.Unlock()
	rs.progress = progress
	rs.Publish(false)
}

func (rs *ReplicationStatus) GetProgress() string {
	rs.Lock.RLock()
	defer rs.Lock.RUnlock()
	return rs.progress
}

func (rs *ReplicationStatus) String() string {
	return fmt.Sprintf("name={%v}, status={%v}, errors={%v}, progress={%v}\n", rs.specId, rs.RuntimeStatus(true), rs.Errors(), rs.progress)
}

func (rs *ReplicationStatus) Updater() interface{} {
	rs.Lock.RLock()
	defer rs.Lock.RUnlock()
	return rs.pipeline_updater
}

func (rs *ReplicationStatus) SetUpdater(updater interface{}) error {
	rs.Lock.Lock()
	defer rs.Lock.Unlock()
	if rs.pipeline_updater != nil && updater != nil {
		return errors.New("There is already an updater in place, can't set the updater")
	}
	rs.pipeline_updater = updater
	return nil
}

func (rs *ReplicationStatus) ObjectPool() *base.MCRequestPool {
	return rs.obj_pool
}
