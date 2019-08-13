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

// temporary custom settings that need to be cleared after pipeline update
var TemporaryCustomSettings = []string{metadata.CompressionTypeKey}

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

const PipelineErrorMaxEntries = 20

type ReplicationSpecGetter func(specId string) (*metadata.ReplicationSpecification, error)

func (errArray PipelineErrorArray) String() string {
	bytes, err := json.Marshal(errArray)
	if err != nil {
		fmt.Printf(" Failed to marshal PipelineErrorArray. err=%v\n", err)
		return ""
	}

	return string(bytes)
}

type ReplicationStatusIface interface {
	SetPipeline(pipeline common.Pipeline)
	Spec() *metadata.ReplicationSpecification
	RepId() string
	AddError(err error)
	AddErrorsFromMap(errMap base.ErrorMap)
	RuntimeStatus(lock bool) ReplicationState
	Storage() *expvar.Map
	// Called by UI every second
	GetStats(registryName string) *expvar.Map
	GetSpecInternalId() string
	GetOverviewStats() *expvar.Map
	SetStats(registryName string, stats *expvar.Map)
	SetOverviewStats(stats *expvar.Map)
	CleanupBeforeExit(statsToClear []string)
	ResetStorage()
	Publish(lock bool)
	PublishWithStatus(status string, lock bool)
	Pipeline() common.Pipeline
	VbList() []uint16
	SetVbList(vb_list []uint16)
	SettingsMap() map[string]interface{}
	Errors() PipelineErrorArray
	ClearErrors()
	RecordProgress(progress string)
	GetProgress() string
	String() string
	Updater() interface{}
	SetUpdater(updater interface{}) error
	ObjectPool() *base.MCRequestPool
	SetCustomSettings(customSettings map[string]interface{})
	ClearCustomSetting(settingsKey string)
	ClearTemporaryCustomSettings()
}

type ReplicationStatus struct {
	pipeline_        common.Pipeline
	err_list         PipelineErrorArray
	progress         string
	oldProgress      string
	logger           *log.CommonLogger
	specId           string
	specInternalId   string
	spec_getter      ReplicationSpecGetter
	pipeline_updater interface{}
	obj_pool         *base.MCRequestPool
	lock             *sync.RWMutex
	customSettings   map[string]interface{}
	// tracks the list of vbs managed by the replication.
	// useful when replication is paused, when it can be compared with the current vb_list to determine
	// whether topology change has occured on source
	vb_list []uint16
}

func NewReplicationStatus(specId string, spec_getter ReplicationSpecGetter, logger *log.CommonLogger) *ReplicationStatus {
	rep_status := &ReplicationStatus{specId: specId,
		pipeline_:      nil,
		logger:         logger,
		err_list:       make(PipelineErrorArray, 0, PipelineErrorMaxEntries),
		spec_getter:    spec_getter,
		lock:           &sync.RWMutex{},
		obj_pool:       base.NewMCRequestPool(specId, logger),
		customSettings: make(map[string]interface{}),
		progress:       ""}

	rep_status.Publish(false)
	return rep_status
}

func (rs *ReplicationStatus) SetPipeline(pipeline common.Pipeline) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.pipeline_ = pipeline
	if pipeline != nil {
		rs.vb_list = pipeline_utils.GetSourceVBListPerPipeline(pipeline)
		base.SortUint16List(rs.vb_list)
		rs.specInternalId = pipeline.Specification().InternalId
	}

	rs.Publish(false)
}

func (rs *ReplicationStatus) SetCustomSettings(customSettings map[string]interface{}) {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	for key, value := range customSettings {
		rs.customSettings[key] = value
	}
}

func (rs *ReplicationStatus) ClearCustomSetting(settingsKey string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	delete(rs.customSettings, settingsKey)
}

func (rs *ReplicationStatus) ClearTemporaryCustomSettings() {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	for _, settingsKey := range TemporaryCustomSettings {
		delete(rs.customSettings, settingsKey)
	}
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
	rs.lock.Lock()
	defer rs.lock.Unlock()

	if err != nil {
		end := len(rs.err_list)
		if end > PipelineErrorMaxEntries-1 {
			end = PipelineErrorMaxEntries - 1
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

func (rs *ReplicationStatus) AddErrorsFromMap(errMap base.ErrorMap) {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	var i int

	// Need to potentially shift existing errors over
	var errorsToKeep int
	if len(errMap) == 0 {
		return
	} else if len(errMap) >= PipelineErrorMaxEntries {
		errorsToKeep = 0
	} else if len(errMap)+len(rs.err_list) <= PipelineErrorMaxEntries {
		errorsToKeep = len(rs.err_list)
	} else {
		errorsToKeep = PipelineErrorMaxEntries - len(errMap)
	}

	// Grow the slice if needed
	totalErrorsToBeInList := base.IntMin((errorsToKeep + len(errMap)), PipelineErrorMaxEntries)
	needToGrowListBy := totalErrorsToBeInList - len(rs.err_list)
	for i = 0; i < needToGrowListBy; i++ {
		rs.err_list = append(rs.err_list, PipelineError{})
	}

	// Shift existing elements that are to be saved
	keepBeginIndex := totalErrorsToBeInList - errorsToKeep
	for i = totalErrorsToBeInList - 1; i >= keepBeginIndex; i-- {
		readFromIndex := i - keepBeginIndex
		rs.err_list[i] = rs.err_list[readFromIndex]
	}

	// Now that shifted has finished, fill in the newer ones
	i = 0
	for k, v := range errMap {
		if i == keepBeginIndex {
			break
		}
		rs.err_list[i] = PipelineError{Timestamp: time.Now(), ErrMsg: fmt.Sprintf("%s:%s", k, v.Error())}
		i++
	}
	rs.Publish(false)
}

func (rs *ReplicationStatus) RuntimeStatus(lock bool) ReplicationState {
	if lock {
		rs.lock.RLock()
		defer rs.lock.RUnlock()
	}

	spec := rs.Spec()
	if rs.pipeline_ != nil && rs.pipeline_.State() == common.Pipeline_Running {
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

// Called by UI every second
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
	overviewStats := rs.GetOverviewStats()
	rs.ResetStorage()
	// clear a subset of stats and preserve the rest
	if overviewStats != nil {
		zero_var := new(expvar.Int)
		zero_var.Set(0)
		for _, statsToClear := range statsToClear {
			overviewStats.Set(statsToClear, zero_var)
		}
		rs.SetOverviewStats(overviewStats)
	}

	rs.PublishWithStatus(base.Pending, true)
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
	rs.PublishWithStatus(rs.RuntimeStatus(lock).String(), lock)
}

// there may be cases, e.g., when we are about to pause the replication, where we want to publish
// a specified status instead of the one inferred from pipeline.State()
func (rs *ReplicationStatus) PublishWithStatus(status string, lock bool) {
	if lock {
		rs.lock.RLock()
		defer rs.lock.RUnlock()
	}

	rep_map := rs.Storage()

	//publish status
	statusVar := new(expvar.String)
	statusVar.Set(status)
	rep_map.Set("Status", statusVar)

	//publish old progress
	oldProgressVar := new(expvar.String)
	oldProgressVar.Set(rs.oldProgress)
	rep_map.Set("OldProgress", oldProgressVar)

	//publish progress
	progressVar := new(expvar.String)
	progressVar.Set(rs.progress)
	rep_map.Set("Progress", progressVar)

	//publish errors
	errorVar := new(expvar.String)
	errorVar.Set(rs.err_list.String())
	rep_map.Set(base.ErrorsStatsKey, errorVar)

}

func (rs *ReplicationStatus) Pipeline() common.Pipeline {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.pipeline_
}

func (rs *ReplicationStatus) VbList() []uint16 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.vb_list
}

func (rs *ReplicationStatus) SetVbList(vb_list []uint16) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.vb_list = vb_list
}

func (rs *ReplicationStatus) SettingsMap() map[string]interface{} {
	settingsMap := rs.getSpecSettingsMap()

	rs.lock.RLock()
	defer rs.lock.RUnlock()

	// add custom settings on top of repl spec settings

	if len(rs.customSettings) > 0 {
		rs.logger.Infof("Applying custom settings for %v. settings = %v\n", rs.specId, rs.customSettings)
		for key, value := range rs.customSettings {
			settingsMap[key] = value
		}
	}

	return settingsMap
}

func (rs *ReplicationStatus) getSpecSettingsMap() map[string]interface{} {
	spec := rs.Spec()
	if spec != nil {
		return spec.Settings.ToMap(false /*isDefaultSettings*/)
	} else {
		return make(map[string]interface{})
	}
}

func (rs *ReplicationStatus) Errors() PipelineErrorArray {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.err_list
}

func (rs *ReplicationStatus) ClearErrors() {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.err_list = PipelineErrorArray{}
	rs.Publish(false)
}

func (rs *ReplicationStatus) RecordProgress(progress string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.oldProgress = rs.progress
	rs.progress = progress
	rs.Publish(false)
}

func (rs *ReplicationStatus) GetProgress() string {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.progress
}

func (rs *ReplicationStatus) String() string {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return fmt.Sprintf("name={%v}, status={%v}, errors={%v}, oldProgress={%v}, progress={%v}\n", rs.specId, rs.RuntimeStatus(false), rs.err_list, rs.oldProgress, rs.progress)
}

// The caller should check if return value is null, if the creation process is ongoing
func (rs *ReplicationStatus) Updater() interface{} {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.pipeline_updater
}

func (rs *ReplicationStatus) SetUpdater(updater interface{}) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	if updater == nil {
		return errors.New("We should not allow updater to be cleared")
	}
	rs.pipeline_updater = updater
	return nil
}

func (rs *ReplicationStatus) ObjectPool() *base.MCRequestPool {
	return rs.obj_pool
}

func (rs *ReplicationStatus) GetSpecInternalId() string {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.specInternalId
}
