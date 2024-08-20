// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline_utils"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
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
	RemovePipeline(pipeline common.Pipeline)
	Spec() *metadata.ReplicationSpecification
	RepId() string
	AddError(err error)
	AddErrorsFromMap(errMap base.ErrorMap)
	RuntimeStatus(lock bool) ReplicationState
	Storage(pipelineType common.PipelineType) *expvar.Map
	// Called by UI every second
	GetStats(registryName string, pipelineType common.PipelineType) *expvar.Map
	GetSpecInternalId() string
	GetOverviewStats(pipelineType common.PipelineType) *expvar.Map
	SetStats(registryName string, stats *expvar.Map, pipelineType common.PipelineType)
	SetOverviewStats(stats *expvar.Map, pipelineType common.PipelineType)
	CleanupBeforeExit(statsToClear []string)
	ResetStorage(pipelineType common.PipelineType)
	Publish(lock bool)
	PublishWithStatus(status string, lock bool)
	Pipeline() common.Pipeline
	BackfillPipeline() common.Pipeline
	AllPipelines() []common.Pipeline
	VbList() []uint16
	SetVbList(vb_list []uint16)
	SettingsMap() map[string]interface{}
	Errors() PipelineErrorArray
	ClearErrors()
	ClearErrorsWithString(subStr string)
	RecordProgress(progress string)
	GetProgress() string
	String() string
	Updater() interface{}
	SetUpdater(updater interface{}) error
	SetCustomSettings(customSettings map[string]interface{})
	ClearCustomSetting(settingsKey string)
	ClearTemporaryCustomSettings()
	GetEventsManager() PipelineEventsManager
	GetEventsProducer() common.PipelineEventsProducer
	PopulateReplInfo(replInfo *base.ReplicationInfo, bypassUIErrorCodes func(string) bool, processErrorMsgForUI func(string) string)
	LoadLatestBrokenMap()
	RecordBackfillProgress(progress string)
}

type ReplicationStatus struct {
	pipeline_           common.Pipeline
	backfillPipeline_   common.Pipeline
	err_list            PipelineErrorArray // a.k.a. high priority events
	progress            string
	oldProgress         string
	backfillProgress    string
	backfillOldProgress string
	logger              *log.CommonLogger
	specId              string
	specInternalId      string
	spec_getter         ReplicationSpecGetter
	pipeline_updater    interface{}
	lock                *sync.RWMutex
	customSettings      map[string]interface{}
	// tracks the list of vbs managed by the replication.
	// useful when replication is paused, when it can be compared with the current vb_list to determine
	// whether topology change has occured on source
	vb_list []uint16

	eventIdWell   *int64
	eventsManager PipelineEventsManager

	utils           utils.UtilsIface
	loadBrokenMapCh chan bool
}

func NewReplicationStatus(specId string, spec_getter ReplicationSpecGetter, logger *log.CommonLogger, eventIdWell *int64, utils utils.UtilsIface) *ReplicationStatus {
	if eventIdWell == nil {
		idWell := int64(-1)
		eventIdWell = &idWell
	}

	rep_status := &ReplicationStatus{specId: specId,
		pipeline_:       nil,
		logger:          logger,
		err_list:        make(PipelineErrorArray, 0, PipelineErrorMaxEntries),
		spec_getter:     spec_getter,
		lock:            &sync.RWMutex{},
		customSettings:  make(map[string]interface{}),
		progress:        "",
		eventIdWell:     eventIdWell,
		eventsManager:   NewPipelineEventsMgr(eventIdWell, specId, spec_getter, logger, utils),
		loadBrokenMapCh: make(chan bool, 1),
		utils:           utils,
	}

	// Allow first caller to load brokenMap
	rep_status.loadBrokenMapCh <- true

	rep_status.Publish(false)
	return rep_status
}

func (rs *ReplicationStatus) SetPipeline(pipeline common.Pipeline) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	switch pipeline.Type() {
	case common.MainPipeline:
		rs.pipeline_ = pipeline
		if pipeline != nil {
			rs.vb_list = pipeline_utils.GetSourceVBListPerPipeline(pipeline)
			base.SortUint16List(rs.vb_list)
			rs.specInternalId = pipeline.Specification().GetReplicationSpec().InternalId
		}
	case common.BackfillPipeline:
		rs.backfillPipeline_ = pipeline
	default:
		rs.logger.Errorf("SetPipeline called on unknown pipeline type %v", pipeline.Type())
	}

	rs.Publish(false)
}

func (rs *ReplicationStatus) RemovePipeline(pipeline common.Pipeline) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	switch pipeline.Type() {
	case common.MainPipeline:
		rs.pipeline_ = nil
	case common.BackfillPipeline:
		rs.backfillPipeline_ = nil
	default:
		rs.logger.Warnf("Unknown pipeline type: %v\n", pipeline.Type())
	}
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

// return the corresponding expvar map as its storage
func (rs *ReplicationStatus) Storage(pipelineType common.PipelineType) *expvar.Map {
	var rep_map *expvar.Map
	root_map := RootStorage()
	id := common.ComposeFullTopic(rs.specId, pipelineType)
	rep_map_var := root_map.Get(id)
	if rep_map_var == nil {
		rep_map = new(expvar.Map).Init()
		root_map.Set(id, rep_map)
	} else {
		rep_map = rep_map_var.(*expvar.Map)
	}

	return rep_map
}

// Called by UI every second
func (rs *ReplicationStatus) GetStats(registryName string, pipelineType common.PipelineType) *expvar.Map {
	expvar_var := rs.Storage(pipelineType)
	stats := expvar_var.Get(registryName)
	if stats != nil {
		statsMap, ok := stats.(*expvar.Map)
		if ok {
			return statsMap
		}
	}
	return nil
}

func (rs *ReplicationStatus) GetOverviewStats(pipelineType common.PipelineType) *expvar.Map {
	return rs.GetStats(OVERVIEW_METRICS_KEY, pipelineType)
}

func (rs *ReplicationStatus) SetStats(registryName string, stats *expvar.Map, pipelineType common.PipelineType) {
	expvar_var := rs.Storage(pipelineType)
	expvar_var.Set(registryName, stats)
}

func (rs *ReplicationStatus) SetOverviewStats(stats *expvar.Map, pipelineType common.PipelineType) {
	rs.SetStats(OVERVIEW_METRICS_KEY, stats, pipelineType)
}

func (rs *ReplicationStatus) CleanupBeforeExit(statsToClear []string) {
	overviewStats := rs.GetOverviewStats(common.MainPipeline)
	rs.ResetStorage(common.MainPipeline)

	backfillOverviewStats := rs.GetOverviewStats(common.BackfillPipeline)
	rs.ResetStorage(common.BackfillPipeline)

	// clear a subset of stats and preserve the rest
	rs.clearStats(statsToClear, overviewStats, common.MainPipeline)
	rs.clearStats(statsToClear, backfillOverviewStats, common.BackfillPipeline)

	rs.PublishWithStatus(base.Pending, true)
}

func (rs *ReplicationStatus) clearStats(statsToClear []string, overviewStats *expvar.Map, pipelineType common.PipelineType) {
	if overviewStats != nil {
		zero_var := new(expvar.Int)
		zero_var.Set(0)
		for _, statsToClear := range statsToClear {
			overviewStats.Set(statsToClear, zero_var)
		}
		rs.SetOverviewStats(overviewStats, pipelineType)
	}
}

func RootStorage() *expvar.Map {
	replications_root_map := expvar.Get(base.XDCR_EXPVAR_ROOT)
	if replications_root_map == nil {
		return expvar.NewMap(base.XDCR_EXPVAR_ROOT)
	}
	return replications_root_map.(*expvar.Map)
}

func (rs *ReplicationStatus) ResetStorage(pipelineType common.PipelineType) {
	root_map := RootStorage()
	id := common.ComposeFullTopic(rs.specId, pipelineType)
	root_map.Set(id, nil)
}

func (rs *ReplicationStatus) Publish(lock bool) {
	rs.PublishWithStatus(rs.RuntimeStatus(lock).String(), lock)
}

// there may be cases, e.g., when we are about to pause the replication, where we want to publish
// a specified status instead of the one inferred from pipeline.State()
// Any type of publish is outward facing and will only contain main pipeline information
func (rs *ReplicationStatus) PublishWithStatus(status string, lock bool) {
	if lock {
		rs.lock.RLock()
		defer rs.lock.RUnlock()
	}

	rep_map := rs.Storage(common.MainPipeline)

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

	//publish oldBackfillProgress
	backfillOldProgressVar := new(expvar.String)
	backfillOldProgressVar.Set(rs.backfillOldProgress)
	rep_map.Set("Backfill Old Progress", backfillOldProgressVar)

	// publish backfill progress
	backfillProgressVar := new(expvar.String)
	backfillProgressVar.Set(rs.backfillProgress)
	rep_map.Set("Backfill Progress", backfillProgressVar)

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

func (rs *ReplicationStatus) BackfillPipeline() common.Pipeline {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.backfillPipeline_
}

func (rs *ReplicationStatus) AllPipelines() []common.Pipeline {
	var retList []common.Pipeline
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	if rs.pipeline_ != nil {
		retList = append(retList, rs.pipeline_)
	}
	if rs.backfillPipeline_ != nil {
		retList = append(retList, rs.backfillPipeline_)
	}
	return retList
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

	// if replicationStatus has errors, note them so prometheus can read the stats
	numErrs := len(rs.err_list)
	if numErrs > 0 {
		settingsMap[service_def.PIPELINE_STATUS] = base.PipelineStatusError
	}
	settingsMap[service_def.PIPELINE_ERRORS] = numErrs
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

// Errors are essentially high priority events
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

func (rs *ReplicationStatus) ClearErrorsWithString(subStr string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()

	if len(rs.err_list) == 0 {
		return
	}

	replacementArr := PipelineErrorArray{}
	for _, pipelineErr := range rs.err_list {
		if !strings.Contains(pipelineErr.ErrMsg, subStr) {
			replacementArr = append(replacementArr, pipelineErr)
		}
	}

	rs.err_list = replacementArr
	rs.Publish(false)
}

func (rs *ReplicationStatus) RecordProgress(progress string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.oldProgress = rs.progress
	rs.progress = progress
	rs.Publish(false)
}

func (rs *ReplicationStatus) RecordBackfillProgress(progress string) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.backfillOldProgress = rs.backfillProgress
	rs.backfillProgress = progress
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
	retStr := fmt.Sprintf("name={%v}, status={%v}, errors={%v}, oldProgress={%v}, progress={%v}", rs.specId, rs.RuntimeStatus(false), rs.err_list, rs.oldProgress, rs.progress)
	if rs.backfillOldProgress != "" || rs.backfillProgress != "" {
		retStr = fmt.Sprintf("%v, oldBackfillProgress={%v}, backfillProgress={%v}", retStr, rs.backfillOldProgress, rs.backfillProgress)
	}
	return retStr
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

func (rs *ReplicationStatus) GetSpecInternalId() string {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	return rs.specInternalId
}

func (rs *ReplicationStatus) GetEventsManager() PipelineEventsManager {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	return rs.eventsManager
}

func (rs *ReplicationStatus) GetEventsProducer() common.PipelineEventsProducer {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	return rs.eventsManager
}

// Note - no concurrency allowed for replInfo
func (rs *ReplicationStatus) PopulateReplInfo(replInfo *base.ReplicationInfo, bypassUIErrorCodes func(string) bool, processErrorMsgForUI func(string) string) {
	rs.LoadLatestBrokenMap()

	// First populate the legacy errors
	errs := rs.Errors()
	if len(errs) > 0 {
		for _, pipeline_error := range errs {
			if bypassUIErrorCodes(pipeline_error.ErrMsg) {
				continue
			}
			err_msg := processErrorMsgForUI(pipeline_error.ErrMsg)
			errInfo := base.NewErrorInfo(pipeline_error.Timestamp.UnixNano(), err_msg, rs.eventIdWell)
			(*replInfo).ErrorList = append((*replInfo).ErrorList, errInfo)
		}
	}

	// Then populate the non-error based events
	eventsList := rs.GetEventsManager().GetCurrentEvents()
	eventsList.Mutex.RLock()
	defer eventsList.Mutex.RUnlock()

	for i, event := range eventsList.EventInfos {
		errInfoFromReplInfo := base.NewErrorInfoFromEventInfo(event, eventsList.TimeInfos[i])
		replInfo.ErrorList = append(replInfo.ErrorList, errInfoFromReplInfo)
	}
}

func (rs *ReplicationStatus) LoadLatestBrokenMap() {
	// Load brokenMap could get slow or expensive if it gets too big
	// To prevent hanging up the caller, provide an eventual consistent API here
	select {
	case <-rs.loadBrokenMapCh:
		timeoutErr := base.ExecWithTimeout(rs.loadLatestBrokenMapInternal, base.ReplStatusLoadBrokenMapTimeout, rs.logger)
		if timeoutErr != nil {
			rs.logger.Warnf("Loading brokenMap is taking longer than usual. It will finish up eventually")
		}
	default:
		// A loading is taking place above - let it finish
		return
	}
}

// Should return only nil, but need error return to fit ExecWithTimeout argument signature
func (rs *ReplicationStatus) loadLatestBrokenMapInternal() error {
	var brokenMapRO metadata.CollectionNamespaceMapping
	var brokenMapDoneFunc func()

	doneFunc := rs.utils.StartDiagStopwatch(fmt.Sprintf("loadLatestBrokenMapInternal %v", rs.RepId()), base.ReplStatusLoadBrokenMapTimeout)
	defer doneFunc()

	repStatusPipeline := rs.Pipeline()
	if repStatusPipeline != nil {
		brokenMapRO, brokenMapDoneFunc = repStatusPipeline.GetBrokenMapRO()
	}
	if brokenMapDoneFunc != nil {
		defer brokenMapDoneFunc()
	}
	if brokenMapRO != nil {
		rs.GetEventsManager().LoadLatestBrokenMap(brokenMapRO)
	}

	// Once it is done, put back the token
	rs.loadBrokenMapCh <- true
	return nil
}
