package pipeline

import (
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
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

func (errArray PipelineErrorArray) String() string {
	bytes, err := json.Marshal(errArray)
	if err != nil {
		fmt.Printf(" Failed to marshal PipelineErrorArray. err=%v\n", err)
		return ""
	}

	return string(bytes)
}

type ReplicationStatus struct {
	rep_spec *metadata.ReplicationSpecification
	pipeline common.Pipeline
	err_list PipelineErrorArray
	progress string
	obj_pool *base.MCRequestPool
	logger   *log.CommonLogger
}

func NewReplicationStatus(rep_spec *metadata.ReplicationSpecification, logger *log.CommonLogger) *ReplicationStatus {
	rep_status := &ReplicationStatus{rep_spec: rep_spec,
		pipeline: nil,
		logger:   logger,
		err_list: PipelineErrorArray{},
		progress: "",
		obj_pool: base.NewMCRequestPool(rep_spec.Id, 0, logger)}
	rep_status.Publish()
	return rep_status
}

func (rs *ReplicationStatus) SetPipeline(pipeline common.Pipeline) {
	rs.pipeline = pipeline

	rs.Publish()
}

func (rs *ReplicationStatus) SetSpec(rep_spec *metadata.ReplicationSpecification) {
	rs.rep_spec = rep_spec
	rs.Publish()
}

func (rs *ReplicationStatus) AddError(err error) {
	if err != nil {
		length := len(rs.err_list)
		rs.err_list = append(rs.err_list, PipelineError{})
		for i := length; i > 0; i-- {
			rs.err_list[i] = rs.err_list[i-1]
		}
		errStr := err.Error()

		rs.err_list[0] = PipelineError{Timestamp: time.Now(), ErrMsg: errStr}

		rs.logger.Infof("err_list=%v\n", rs.err_list)
		rs.Publish()
	}
}

func (rs *ReplicationStatus) RuntimeStatus() ReplicationState {
	if rs.pipeline != nil && rs.pipeline.State() == common.Pipeline_Running {
		return Replicating
	} else if !rs.rep_spec.Settings.Active {
		return Paused
	} else {
		return Pending
	}
}

//return the corresponding expvar map as its storage
func (rs *ReplicationStatus) Storage() *expvar.Map {
	var rep_map *expvar.Map
	root_map := rootStorage()
	rep_map_var := root_map.Get(rs.rep_spec.Id)
	if rep_map_var == nil {
		rep_map = new(expvar.Map).Init()
		root_map.Set(rs.rep_spec.Id, rep_map)
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

	rs.publishWithStatus(base.Pending)
}

func rootStorage() *expvar.Map {
	replications_root_map := expvar.Get(base.XDCR_EXPVAR_ROOT)
	if replications_root_map == nil {
		return expvar.NewMap(base.XDCR_EXPVAR_ROOT)
	}
	return replications_root_map.(*expvar.Map)
}

func (rs *ReplicationStatus) ResetStorage() {
	root_map := rootStorage()
	root_map.Set(rs.rep_spec.Id, nil)
}

func (rs *ReplicationStatus) Publish() {
	rs.publishWithStatus(rs.RuntimeStatus().String())
}

// there may be cases, e.g., when we are about to pause the replication, where we want to publish
// a specified status instead of the one inferred from pipeline.State()
func (rs *ReplicationStatus) publishWithStatus(status string) {
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
	return rs.pipeline
}

func (rs *ReplicationStatus) Spec() *metadata.ReplicationSpecification {
	return rs.rep_spec
}

func (rs *ReplicationStatus) SettingsMap() map[string]interface{} {
	return rs.rep_spec.Settings.ToMap()
}

func (rs *ReplicationStatus) Settings() *metadata.ReplicationSettings {
	return rs.rep_spec.Settings
}

func (rs *ReplicationStatus) Errors() PipelineErrorArray {
	return rs.err_list
}

func (rs *ReplicationStatus) ClearErrors() {
	rs.err_list = PipelineErrorArray{}
}

func (rs *ReplicationStatus) RecordProgress(progress string) {
	rs.progress = progress
	rs.Publish()
}

func (rs *ReplicationStatus) String() string {
	return fmt.Sprintf("name={%v}, status={%v}, errors={%v}, progress={%v}, request_pool={%v}\n", rs.rep_spec.Id, rs.RuntimeStatus(), rs.Errors(), rs.progress, rs.obj_pool)
}

func (rs *ReplicationStatus) ObjectPool() *base.MCRequestPool {
	return rs.obj_pool
}
