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
	ErrMsg       string     `json:"errMsg"`
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
	rep_spec    *metadata.ReplicationSpecification
	pipeline    common.Pipeline
	err_list    PipelineErrorArray
	settings    map[string]interface{}
	description string
	logger      *log.CommonLogger
}

func NewReplicationStatus(rep_spec *metadata.ReplicationSpecification, settings map[string]interface{}, logger *log.CommonLogger) *ReplicationStatus {
	rep_status := &ReplicationStatus{rep_spec: rep_spec,
		pipeline:    nil,
		logger:      logger,
		settings:    settings,
		description: ""}
	rep_status.publish()
	return rep_status
}

func (rs *ReplicationStatus) SetPipeline(pipeline common.Pipeline) {
	rs.pipeline = pipeline

	rs.publish()
}

func (rs *ReplicationStatus) SetSpec(rep_spec *metadata.ReplicationSpecification) {
	rs.rep_spec = rep_spec
	rs.publish()
}

func (rs *ReplicationStatus) AddError(err error) {
	length := len(rs.err_list)
	rs.err_list = append(rs.err_list, PipelineError{})
	for i := length; i > 0; i-- {
		rs.err_list[i] = rs.err_list[i-1]
	}
	rs.err_list[0] = PipelineError{Timestamp: time.Now(), ErrMsg: err.Error()}

	rs.logger.Infof("err_list=%v\n", rs.err_list)
	rs.publish()
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
	name := rs.rep_spec.Id
	return StorageForRep(name)
}

func StorageForRep(name string) *expvar.Map {
	var rep_map *expvar.Map
	root_map := RootStorage()
	rep_map_var := root_map.Get(name)
	if rep_map_var == nil {
		rep_map = new(expvar.Map).Init()
		root_map.Set(name, rep_map)
	} else {
		rep_map = rep_map_var.(*expvar.Map)
	}

	return rep_map
}

func RootStorage() *expvar.Map {
	replications_root_map := expvar.Get(base.XDCR_EXPVAR_ROOT)
	if replications_root_map == nil {
		return expvar.NewMap(base.XDCR_EXPVAR_ROOT)
	}
	return replications_root_map.(*expvar.Map)
}

func (rs *ReplicationStatus) publish() {
	rep_map := rs.Storage()

	//publish status
	status := rs.RuntimeStatus().String()
	statusVar := new(expvar.String)
	statusVar.Set(status)
	rep_map.Set("Status", statusVar)

	//publish errors
	errorVar := new(expvar.String)
	errorVar.Set(rs.err_list.String())
	rep_map.Set(base.ErrorsStatsKey, errorVar)

}

func (rs *ReplicationStatus) Pipeline() common.Pipeline {
	return rs.pipeline
}

func (rs *ReplicationStatus) Settings() map[string]interface{} {
	return rs.settings
}

func (rs *ReplicationStatus) PutSettings(settings map[string]interface{}) {
	rs.settings = settings
}

func (rs *ReplicationStatus) Errors() PipelineErrorArray {
	return rs.err_list
}
