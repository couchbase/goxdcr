/*
Copyright 2015-Present Couchbase, Inc.
Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_svc

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")
var target_topology_changedErr = errors.New("Topology has changed on target cluster")
var errPipelinesDetached = errors.New("No more pipelines are attached to this service")

var topologyDetectorIterationId uint32

type TopologyChangeDetectorSvc struct {
	*comp.AbstractComponent

	//xdcr topology service
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	remote_cluster_svc service_def.RemoteClusterSvc
	repl_spec_svc      service_def.ReplicationSpecSvc
	logger             *log.CommonLogger
	finish_ch          chan bool
	wait_grp           *sync.WaitGroup

	// the number of source topology changes seen so far
	source_topology_change_count int
	// the number of consecutive stable source topology seen so far
	source_topology_stable_count int
	// the number of target topology changes seen so far
	target_topology_change_count int
	// the number of consecutive stable target topology seen so far
	target_topology_stable_count int
	// list of vbs managed by the current node when pipeline was first started
	// used for source topology change detection
	vblist_original        []uint16
	vblistOriginalInitDone chan bool
	// list of vbs managed by the current node in the last topology change check time
	// used for source topology change detection
	vblist_last []uint16
	// vb server map of target bucket when pipeline was first started
	// used for target topology change detection
	target_vb_server_map_original map[uint16]string
	// vb server map of target bucket in the last topology change check time
	// used for target topology change detection
	target_vb_server_map_last map[uint16]string
	// number of nodes in source cluster
	number_of_source_nodes int

	// Timer to restart due to source side topology change
	sourceTopologyMaxWait           *time.Timer
	sourceTopologyMaxWaitTime       time.Time
	sourceTopologyMaxStableWait     *time.Timer
	sourceTopologyMaxStableWaitTime time.Time
	needToPostSourceTopologyEvent   bool

	// target side
	needToPostTargetTopologyEvent   bool
	targetTopologyMaxWaitTime       time.Time
	targetTopologyMaxStableWaitTime time.Time

	// key = hostname; value = https address of hostname
	httpsAddrMap map[string]string

	// whether main replication is of capi type
	capi bool

	utils utilities.UtilsIface

	// Multiple pipelines support
	pipelines         []common.Pipeline
	pipelinesMtx      sync.RWMutex
	detachCbs         []func()
	mainPipelineTopic string
	// Main pipeline to start or register once
	startOnce sync.Once

	enableCrossClusterVersioning bool
	versionPruningWindow         int

	bucketTopologySvc     service_def.BucketTopologySvc
	bucketTopSubscriberId string
}

func NewTopologyChangeDetectorSvc(xdcr_topology_svc service_def.XDCRCompTopologySvc, remote_cluster_svc service_def.RemoteClusterSvc, repl_spec_svc service_def.ReplicationSpecSvc, logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface, bucketTopologySvc service_def.BucketTopologySvc) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("TopoChangeDet", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		remote_cluster_svc:     remote_cluster_svc,
		repl_spec_svc:          repl_spec_svc,
		AbstractComponent:      comp.NewAbstractComponentWithLogger("TopoChangeDet", logger),
		finish_ch:              make(chan bool, 1),
		wait_grp:               &sync.WaitGroup{},
		logger:                 logger,
		vblist_last:            make([]uint16, 0),
		httpsAddrMap:           make(map[string]string),
		utils:                  utilsIn,
		bucketTopologySvc:      bucketTopologySvc,
		vblistOriginalInitDone: make(chan bool),
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipelinesMtx.Lock()
	defer top_detect_svc.pipelinesMtx.Unlock()
	pipelineType := "secondary"

	if len(top_detect_svc.pipelines) == 0 {
		top_detect_svc.capi = pipeline.Specification().GetReplicationSpec().Settings.IsCapi()
		pipelineType = "main"
		top_detect_svc.mainPipelineTopic = pipeline.Topic()
		top_detect_svc.bucketTopSubscriberId = fmt.Sprintf("%v_%v_%v", "topologySvc", pipeline.InstanceId(), base.GetIterationId(&topologyDetectorIterationId))
	}

	top_detect_svc.logger.Infof("Attaching %v pipeline", pipelineType)
	top_detect_svc.pipelines = append(top_detect_svc.pipelines, pipeline)
	top_detect_svc.detachCbs = append(top_detect_svc.detachCbs, nil)

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Start(settings metadata.ReplicationSettingsMap) error {
	top_detect_svc.pipelinesMtx.Lock()
	defer top_detect_svc.pipelinesMtx.Unlock()
	// Only the first Start() (main pipeline) is the actual start
	// Secondary starts are to ensure that detachCbs are registered

	if val, ok := settings[base.EnableCrossClusterVersioningKey]; ok {
		top_detect_svc.enableCrossClusterVersioning = val.(bool)
	}
	if val, ok := settings[base.VersionPruningWindowHrsKey]; ok {
		top_detect_svc.versionPruningWindow = val.(int)
	}

	for i, pipeline := range top_detect_svc.pipelines {
		//register itself with pipeline supervisor
		supervisor := pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
		if supervisor == nil {
			return fmt.Errorf("Error starting TopologyChangeDetectorSvc for pipeline %v since pipeline supervisor does not exist", pipeline.Topic())
		}

		if i == 0 {
			var startErr error
			top_detect_svc.startOnce.Do(func() {
				err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
				if err != nil {
					startErr = err
					return
				}
				top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline Starting...")

				go top_detect_svc.initializeMonitors()

				top_detect_svc.number_of_source_nodes, err = top_detect_svc.xdcr_topology_svc.NumberOfKVNodes()
				if err != nil {
					startErr = err
					return
				}
				top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline has started")
			})
			if startErr != nil {
				return startErr
			}
		} else if top_detect_svc.detachCbs[i] == nil {
			// This pipeline has not been started yet
			err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
			if err != nil {
				return err
			}
			top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for secondary pipeline Starting...")
			top_detect_svc.detachCbs[i] = func() {
				err := top_detect_svc.UnRegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
				top_detect_svc.logger.Infof("TopologyChangeDetectorSvc stopping by deregistering listener with err %v", err)
			}
		}
	}

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) initializeMonitors() {
	initMonitors := func() error {
		stopFunc := top_detect_svc.utils.StartDiagStopwatch(fmt.Sprintf("TopologyDetectSvc_%v_initializeMonitors", top_detect_svc.Id()), base.DiagTopologyMonitorThreshold)
		defer stopFunc()
		var initWaitGrp sync.WaitGroup
		var initSrcErr error
		var initTgtErr error
		initWaitGrp.Add(2)
		go func() {
			top_detect_svc.monitorSource(&initWaitGrp, &initSrcErr)
		}()
		go func() {
			top_detect_svc.monitorTarget(&initWaitGrp, &initTgtErr)
		}()
		initWaitGrp.Wait()
		if initSrcErr != nil || initTgtErr != nil {
			return fmt.Errorf("TopDetectSvc for %v MonitorSrc error: %v MonitorTgt error: %v", top_detect_svc.Id(), initSrcErr, initTgtErr)
		} else {
			return nil
		}
	}
	execErr := base.ExecWithTimeout(initMonitors, base.TimeoutRuntimeContextStart, top_detect_svc.logger)
	if execErr != nil {
		top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, execErr))
	}
	return
}

// Should only called by main pipeline
func (top_detect_svc *TopologyChangeDetectorSvc) Stop() error {
	close(top_detect_svc.finish_ch)
	top_detect_svc.wait_grp.Wait()
	top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline has stopped")
	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) pipelineHasStopped() bool {
	top_detect_svc.pipelinesMtx.RLock()
	var mainPipelineState common.PipelineState
	// Detach() is called before Stop() is called so there is a chance that pipelines no longer exist
	mainPipelineExists := len(top_detect_svc.pipelines) > 0
	if mainPipelineExists {
		mainPipelineState = top_detect_svc.pipelines[0].State()
	}
	top_detect_svc.pipelinesMtx.RUnlock()
	if !mainPipelineExists || !pipeline_utils.IsPipelineRunning(mainPipelineState) {
		//pipeline is no longer running, kill itself
		top_detect_svc.logger.Infof("Pipeline is no longer running. TopologyChangeDetectorSvc is exitting.")
		return true
	}
	return false
}

func (top_detect_svc *TopologyChangeDetectorSvc) handleSourceTopologyChange(vblist_supposed []uint16, number_of_source_nodes int, err_in error) error {
	defer top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline handleSourceTopologyChange completed")

	var err error
	if vblist_supposed != nil {
		vblist_removed, vblist_new, _ := base.ComputeDeltaOfUint16Lists(top_detect_svc.vblist_original, vblist_supposed, false)
		if len(vblist_removed) > 0 || len(vblist_new) > 0 {
			top_detect_svc.logger.Infof("Source topology changed for pipeline: vblist_removed=%v, vblist_new=%v\n", vblist_removed, vblist_new)
		}

		// first check if relevant problematic vbs in pipeline are due to source topology changes.
		err = top_detect_svc.validateVbErrors(vblist_removed, true /*source*/)
		if err != nil {
			return err
		}
	}

	// The concept of "stable_count" was introduced (and still applicable for target side) pre-streaming API
	// Prior to streaming API, XDCR would periodically pull VB topology changes on a periodic basis
	// If each time it pulls, the VBs remain the same, that would be considered a "count"
	// Ideally, the "count" was technically translated into a "period of time" because each pull would take place
	// on a regular interval
	// To keep things are regular as possible, we would need to translate the original settings that apply for
	// counting periods into a steady timer duration
	secsForMaxTopologyChange := int32(base.TopologyChangeCheckInterval.Seconds()) * int32(base.MaxTopologyChangeCountBeforeRestart)
	secsForMaxStableTopologyChange := int32(base.TopologyChangeCheckInterval.Seconds()) * int32(base.MaxTopologyStableCountBeforeRestart)

	if err_in == source_topology_changedErr || top_detect_svc.source_topology_change_count > 0 {
		top_detect_svc.source_topology_change_count++
		top_detect_svc.logger.Infof("Number of source topology changes seen by pipeline is %v\n", top_detect_svc.source_topology_change_count)
		if top_detect_svc.source_topology_change_count == 1 {
			// First instance of topology change, start timer
			top_detect_svc.sourceTopologyMaxWaitTime = time.Now().Add(time.Duration(secsForMaxTopologyChange) * time.Second)
			top_detect_svc.sourceTopologyMaxWait = time.AfterFunc(time.Duration(secsForMaxTopologyChange)*time.Second, func() {
				sourceTopoChangeRestartString := "Restarting pipeline due to source topology change..."
				top_detect_svc.logger.Warnf("%v", sourceTopoChangeRestartString)
				err = errors.New(sourceTopoChangeRestartString)
				top_detect_svc.restartPipeline(err)
			})
			top_detect_svc.needToPostSourceTopologyEvent = true
		}

		if vblist_supposed != nil {
			if base.AreSortedUint16ListsTheSame(top_detect_svc.vblist_last, vblist_supposed) {
				top_detect_svc.source_topology_stable_count++
				top_detect_svc.logger.Infof("Number of consecutive stable source topology seen by pipeline is %v\n", top_detect_svc.source_topology_stable_count)
				if top_detect_svc.source_topology_stable_count == 1 {
					// First instance of stable count, start timer
					top_detect_svc.sourceTopologyMaxStableWaitTime = time.Now().Add(time.Duration(secsForMaxStableTopologyChange) * time.Second)
					top_detect_svc.sourceTopologyMaxStableWait = time.AfterFunc(time.Duration(secsForMaxStableTopologyChange)*time.Second, func() {
						// restart pipeline if source topology change has stopped for a while and is assumbly completed
						err = fmt.Errorf("Source topology change for pipeline %v seems to have completed.", top_detect_svc.mainPipelineTopic)
						top_detect_svc.restartPipeline(err)
					})
				}
			} else {
				top_detect_svc.source_topology_stable_count = 0
				if top_detect_svc.sourceTopologyMaxStableWait != nil {
					top_detect_svc.sourceTopologyMaxStableWait.Stop()
					top_detect_svc.sourceTopologyMaxStableWait = nil
					// If we disabled a timer, we should double check the event information
					top_detect_svc.needToPostSourceTopologyEvent = true
				}
			}

			// otherwise, keep pipeline running for now.
			top_detect_svc.vblist_last = vblist_supposed

			// if number of source nodes has changed since last topology change check,
			// the bandwith limit assigned to the current node needs to be changed as well
			// update pipeline settings to get bandwith throttler updated
			if number_of_source_nodes != top_detect_svc.number_of_source_nodes {
				top_detect_svc.logger.Infof("Number of source nodes has changed from %v to %v. Updating bandwidth throttler setting.",
					top_detect_svc.number_of_source_nodes, number_of_source_nodes)
				settings := make(map[string]interface{})
				settings[NUMBER_OF_SOURCE_NODES] = number_of_source_nodes
				top_detect_svc.pipelinesMtx.RLock()
				if len(top_detect_svc.pipelines) > 0 {
					for _, pipeline := range top_detect_svc.pipelines {
						updateErr := pipeline.UpdateSettings(settings)
						if updateErr != nil {
							top_detect_svc.logger.Errorf("updating %v: %v", settings, updateErr)
						}
					}
				}
				top_detect_svc.pipelinesMtx.RUnlock()

				top_detect_svc.number_of_source_nodes = number_of_source_nodes
			}
		}
	}

	if top_detect_svc.needToPostSourceTopologyEvent {
		var times []time.Time
		if top_detect_svc.sourceTopologyMaxWait != nil {
			times = append(times, top_detect_svc.sourceTopologyMaxWaitTime)
		}
		if top_detect_svc.sourceTopologyMaxStableWait != nil {
			times = append(times, top_detect_svc.sourceTopologyMaxStableWaitTime)
		}
		err = top_detect_svc.postTopologyEstTime(true, times)
		if err != nil {
			top_detect_svc.logger.Warnf("Unable to post estimated times for source: %v", err)
		} else {
			top_detect_svc.needToPostSourceTopologyEvent = false
		}
	}

	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) postTopologyEstTime(source bool, times []time.Time) error {
	var timeToUse *time.Time
	// We have a list of timers. The timer that happens first will fire and we'll use that time for reporting
	for _, timeToCheck := range times {
		if timeToUse == nil {
			timeToUse = &timeToCheck
		}

		if timeToCheck.Before(*timeToUse) {
			timeToUse = &timeToCheck
		}
	}

	if timeToUse == nil {
		return fmt.Errorf("No time to use but called to post a time")
	}

	outputMap := make(metadata.ReplicationSettingsMap)
	key := metadata.SourceTopologyChangeStatusKey
	var srcOrTgtStr = "Source"
	if !source {
		key = metadata.TargetTopologyChangeStatusKey
		srcOrTgtStr = "Target"
	}

	myHostName, err := top_detect_svc.xdcr_topology_svc.MyHostAddr()
	if err != nil {
		myHostName = "UnknownHostName"
	}

	msg := fmt.Sprintf("%s %s %s: Topology change detected. Estimated time of pipeline restart: %s",
		myHostName, top_detect_svc.mainPipelineTopic, srcOrTgtStr, timeToUse.String())

	outputMap[key] = msg

	top_detect_svc.pipelinesMtx.Lock()
	defer top_detect_svc.pipelinesMtx.Unlock()
	if len(top_detect_svc.pipelines) > 0 {
		return top_detect_svc.pipelines[0].UpdateSettings(outputMap)
	}

	// It is possible for this to be called during pipeline restart/shutdown already and things are detached
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) handleTargetTopologyChange(diff_vb_list []uint16, target_vb_server_map map[uint16]string, err_in error) error {

	var err error
	// first check if relevant problematic vbs in pipeline are due to target topology changes.
	// the if conditions are to ensure that diff_vb_list is valid
	if err_in == nil || err_in == target_topology_changedErr {
		err = top_detect_svc.validateVbErrors(diff_vb_list, false /*source*/)
		if err != nil {
			return err
		}
	}

	if err_in == target_topology_changedErr || top_detect_svc.target_topology_change_count > 0 {
		top_detect_svc.target_topology_change_count++
		if top_detect_svc.target_topology_change_count == 1 {
			secsForMaxTopologyChange := int32(base.TopologyChangeCheckInterval.Seconds()) * int32(base.MaxTopologyChangeCountBeforeRestart)
			top_detect_svc.targetTopologyMaxWaitTime = time.Now().Add(time.Duration(secsForMaxTopologyChange) * time.Second)
			top_detect_svc.needToPostTargetTopologyEvent = true
		}
		top_detect_svc.logger.Infof("Number of target topology changes seen by pipeline is %v\n", top_detect_svc.target_topology_change_count)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.target_topology_change_count >= base.MaxTopologyChangeCountBeforeRestart {
			var targetTopoChangeRestartString = "Restarting pipeline due to target topology change..."
			top_detect_svc.logger.Warnf("%v", targetTopoChangeRestartString)
			err = errors.New(targetTopoChangeRestartString)
			top_detect_svc.restartPipeline(err)
			return err
		}

		if target_vb_server_map != nil {
			if base.AreVBServerMapsTheSame(top_detect_svc.target_vb_server_map_last, target_vb_server_map) {
				top_detect_svc.target_topology_stable_count++
				if top_detect_svc.target_topology_stable_count == 1 {
					secsForMaxStableTopologyChange := int32(base.TopologyChangeCheckInterval.Seconds()) * int32(base.MaxTopologyStableCountBeforeRestart)
					top_detect_svc.targetTopologyMaxStableWaitTime = time.Now().Add(time.Duration(secsForMaxStableTopologyChange) * time.Second)
					top_detect_svc.needToPostTargetTopologyEvent = true
				}
				top_detect_svc.logger.Infof("Number of stable target topology seen by pipeline is %v\n", top_detect_svc.target_topology_stable_count)
				if top_detect_svc.target_topology_stable_count >= base.MaxTopologyStableCountBeforeRestart {
					// restart pipeline if target topology change has stopped for a while and is assumbly completed
					err = fmt.Errorf("Target topology change for pipeline %v seems to have completed.", top_detect_svc.mainPipelineTopic)
					top_detect_svc.restartPipeline(err)
					return err
				}
			} else {
				top_detect_svc.target_topology_stable_count = 0
			}

			// otherwise, keep pipeline running for now.
			top_detect_svc.target_vb_server_map_last = target_vb_server_map
		}
	}

	if top_detect_svc.needToPostTargetTopologyEvent {
		var times []time.Time
		if top_detect_svc.targetTopologyMaxWaitTime.After(time.Now()) {
			times = append(times, top_detect_svc.targetTopologyMaxWaitTime)
		}
		if top_detect_svc.targetTopologyMaxStableWaitTime.After(time.Now()) {
			times = append(times, top_detect_svc.targetTopologyMaxStableWaitTime)
		}
		err = top_detect_svc.postTopologyEstTime(false, times)
		if err != nil {
			top_detect_svc.logger.Errorf("Unable to post estimated times for target: %v", err)
		} else {
			top_detect_svc.needToPostTargetTopologyEvent = false
		}
	}

	top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline handleTargetTopologyChange completed")
	return nil
}

// check if problematic vbs seen have been caused by source or target topology changes described by diff_vb_list
// if not, pipeline needs to be restarted right away
func (top_detect_svc *TopologyChangeDetectorSvc) validateVbErrors(diff_vb_list []uint16, source bool) error {
	var settings metadata.ReplicationSettingsMap
	top_detect_svc.pipelinesMtx.RLock()
	pipelineStillExist := len(top_detect_svc.pipelines) > 0
	if pipelineStillExist {
		settings = top_detect_svc.pipelines[0].Settings()
	}
	top_detect_svc.pipelinesMtx.RUnlock()

	if !pipelineStillExist {
		return errPipelinesDetached
	}

	var problematic_vb_key string
	if source {
		problematic_vb_key = base.ProblematicVBSource
	} else {
		problematic_vb_key = base.ProblematicVBTarget
	}
	vb_err_map_obj := settings[problematic_vb_key].(*base.ObjectWithLock)
	vb_err_map_obj.Lock.RLock()
	defer vb_err_map_obj.Lock.RUnlock()
	vb_err_map := vb_err_map_obj.Object.(map[uint16]error)

	for vbno, vb_err := range vb_err_map {
		_, found := base.SearchVBInSortedList(vbno, diff_vb_list)
		if !found {
			top_detect_svc.logger.Errorf("Vbucket %v for pipeline saw an error, %v, that had not been caused by topology changes. diff_vb_list=%v", vbno, vb_err, diff_vb_list)
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, vb_err))
			return vb_err
		}
	}

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) pipelineStillExists() (*metadata.ReplicationSpecification, bool) {
	var mainPipelineSpec *metadata.ReplicationSpecification
	top_detect_svc.pipelinesMtx.RLock()
	pipelineStillExists := len(top_detect_svc.pipelines) > 0
	if pipelineStillExists {
		mainPipelineSpec = top_detect_svc.pipelines[0].Specification().GetReplicationSpec()
	}
	top_detect_svc.pipelinesMtx.RUnlock()
	return mainPipelineSpec, pipelineStillExists
}

func (top_detect_svc *TopologyChangeDetectorSvc) DelReplicationSpec(spec *metadata.ReplicationSpecification, reason string) error {
	logMessage := fmt.Sprintf("Deleting replication spec %v since %v.\n", spec.Id, reason)
	top_detect_svc.logger.Info(logMessage)
	// provide the reason for replication spec deletion, which will be shown on UI
	_, err1 := top_detect_svc.repl_spec_svc.DelReplicationSpecWithReason(spec.Id, reason)
	if err1 != nil {
		top_detect_svc.logger.Errorf("Error deleting replication spec %v. err=%v\n", spec.Id, err1)
	}
	return fmt.Errorf(logMessage)
}

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}

// restart pipeline to handle topology change
func (top_detect_svc *TopologyChangeDetectorSvc) restartPipeline(err error) {
	top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
}

func (top_detect_svc *TopologyChangeDetectorSvc) IsSharable() bool {
	// Multiple pipelines should share the same topology
	return true
}

func (top_detect_svc *TopologyChangeDetectorSvc) Detach(pipeline common.Pipeline) error {
	top_detect_svc.pipelinesMtx.Lock()
	defer top_detect_svc.pipelinesMtx.Unlock()

	var idxToDel int = -1

	for i, attachedP := range top_detect_svc.pipelines {
		if pipeline.FullTopic() == attachedP.FullTopic() {
			top_detect_svc.logger.Infof("Detaching")
			idxToDel = i
			break
		}
	}

	if idxToDel == -1 {
		return base.ErrorNotFound
	}

	if top_detect_svc.detachCbs[idxToDel] != nil {
		top_detect_svc.detachCbs[idxToDel]()
	}

	top_detect_svc.pipelines = append(top_detect_svc.pipelines[:idxToDel], top_detect_svc.pipelines[idxToDel+1:]...)
	top_detect_svc.detachCbs = append(top_detect_svc.detachCbs[:idxToDel], top_detect_svc.detachCbs[idxToDel+1:]...)
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) monitorSource(initWg *sync.WaitGroup, initErr *error) {
	var replicationSpec *metadata.ReplicationSpecification
	top_detect_svc.pipelinesMtx.RLock()
	mainPipeline := top_detect_svc.pipelines[0]
	top_detect_svc.pipelinesMtx.RUnlock()
	genSpec := mainPipeline.Specification()
	if genSpec == nil {
		*initErr = fmt.Errorf("mainPipeline has no spec")
		initWg.Done()
		return
	} else {
		replicationSpec = genSpec.GetReplicationSpec()
	}
	sourceVbUpdateCh, subscribeErr := top_detect_svc.bucketTopologySvc.SubscribeToLocalBucketFeed(replicationSpec, top_detect_svc.bucketTopSubscriberId)
	if subscribeErr != nil {
		*initErr = subscribeErr
		initWg.Done()
		return
	}
	initWg.Done()

	//initialize source vb list to set up a baseline for source topology change detection
	top_detect_svc.vblist_original = pipeline_utils.GetSourceVBListPerPipeline(mainPipeline)
	base.SortUint16List(top_detect_svc.vblist_original)
	close(top_detect_svc.vblistOriginalInitDone)

	go func() {
		for {
			select {
			case <-top_detect_svc.finish_ch:
				err := top_detect_svc.bucketTopologySvc.UnSubscribeLocalBucketFeed(replicationSpec, top_detect_svc.bucketTopSubscriberId)
				if err != nil {
					top_detect_svc.logger.Warnf("Unsubscribing local bucket feed for %v resulted in %v", mainPipeline.InstanceId(), err)
				}
				top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline received finish signal and is exiting")
				return
			case notification := <-sourceVbUpdateCh:
				var updateOnceErr error
				if top_detect_svc.pipelineHasStopped() {
					notification.Recycle()
					err := top_detect_svc.bucketTopologySvc.UnSubscribeLocalBucketFeed(replicationSpec, top_detect_svc.bucketTopSubscriberId)
					if err != nil {
						top_detect_svc.logger.Warnf("Unsubscribing local bucket feed for %v resulted in %v", mainPipeline.InstanceId(), err)
					}
					return
				}
				kv_vb_map := notification.GetSourceVBMapRO()
				if updateOnceErr != nil {
					top_detect_svc.logger.Errorf("Unable to get KV VB Map - %v", updateOnceErr)
					notification.Recycle()
					continue
				}

				number_of_source_nodes := notification.GetNumberOfSourceNodes()
				if updateOnceErr != nil {
					top_detect_svc.logger.Errorf("Unable to get number of source nodes - %v", updateOnceErr)
					notification.Recycle()
					continue
				}
				vblist_supposed := []uint16{}
				for _, vblist := range kv_vb_map {
					vblist_supposed = append(vblist_supposed, vblist...)
				}
				base.SortUint16List(vblist_supposed)

				if !base.AreSortedUint16ListsTheSame(top_detect_svc.vblist_original, vblist_supposed) {
					top_detect_svc.logger.Infof("Source topology has changed for pipeline\n")
					top_detect_svc.logger.Infof("vblist_supposed=%v, vblist_now=%v\n", vblist_supposed, top_detect_svc.vblist_original)
					updateOnceErr = source_topology_changedErr
				}

				err := top_detect_svc.handleSourceTopologyChange(vblist_supposed, number_of_source_nodes, updateOnceErr)
				if err != nil {
					if err == errPipelinesDetached {
						return
					}
					top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc received error when handling source topology change. err=%v", err)
				}
				newEnableCrossClusterVersioning := notification.GetEnableCrossClusterVersioning()
				if top_detect_svc.enableCrossClusterVersioning != newEnableCrossClusterVersioning {
					err = fmt.Errorf("enableCrossClusterVersioning for source bucket has changed from %v to %v. Restarting pipeline", top_detect_svc.enableCrossClusterVersioning, newEnableCrossClusterVersioning)
					top_detect_svc.restartPipeline(err)
				}
				newVersionPruningWindow := notification.GetVersionPruningWindowHrs()
				if top_detect_svc.versionPruningWindow != newVersionPruningWindow {
					err = fmt.Errorf("versionPruningWindowHrs for source bucket has changed from %v to %v. Restarting pipeline", top_detect_svc.versionPruningWindow, newVersionPruningWindow)
					top_detect_svc.restartPipeline(err)
				}
				notification.Recycle()
			}
		}
	}()
	return
}

func (top_detect_svc *TopologyChangeDetectorSvc) monitorTarget(initWg *sync.WaitGroup, initErr *error) {
	var spec *metadata.ReplicationSpecification
	top_detect_svc.pipelinesMtx.RLock()
	mainPipeline := top_detect_svc.pipelines[0]
	top_detect_svc.pipelinesMtx.RUnlock()
	genSpec := mainPipeline.Specification()
	if genSpec == nil {
		*initErr = fmt.Errorf("main pipeline has no spec")
		initWg.Done()
		return
	} else {
		spec = genSpec.GetReplicationSpec()
	}
	targetVbUpdateCh, subscribeErr := top_detect_svc.bucketTopologySvc.SubscribeToRemoteBucketFeed(spec, top_detect_svc.bucketTopSubscriberId)
	if subscribeErr != nil {
		*initErr = subscribeErr
		initWg.Done()
		return
	}
	initWg.Done()

	// Init with initial info
	firstNotification := <-targetVbUpdateCh
	target_server_vb_map := firstNotification.GetTargetServerVBMap()

	// Wait for vblistOriginal to be done first
	// monitorSource must have had occurred first already
	<-top_detect_svc.vblistOriginalInitDone
	top_detect_svc.target_vb_server_map_original = base.ConstructVbServerMap(top_detect_svc.vblist_original, target_server_vb_map)

	go func() {
		for {
			select {
			case <-top_detect_svc.finish_ch:
				err := top_detect_svc.bucketTopologySvc.UnSubscribeRemoteBucketFeed(spec, top_detect_svc.bucketTopSubscriberId)
				if err != nil {
					top_detect_svc.logger.Warnf("Unsubscribing remote bucket feed for %v resulted in %v", mainPipeline.InstanceId(), err)
				}
				top_detect_svc.logger.Infof("TopologyChangeDetectorSvc validateTargetTopology completed")
				return
			case notification := <-targetVbUpdateCh:
				if top_detect_svc.pipelineHasStopped() {
					notification.Recycle()
					err := top_detect_svc.bucketTopologySvc.UnSubscribeRemoteBucketFeed(spec, top_detect_svc.bucketTopSubscriberId)
					if err != nil {
						top_detect_svc.logger.Warnf("Unsubscribing remote bucket feed for %v resulted in %v", mainPipeline.InstanceId(), err)
					}
					return
				}
				targetBucketUUID := notification.GetTargetBucketUUID()

				// validate target bucket uuid
				if spec.TargetBucketUUID != "" && targetBucketUUID != "" && spec.TargetBucketUUID != targetBucketUUID {
					shouldTryAgain := top_detect_svc.validateTargetBucketUUIDDifferences(spec)
					if shouldTryAgain {
						notification.Recycle()
						continue
					}
				}

				targetServerVBMap := notification.GetTargetServerVBMap()
				// check for target topology changes
				target_vb_server_map := base.ConstructVbServerMap(top_detect_svc.vblist_original, targetServerVBMap)

				diff_vb_list := base.GetDiffVBList(top_detect_svc.vblist_original, top_detect_svc.target_vb_server_map_original, target_vb_server_map)
				var errToHandleTargetChange error
				if len(diff_vb_list) > 0 {
					errToHandleTargetChange = target_topology_changedErr
					top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc received error when validating target topology change. err=%v", errToHandleTargetChange)
				}
				err := top_detect_svc.handleTargetTopologyChange(diff_vb_list, target_vb_server_map, errToHandleTargetChange)
				notification.Recycle()
				if err != nil {
					if err == errPipelinesDetached {
						return
					}
					top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc received error when handling target topology change. err=%v", err)
				}
			}
		}
	}()
	return
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetBucketUUIDDifferences(spec *metadata.ReplicationSpecification) (shouldTryAgain bool) {
	/*When target bucket uuid does not match, there are two possibilities:
	4. target node has been moved to a different cluster, which happens to have bucket with the same name
	5. target bucket has been deleted and re-created. in this case we need to delete repl spec
	We need to make an additional call to retrieve target cluster uuid to differentiate between these two cases
	D. if the call returns a different cluster uuid as that in repl spec, it is case #4
	E. if the call returns the same cluster uuid as that in repl spec, it is case #5
	F. if the call returns error, we have to play safe and skip the current target bucket check */
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil {
		shouldTryAgain = true
		return
	}
	connStr, err := top_detect_svc.remote_cluster_svc.GetConnectionStringForRemoteCluster(targetClusterRef, top_detect_svc.capi)
	if err != nil {
		shouldTryAgain = true
		return
	}
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := targetClusterRef.MyCredentials()
	if err != nil {
		shouldTryAgain = true
		return
	}
	curTargetClusterUUID, err := top_detect_svc.utils.GetClusterUUID(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, top_detect_svc.logger)
	if err != nil {
		// target node not accessible, skip target check
		logMessage := fmt.Sprintf("%v skipping target bucket check since %v is not accessible. err=%v\n", spec.Id, connStr, err)
		top_detect_svc.logger.Warn(logMessage)
		shouldTryAgain = true
		return
	}
	if curTargetClusterUUID != spec.TargetClusterUUID {
		// case 4, target node has been moved to a different cluster. skip target check
		logMessage := fmt.Sprintf("%v skipping target bucket check since %v has been moved to a different cluster %v.\n", spec.Id, connStr, curTargetClusterUUID)
		top_detect_svc.logger.Warn(logMessage)
		shouldTryAgain = true
		return
	}
	// if we get here, it is case 5, delete repl spec
	reason := fmt.Sprintf("the target bucket \"%v\" has been deleted and recreated", spec.TargetBucketName)
	err = top_detect_svc.DelReplicationSpec(spec, reason)
	return
}
