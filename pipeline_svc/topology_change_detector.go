package pipeline_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"sync"
	"time"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")
var target_topology_changedErr = errors.New("Topology has changed on target cluster")
var target_cluster_versionChangeErr = errors.New("Target cluster version has moved to 3.0 or above")

type TopologyChangeDetectorSvc struct {
	*comp.AbstractComponent

	//xdcr topology service
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	cluster_info_svc   service_def.ClusterInfoSvc
	remote_cluster_svc service_def.RemoteClusterSvc
	logger             *log.CommonLogger
	pipeline           common.Pipeline
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
	vblist_original []uint16
	// list of vbs managed by the current node in the last topology change check time
	// used for source topology change detection
	vblist_last []uint16
	// vb server map of target bucket when pipeline was first started
	// used for target topology change detection
	target_vb_server_map_original map[uint16]string
	// vb server map of target bucket in the last topology change check time
	// used for target topology change detection
	target_vb_server_map_last map[uint16]string
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	logger_ctx *log.LoggerContext) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("ToplogyChangeDetector", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:   cluster_info_svc,
		remote_cluster_svc: remote_cluster_svc,
		AbstractComponent:  comp.NewAbstractComponentWithLogger("ToplogyChangeDetector", logger),
		pipeline:           nil,
		finish_ch:          make(chan bool, 1),
		wait_grp:           &sync.WaitGroup{},
		logger:             logger,
		vblist_last:        make([]uint16, 0)}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipeline = pipeline
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Start(map[string]interface{}) error {
	//register itself with pipeline supervisor
	supervisor := top_detect_svc.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return fmt.Errorf("Error starting ToplogyChangeDetectorSvc for pipeline %v since pipeline supervisor does not exist", top_detect_svc.pipeline.Topic())
	}
	err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}

	//initialize source vb list to set up a baseline for source topology change detection
	top_detect_svc.vblist_original = pipeline_utils.GetSourceVBListPerPipeline(top_detect_svc.pipeline)
	simple_utils.SortUint16List(top_detect_svc.vblist_original)

	//initialize target vb server map to set up a baseline for target topology change detection
	top_detect_svc.target_vb_server_map_original, err = top_detect_svc.getTargetVBServerMap()
	if err != nil {
		return err
	}

	top_detect_svc.wait_grp.Add(1)

	go top_detect_svc.watch(top_detect_svc.finish_ch, top_detect_svc.wait_grp)

	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v has started", top_detect_svc.pipeline.Topic())
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Stop() error {
	close(top_detect_svc.finish_ch)
	top_detect_svc.wait_grp.Wait()
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v has stopped", top_detect_svc.pipeline.Topic())
	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) watch(fin_ch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	checkingTargetVersion, needToReComputeCheckingTargetVersion := top_detect_svc.needCheckTarget()
	top_detect_svc.logger.Infof("checkingTargetVersion=%v, needToReComputeCheckingTargetVersion=%v in ToplogyChangeDetectorSvc for pipeline %v", checkingTargetVersion, needToReComputeCheckingTargetVersion, top_detect_svc.pipeline.Topic())
	//run it once right at the beginning
	top_detect_svc.validate(checkingTargetVersion)
	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fin_ch:
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received finish signal and is exitting", top_detect_svc.pipeline.Topic())
			return
		case <-ticker.C:
			if top_detect_svc.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				top_detect_svc.logger.Infof("Pipeline %v is no longer running. ToplogyChangeDetectorSvc is exitting.", top_detect_svc.pipeline.Topic())
				return
			}
			if needToReComputeCheckingTargetVersion {
				checkingTargetVersion, needToReComputeCheckingTargetVersion = top_detect_svc.needCheckTarget()
				top_detect_svc.logger.Infof("After re-computation, checkingTargetVersion=%v, needToReComputeCheckingTargetVersion=%v in ToplogyChangeDetectorSvc for pipeline %v", checkingTargetVersion, needToReComputeCheckingTargetVersion, top_detect_svc.pipeline.Topic())
			}
			top_detect_svc.validate(checkingTargetVersion)
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validate(checkingTargetVersion bool) {
	vblist_supposed, err := top_detect_svc.validateSourceTopology()

	if err == nil || err == source_topology_changedErr {
		err = top_detect_svc.handleSourceToplogyChange(vblist_supposed, err)
	}

	if err != nil {
		top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error when validating or handling source topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
	}

	diff_vb_list, target_vb_server_map, err := top_detect_svc.validateTargetTopology()
	if err == nil || err == target_topology_changedErr {
		err = top_detect_svc.handleTargetToplogyChange(diff_vb_list, target_vb_server_map, err)
	}

	if err != nil {
		top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error when validating or handling target topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
	}

	if checkingTargetVersion {
		err = top_detect_svc.validateTargetVersion()
		if err != nil {
			if err == target_cluster_versionChangeErr {
				top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
			} else {
				top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error=%v when validating target version", top_detect_svc.pipeline.Topic(), err)
			}
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) handleSourceToplogyChange(vblist_supposed []uint16, err_in error) error {
	vblist_removed, vblist_new := simple_utils.ComputeDeltaOfUint16Lists(top_detect_svc.vblist_original, vblist_supposed, false)
	if len(vblist_removed) > 0 || len(vblist_new) > 0 {
		top_detect_svc.logger.Infof("Source topology changed for pipeline %v: vblist_removed=%v, vblist_new=%v\n", top_detect_svc.pipeline.Topic(), vblist_removed, vblist_new)
	}

	// first check if relevant problematic vbs in pipeline are due to source topology changes.
	err := top_detect_svc.validateVbErrors(vblist_removed, true /*source*/)
	if err != nil {
		return err
	}

	if err_in == source_topology_changedErr {
		top_detect_svc.source_topology_change_count++
		top_detect_svc.logger.Infof("Number of source topology changes seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.source_topology_change_count)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.source_topology_change_count >= base.MaxTopologyChangeCountBeforeRestart {
			err = fmt.Errorf("Timeout waiting for source topology changes to complete for pipeline %v.", top_detect_svc.pipeline.Topic())
			top_detect_svc.restartPipeline(err)
			return err
		}

		if simple_utils.AreSortedUint16ListsTheSame(top_detect_svc.vblist_last, vblist_supposed) {
			top_detect_svc.source_topology_stable_count++
			top_detect_svc.logger.Infof("Number of consecutive stable source topology seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.source_topology_stable_count)
			if top_detect_svc.source_topology_stable_count >= base.MaxTopologyStableCountBeforeRestart {
				// restart pipeline if source topology change has stopped for a while and is assumbly completed
				err = fmt.Errorf("Source topology change for pipeline %v seems to have completed.", top_detect_svc.pipeline.Topic())
				top_detect_svc.restartPipeline(err)
				return err
			}
		} else {
			top_detect_svc.source_topology_stable_count = 0
		}

		// otherwise, keep pipeline running for now.
		top_detect_svc.vblist_last = vblist_supposed
	}

	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) handleTargetToplogyChange(diff_vb_list []uint16, target_vb_server_map map[uint16]string, err_in error) error {
	// first check if relevant problematic vbs in pipeline are due to target topology changes.
	err := top_detect_svc.validateVbErrors(diff_vb_list, false /*source*/)
	if err != nil {
		return err
	}

	if err_in == target_topology_changedErr {
		top_detect_svc.target_topology_change_count++
		top_detect_svc.logger.Infof("Number of target topology changes seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.target_topology_change_count)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.target_topology_change_count >= base.MaxTopologyChangeCountBeforeRestart {
			err = fmt.Errorf("Timeout waiting for target topology changes to complete for pipeline %v.", top_detect_svc.pipeline.Topic())
			top_detect_svc.restartPipeline(err)
			return err
		}

		if simple_utils.AreVBServerMapsTheSame(top_detect_svc.target_vb_server_map_last, target_vb_server_map) {
			top_detect_svc.target_topology_stable_count++
			top_detect_svc.logger.Infof("Number of stable target topology seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.target_topology_stable_count)
			if top_detect_svc.target_topology_stable_count >= base.MaxTopologyStableCountBeforeRestart {
				// restart pipeline if target topology change has stopped for a while and is assumbly completed
				err = fmt.Errorf("Target topology change for pipeline %v seems to have completed.", top_detect_svc.pipeline.Topic())
				top_detect_svc.restartPipeline(err)
				return err
			}
		} else {
			top_detect_svc.target_topology_stable_count = 0
		}

		// otherwise, keep pipeline running for now.
		top_detect_svc.target_vb_server_map_last = target_vb_server_map
	}

	return nil

}

// check if problematic vbs seen have been caused by source or target topology changes described by diff_vb_list
// if not, pipeline needs to be restarted right away
func (top_detect_svc *TopologyChangeDetectorSvc) validateVbErrors(diff_vb_list []uint16, source bool) error {
	settings := top_detect_svc.pipeline.Settings()
	var problematic_vb_key string
	if source {
		problematic_vb_key = base.ProblematicVBSource
	} else {
		problematic_vb_key = base.ProblematicVBTarget
	}
	vb_err_map := settings[problematic_vb_key].(map[uint16]error)

	for vbno, vb_err := range vb_err_map {
		_, found := simple_utils.SearchVBInSortedList(vbno, diff_vb_list)
		if !found {
			top_detect_svc.logger.Errorf("Vbucket %v for pipeline %v saw an error, %v, that had not been caused by topology changes. diff_vb_list=%v", vbno, top_detect_svc.pipeline.Topic(), vb_err, diff_vb_list)
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, vb_err))
			return vb_err
		}
	}

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetVersion() error {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil {
		var hasSSLOverMemSupport bool
		hasSSLOverMemSupport, err = pipeline_utils.HasSSLOverMemSupport(top_detect_svc.cluster_info_svc, targetClusterRef)
		if err == nil && hasSSLOverMemSupport {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v detected that remote cluster %v has upgraded to 3.0 or above", top_detect_svc.pipeline.Topic(), targetClusterRef.HostName)
			err = target_cluster_versionChangeErr
		}
	}
	return err
}

// returns two bools
// 1. First bool indicates whether target version needs to be checked
// 2. second bool indicates whether the first bool needs to be recomputed at the next check
func (top_detect_svc *TopologyChangeDetectorSvc) needCheckTarget() (bool, bool) {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil && targetClusterRef.DemandEncryption {
		pipeline := top_detect_svc.pipeline
		targets := pipeline.Targets()
		for _, target := range targets {
			//only check the first target nozzle, then return
			//the assumption is all target nozzle should have the same type
			//- capi, xmem with MemConn, xmem with SSLOverMem or xmem with SSLOverProxy
			if _, ok := target.(*parts.XmemNozzle); !ok {
				return false, false
			}
			if target.State() == common.Part_Running {
				connType := target.(*parts.XmemNozzle).ConnType()
				if connType == base.SSLOverMem || connType == base.MemConn {
					return false, false
				} else {
					return true, false
				}
			}
		}

	}
	// if we get here, we do not really know whether target version needs to be checked
	// set needCheckTarget to false for now and specify that it needs to be re-computed later
	return false, true
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() ([]uint16, error) {
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateSourceTopology...", top_detect_svc.pipeline.Topic())

	vblist_supposed := []uint16{}
	kv_vb_map, err := pipeline_utils.GetSourceVBMapForReplication(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification(), top_detect_svc.logger)
	if err != nil {
		return nil, err
	}

	for _, vblist := range kv_vb_map {
		vblist_supposed = append(vblist_supposed, vblist...)
	}

	simple_utils.SortUint16List(vblist_supposed)

	if !simple_utils.AreSortedUint16ListsTheSame(top_detect_svc.vblist_original, vblist_supposed) {
		top_detect_svc.logger.Infof("Source topology has changed for pipeline %v\n", top_detect_svc.pipeline.Topic())
		top_detect_svc.logger.Debugf("Pipeline %v - vblist_supposed=%v, vblist_now=%v\n", top_detect_svc.pipeline.Topic(), vblist_supposed, top_detect_svc.vblist_original)
		return vblist_supposed, source_topology_changedErr
	}

	return vblist_supposed, nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetTopology() ([]uint16, map[uint16]string, error) {
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateTargetTopology...", top_detect_svc.pipeline.Topic())

	target_vb_server_map, err := top_detect_svc.getTargetVBServerMap()
	if err != nil {
		return nil, nil, err
	}

	diff_vb_list := simple_utils.GetDiffVBList(top_detect_svc.vblist_original, top_detect_svc.target_vb_server_map_original, target_vb_server_map)

	if len(diff_vb_list) > 0 {
		return diff_vb_list, target_vb_server_map, target_topology_changedErr
	} else {
		return diff_vb_list, target_vb_server_map, nil
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) getTargetVBServerMap() (map[uint16]string, error) {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		return nil, err
	}

	server_vb_map, err := top_detect_svc.cluster_info_svc.GetServerVBucketsMap(targetClusterRef, spec.TargetBucketName)
	if err != nil {
		return nil, err
	}

	vb_server_map := make(map[uint16]string)
	for server, vbList := range server_vb_map {
		for _, vb := range vbList {
			vb_server_map[vb] = server
		}
	}

	return vb_server_map, nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings map[string]interface{}) error {
	return nil
}

// restart pipeline to handle topology change
func (top_detect_svc *TopologyChangeDetectorSvc) restartPipeline(err error) {
	// get one checkpoint done before restarting pipeline
	top_detect_svc.performCheckpoint()
	top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
}

func (top_detect_svc *TopologyChangeDetectorSvc) performCheckpoint() {
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v starting checkpoint", top_detect_svc.pipeline.Topic())
	ckmgr := top_detect_svc.pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC).(*CheckpointManager)

	timeout_ticker := time.NewTicker(base.TopologyChangeCheckpointTimeout)
	defer timeout_ticker.Stop()

	ret := make(chan bool, 1)
	close_ch := make(chan bool, 1)

	go func(finch chan bool) {
		ckmgr.PerformCkpt(close_ch, 0)
		finch <- true
	}(ret)

	for {
		select {
		case <-ret:
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v completed checkpoint", top_detect_svc.pipeline.Topic())
			return
		case <-timeout_ticker.C:
			close(close_ch)
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v timed out after %v", top_detect_svc.pipeline.Topic(), base.TopologyChangeCheckpointTimeout)
			return
		}
	}
}
