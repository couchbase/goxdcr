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
var target_cluster_version_changed_for_ssl_err = errors.New("Target cluster version has moved to 3.0 or above and started to support ssl over mem.")
var target_cluster_version_changed_for_extmeta_err = errors.New("Target cluster version has moved to 4.0 or above and started to support extended metadata.")

// the maximum number of consecutive topology changes seen before pipeline is restarted
var max_topology_changes_before_restart = 3

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
	// the number of consecutive topology changes seen so far
	num_topology_changes int
	// list of vbs managed by the current node in the last topology change check time
	vblist_supposed_last []uint16
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	logger_ctx *log.LoggerContext) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("ToplogyChangeDetector", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:     cluster_info_svc,
		remote_cluster_svc:   remote_cluster_svc,
		AbstractComponent:    comp.NewAbstractComponentWithLogger("ToplogyChangeDetector", logger),
		pipeline:             nil,
		finish_ch:            make(chan bool, 1),
		wait_grp:             &sync.WaitGroup{},
		logger:               logger,
		vblist_supposed_last: make([]uint16, 0)}
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

	checkTargetVersionForSSL, needToReComputeCheckTargetVersionForSSL := top_detect_svc.needCheckTargetForSSL()
	top_detect_svc.logger.Infof("checkTargetVersionForSSL=%v, needToReComputeCheckTargetVersionForSSL=%v in ToplogyChangeDetectorSvc for pipeline %v", checkTargetVersionForSSL, needToReComputeCheckTargetVersionForSSL, top_detect_svc.pipeline.Topic())
	checkTargetVersionForExtMeta, needToReComputeCheckTargetVersionForExtMeta := top_detect_svc.needCheckTargetForExtMeta()
	top_detect_svc.logger.Infof("checkTargetVersionForExtMeta=%v, needToReComputeCheckTargetVersionForExtMeta=%v in ToplogyChangeDetectorSvc for pipeline %v", checkTargetVersionForExtMeta, needToReComputeCheckTargetVersionForExtMeta, top_detect_svc.pipeline.Topic())
	//run it once right at the beginning
	top_detect_svc.validate(checkTargetVersionForSSL, checkTargetVersionForExtMeta)
	ticker := time.NewTicker(10 * time.Second)
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
			if needToReComputeCheckTargetVersionForSSL {
				checkTargetVersionForSSL, needToReComputeCheckTargetVersionForSSL := top_detect_svc.needCheckTargetForSSL()
				top_detect_svc.logger.Infof("After re-computation, checkTargetVersionForSSL=%v, needToReComputeCheckTargetVersionForSSL=%v in ToplogyChangeDetectorSvc for pipeline %v", checkTargetVersionForSSL, needToReComputeCheckTargetVersionForSSL, top_detect_svc.pipeline.Topic())
			}
			if needToReComputeCheckTargetVersionForExtMeta {
				checkTargetVersionForExtMeta, needToReComputeCheckTargetVersionForExtMeta := top_detect_svc.needCheckTargetForExtMeta()
				top_detect_svc.logger.Infof("After re-computation, checkTargetVersionForExtMeta=%v, needToReComputeCheckTargetVersionForExtMeta=%v in ToplogyChangeDetectorSvc for pipeline %v", checkTargetVersionForExtMeta, needToReComputeCheckTargetVersionForExtMeta, top_detect_svc.pipeline.Topic())
			}
			top_detect_svc.validate(checkTargetVersionForSSL, checkTargetVersionForExtMeta)
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validate(checkTargetVersionForSSL bool, checkTargetVersionForExtMeta bool) {
	vblist_now, vblist_supposed, err := top_detect_svc.validateSourceTopology()
	if err == nil || err == source_topology_changedErr {
		err = top_detect_svc.handleSourceToplogyChange(vblist_now, vblist_supposed, err)
		if err != nil {
			return
		}
	}
	// ignore other errors

	if checkTargetVersionForSSL {
		err = top_detect_svc.validateTargetVersionForSSL()
		if err != nil {
			if err == target_cluster_version_changed_for_ssl_err {
				top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
			} else {
				top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error=%v when validating target version for ssl", top_detect_svc.pipeline.Topic(), err)
			}
		}
	}

	if checkTargetVersionForExtMeta {
		err = top_detect_svc.validateTargetVersionForExtMeta()
		if err != nil {
			if err == target_cluster_version_changed_for_extmeta_err {
				top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
			} else {
				top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error=%v when validating target version for extended metadata", top_detect_svc.pipeline.Topic(), err)
			}
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) handleSourceToplogyChange(vblist_now, vblist_supposed []uint16, err_in error) error {
	vblist_removed, vblist_new := simple_utils.ComputeDeltaOfUint16Lists(vblist_now, vblist_supposed, false)
	if len(vblist_removed) > 0 || len(vblist_new) > 0 {
		top_detect_svc.logger.Infof("Source topology changed for pipeline %v: vblist_removed=%v, vblist_new=%v\n", top_detect_svc.pipeline.Topic(), vblist_removed, vblist_new)
	}

	// first check if all the problematic vbs in pipeline are due to source topology changes. If not, restart pipeline
	settings := top_detect_svc.pipeline.Settings()
	vb_err_map := settings[base.ProblematicVBs].(map[uint16]error)
	var err error
	for vbno, vb_err := range vb_err_map {
		_, found := simple_utils.SearchVBInSortedList(vbno, vblist_removed)
		if !found {
			top_detect_svc.logger.Errorf("Error processing vb %v for pipeline %v. err=%v.", vbno, top_detect_svc.pipeline.Topic(), vb_err)
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, vb_err))
			return err
		}
	}

	if err_in == source_topology_changedErr {
		top_detect_svc.num_topology_changes++
		top_detect_svc.logger.Infof("Number of consecutive topology changes seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.num_topology_changes)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.num_topology_changes >= max_topology_changes_before_restart {
			err = fmt.Errorf("Timeout waiting for topology changes to complete for pipeline %v.", top_detect_svc.pipeline.Topic())
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
			return err
		}

		// restart pipeline if
		//	1. there has been topology changes
		//  and 2. there has been no topology change between the last topology check time and now
		//  assumbly topology change has completed and there is no need to wait further
		if simple_utils.AreSortedUint16ListsTheSame(top_detect_svc.vblist_supposed_last, vblist_supposed) {
			err = fmt.Errorf("Source topology change for pipeline %v seems to have completed.", top_detect_svc.pipeline.Topic())
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
			return err
		}

		// otherwise, keep pipeline running for now.
		top_detect_svc.vblist_supposed_last = vblist_supposed
	}

	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetVersionForSSL() error {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil {
		var hasSSLOverMemSupport bool
		hasSSLOverMemSupport, err = pipeline_utils.HasSSLOverMemSupport(top_detect_svc.cluster_info_svc, targetClusterRef)
		if err == nil && hasSSLOverMemSupport {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v detected that remote cluster %v has been upgraded to 3.0 or above and is now supporting ssl over memcached", top_detect_svc.pipeline.Topic(), targetClusterRef.HostName)
			err = target_cluster_version_changed_for_ssl_err
		}
	}
	return err
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetVersionForExtMeta() error {
	var err error
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil {
		var extMetaSupportedByTarget bool
		extMetaSupportedByTarget, err = pipeline_utils.HasExtMetadataSupport(top_detect_svc.cluster_info_svc, targetClusterRef)
		if err == nil && extMetaSupportedByTarget {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v detected that remote cluster %v has been upgraded to 4.0 or above and is now supporting extended metadata", top_detect_svc.pipeline.Topic(), targetClusterRef.HostName)
			err = target_cluster_version_changed_for_extmeta_err
		}
	}
	return err
}

// returns two bools
// 1. First bool indicates whether target version needs to be checked
// 2. second bool indicates whether the first bool needs to be recomputed at the next check
func (top_detect_svc *TopologyChangeDetectorSvc) needCheckTargetForSSL() (bool, bool) {
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

func (top_detect_svc *TopologyChangeDetectorSvc) needCheckTargetForExtMeta() (bool, bool) {
	if pipeline_utils.IsPipelineUsingCapi(top_detect_svc.pipeline) {
		// ext metadata is never needed in capi mode
		return false, false
	}
	var err error
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil {
		var extMetaSupportedByTarget bool
		extMetaSupportedByTarget, err = pipeline_utils.HasExtMetadataSupport(top_detect_svc.cluster_info_svc, targetClusterRef)
		if err == nil {
			return !extMetaSupportedByTarget, false
		}
	}

	// if we get here, we do not really know whether target version needs to be checked
	// set needCheckTarget to false for now and specify that it needs to be re-computed later
	return false, true
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() ([]uint16, []uint16, error) {
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateSourceTopology...", top_detect_svc.pipeline.Topic())
	vblist_now := pipeline_utils.GetSourceVBListPerPipeline(top_detect_svc.pipeline)

	vblist_supposed := []uint16{}
	kv_vb_map, err := pipeline_utils.GetSourceVBMap(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification().SourceBucketName, top_detect_svc.logger)
	if err != nil {
		return nil, nil, err
	}

	for _, vblist := range kv_vb_map {
		vblist_supposed = append(vblist_supposed, vblist...)
	}

	simple_utils.SortUint16List(vblist_now)
	simple_utils.SortUint16List(vblist_supposed)

	if !simple_utils.AreSortedUint16ListsTheSame(vblist_now, vblist_supposed) {
		top_detect_svc.logger.Infof("Source topology has changed for pipeline %v\n", top_detect_svc.pipeline.Topic())
		top_detect_svc.logger.Debugf("Pipeline %v - vblist_supposed=%v, vblist_now=%v\n", top_detect_svc.pipeline.Topic(), vblist_supposed, vblist_now)
		return vblist_now, vblist_supposed, source_topology_changedErr
	}

	return nil, nil, nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings map[string]interface{}) error {
	return nil
}
