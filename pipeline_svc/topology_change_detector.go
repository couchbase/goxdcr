package pipeline_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"sync"
	"time"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")
var target_topology_changedErr = errors.New("Topology has changed on target cluster")
var target_cluster_version_changed_for_ssl_err = errors.New("Target cluster version has moved to 3.0 or above and started to support ssl over mem.")
var target_cluster_version_changed_for_extmeta_err = errors.New("Target cluster version has moved to 4.0 or above and started to support extended metadata.")

type TopologyChangeDetectorSvc struct {
	*comp.AbstractComponent

	//xdcr topology service
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	cluster_info_svc   service_def.ClusterInfoSvc
	remote_cluster_svc service_def.RemoteClusterSvc
	repl_spec_svc      service_def.ReplicationSpecSvc
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

	// key = hostname; value = https address of hostname
	httpsAddrMap map[string]string

	// whether replication is of capi type
	capi bool
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	repl_spec_svc service_def.ReplicationSpecSvc,
	logger_ctx *log.LoggerContext) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("ToplogyChangeDetector", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:   cluster_info_svc,
		remote_cluster_svc: remote_cluster_svc,
		repl_spec_svc:      repl_spec_svc,
		AbstractComponent:  comp.NewAbstractComponentWithLogger("ToplogyChangeDetector", logger),
		pipeline:           nil,
		finish_ch:          make(chan bool, 1),
		wait_grp:           &sync.WaitGroup{},
		logger:             logger,
		vblist_last:        make([]uint16, 0),
		httpsAddrMap:       make(map[string]string)}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipeline = pipeline
	top_detect_svc.capi = pipeline.Specification().Settings.RepType == metadata.ReplicationTypeCapi
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
	_, target_server_vb_map, err := top_detect_svc.getTargetBucketInfo()
	if err != nil {
		return err
	}
	top_detect_svc.target_vb_server_map_original = make(map[uint16]string)
	for server, vbList := range target_server_vb_map {
		for _, vb := range vbList {
			top_detect_svc.target_vb_server_map_original[vb] = server
		}
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
	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fin_ch:
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received finish signal and is exitting", top_detect_svc.pipeline.Topic())
			return
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(top_detect_svc.pipeline.State()) {
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
	vblist_supposed, err := top_detect_svc.validateSourceTopology()
	if err == nil || err == source_topology_changedErr {
		err = top_detect_svc.handleSourceToplogyChange(vblist_supposed, err)
	}

	if err != nil {
		top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error when validating or handling source topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
	}

	diff_vb_list, target_vb_server_map, err := top_detect_svc.validateTargetTopology(checkTargetVersionForSSL)
	if err == target_cluster_version_changed_for_ssl_err {
		// restart pipeline if target begins to support ssl
		top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
	} else {
		if err != nil {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error when validating target topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
		}
		top_detect_svc.handleTargetToplogyChange(diff_vb_list, target_vb_server_map, err)
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

func (top_detect_svc *TopologyChangeDetectorSvc) handleSourceToplogyChange(vblist_supposed []uint16, err_in error) error {
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v handleSourceToplogyChange completed", top_detect_svc.pipeline.Topic())

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
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v handleTargetToplogyChange completed", top_detect_svc.pipeline.Topic())

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
		top_detect_svc.logger.Infof("Number of target topology changes seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.target_topology_change_count)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.target_topology_change_count >= base.MaxTopologyChangeCountBeforeRestart {
			err = fmt.Errorf("Timeout waiting for target topology changes to complete for pipeline %v.", top_detect_svc.pipeline.Topic())
			top_detect_svc.restartPipeline(err)
			return err
		}

		if target_vb_server_map != nil {
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
	vb_err_map_obj := settings[problematic_vb_key].(*base.ObjectWithLock)
	vb_err_map_obj.Lock.RLock()
	defer vb_err_map_obj.Lock.RUnlock()
	vb_err_map := vb_err_map_obj.Object.(map[uint16]error)

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

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetVersionForSSL() error {
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateTargetVersionForSSL completed", top_detect_svc.pipeline.Topic())

	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
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
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateTargetVersionForExtMeta completed", top_detect_svc.pipeline.Topic())

	var err error
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
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
	spec, _ := top_detect_svc.repl_spec_svc.ReplicationSpec(top_detect_svc.pipeline.Topic())
	if spec == nil {
		return false, true
	}
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err == nil {
		if !targetClusterRef.DemandEncryption {
			return false, false
		}
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
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
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

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() ([]uint16, error) {
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateSourceTopology completed", top_detect_svc.pipeline.Topic())

	vblist_supposed := []uint16{}
	kv_vb_map, err := pipeline_utils.GetSourceVBMap(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification().SourceBucketName, top_detect_svc.logger)
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

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetTopology(checkTargetVersionForSSL bool) ([]uint16, map[uint16]string, error) {
	defer top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateTargetTopology completed", top_detect_svc.pipeline.Topic())

	targetClusterCompatibility, targetServerVBMap, err := top_detect_svc.getTargetBucketInfo()

	if err != nil {
		top_detect_svc.logger.Infof("Skipping target check since received error retrieving target bucket info for %v. err=%v", top_detect_svc.pipeline.Topic(), err)
		return nil, nil, err
	}

	// check target version if needed
	if checkTargetVersionForSSL {
		if simple_utils.IsClusterCompatible(targetClusterCompatibility, base.VersionForSSLOverMemSupport) {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v detected that target cluster has been upgraded to 3.0 or above and is now supporting ssl over memcached", top_detect_svc.pipeline.Topic())
			return nil, nil, target_cluster_version_changed_for_ssl_err
		}
	}

	// check for target topology changes
	target_vb_server_map := make(map[uint16]string)
	for server, vbList := range targetServerVBMap {
		for _, vb := range vbList {
			target_vb_server_map[vb] = server
		}
	}

	diff_vb_list := simple_utils.GetDiffVBList(top_detect_svc.vblist_original, top_detect_svc.target_vb_server_map_original, target_vb_server_map)

	if len(diff_vb_list) > 0 {
		return diff_vb_list, target_vb_server_map, target_topology_changedErr
	} else {
		return diff_vb_list, target_vb_server_map, nil
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) getTargetBucketInfo() (int, map[string][]uint16, error) {
	spec, _ := top_detect_svc.repl_spec_svc.ReplicationSpec(top_detect_svc.pipeline.Topic())
	if spec == nil {
		return 0, nil, fmt.Errorf("Cannot find replication spec for %v", top_detect_svc.pipeline.Topic())
	}

	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil {
		return 0, nil, err
	}

	connStr, err := top_detect_svc.remote_cluster_svc.GetConnectionStringForRemoteCluster(targetClusterRef, top_detect_svc.capi)
	if err != nil {
		return 0, nil, err
	}

	username, password, certificate, sanInCertificate, err := targetClusterRef.MyCredentials()
	if err != nil {
		return 0, nil, err
	}

	bucketName := spec.TargetBucketName
	var targetBucketUUID string
	var targetClusterCompatibility int
	var targetServerVBMap map[string][]uint16
	allFieldsFound := false

	targetBucketInfo, err := utils.GetBucketInfo(connStr, bucketName, username, password, certificate, sanInCertificate, top_detect_svc.logger)

	if err == nil {
		targetBucketUUID, err = utils.GetBucketUuidFromBucketInfo(bucketName, targetBucketInfo, top_detect_svc.logger)
		if err == nil {
			targetServerVBMap, err = utils.GetServerVBucketsMap(connStr, bucketName, targetBucketInfo)
			if err == nil {
				if !top_detect_svc.capi {
					targetClusterCompatibility, err = utils.GetClusterCompatibilityFromBucketInfo(bucketName, targetBucketInfo, top_detect_svc.logger)
					if err == nil {
						allFieldsFound = true
					}
				} else {
					// do not try to retrieve cluster compatibility in capi mode, since target cluster may be elastic search cluster
					allFieldsFound = true
				}
			}
		}
	}

	if err != nil && err != utils.NonExistentBucketError {
		errMsg := fmt.Sprintf("Skipping target bucket check for spec %v since failed to get bucket infor for %v. err=%v", top_detect_svc.pipeline.Topic(), bucketName, err)
		top_detect_svc.logger.Info(errMsg)
		return 0, nil, errors.New(errMsg)
	}

	targetClusterUUIDChecked := false

	if err == utils.NonExistentBucketError {
		/* When we get NonExistentBucketError, there are three possibilities:
		1. the target node is not accessible, either because it has been removed from target cluster,
		   or because of temporary network issues. in this case we skip target check for the current round
		   hopefully things will work in the next target check round, when a different node will be used
		   or the same node will come back
		2. the target node has been moved to a different cluster, which does not have bucket with the same name.
		   in this case we skip target check for the current round
		3. the target bucket has been deleted. in this case we need to delete repl spec

		In order to differetiate between these cases, we first make a call to retrieve target cluster uuid
		A. if the call returns error, it is case #1
		B. if the call returns a different cluster uuid as that in repl spec, it is case #2
		C. if the call returns the same cluster uuid as that in repl spec, we need to make another call to retrieve
			bucket list from target
			C.1 if the call returns error, skip current target bucket check
			C.2 if the call returns a bucket list that does not contain target bucket, it is case #3. the repl spec needs to be deleted
			C.3 if the call returns a bucket list that contains target bucket, continue with target bucket validation
		*/
		curTargetClusterUUID, err := utils.GetClusterUUID(connStr, username, password, certificate, sanInCertificate, top_detect_svc.logger)
		if err != nil {
			// case 1, target node not accessible, skip target check
			logMessage := fmt.Sprintf("%v skipping target bucket check since %v is not accessible. err=%v\n", spec.Id, connStr, err)
			top_detect_svc.logger.Info(logMessage)
			return 0, nil, errors.New(logMessage)
		} else {
			if curTargetClusterUUID != spec.TargetClusterUUID {
				// case 2, target node has been moved to a different cluster. skip target check
				logMessage := fmt.Sprintf("%v skipping target bucket check since %v has been moved to a different cluster %v.\n", spec.Id, connStr, curTargetClusterUUID)
				top_detect_svc.logger.Info(logMessage)
				return 0, nil, errors.New(logMessage)
			}

			// set the flag to avoid unnecessary re-work in target bucket uuid validation
			targetClusterUUIDChecked = true

			//	additional check is needed
			buckets, err := utils.GetBuckets(connStr, username, password, certificate, sanInCertificate, top_detect_svc.logger)
			if err != nil {
				// case 1, target node not accessible, skip target check
				errMsg := fmt.Sprintf("Skipping target bucket check for spec %v since target node %v is not accessible. err=%v", spec.Id, connStr, err)
				top_detect_svc.logger.Info(errMsg)
				return 0, nil, errors.New(errMsg)
			}
			foundTargetBucket := false
			for bucketName, bucketUUID := range buckets {
				if bucketName == spec.TargetBucketName {
					foundTargetBucket = true
					targetBucketUUID = bucketUUID
					break
				}
			}
			if !foundTargetBucket {
				// case 3, delete repl spec
				reason := fmt.Sprintf("the target bucket \"%v\" has been deleted", spec.TargetBucketName)
				err = top_detect_svc.DelReplicationSpec(spec, reason)
				return 0, nil, err
			}
			// if target bucket is found, we have already populated targetBucketUUID accordingly
			// continue with target bucket validation
		}
	}

	// validate target bucket uuid
	if spec.TargetBucketUUID != "" && targetBucketUUID != "" && spec.TargetBucketUUID != targetBucketUUID {
		/*When target bucket uuid does not match, there are two possibilities:
		4. target node has been moved to a different cluster, which happens to have bucket with the same name
		5. target bucket has been deleted and re-created. in this case we need to delete repl spec
		We need to make an additional call to retrieve target cluster uuid, if it has not been done before, to differentiate between these two cases
		D. if the call returns a different cluster uuid as that in repl spec, it is case #4
		E. if the call returns the same cluster uuid as that in repl spec, it is case #5
		F. if the call returns error, we have to play safe and skip the current target bucket check */

		if targetClusterUUIDChecked {
			// if we have already verified that target cluster uuid has not changed, it has to be case 5
			reason := fmt.Sprintf("the target bucket \"%v\" has been deleted and recreated", spec.TargetBucketName)
			err = top_detect_svc.DelReplicationSpec(spec, reason)
			return 0, nil, err
		}

		//
		curTargetClusterUUID, err := utils.GetClusterUUID(connStr, username, password, certificate, sanInCertificate, top_detect_svc.logger)
		if err != nil {
			// target node not accessible, skip target check
			logMessage := fmt.Sprintf("%v skipping target bucket check since %v is not accessible. err=%v\n", spec.Id, connStr, err)
			top_detect_svc.logger.Info(logMessage)
			return 0, nil, errors.New(logMessage)
		}
		if curTargetClusterUUID != spec.TargetClusterUUID {
			// case 4, target node has been moved to a different cluster. skip target check
			logMessage := fmt.Sprintf("%v skipping target bucket check since %v has been moved to a different cluster %v.\n", spec.Id, connStr, curTargetClusterUUID)
			top_detect_svc.logger.Info(logMessage)
			return 0, nil, errors.New(logMessage)
		}

		// if we get here, it is case 5, delete repl spec
		reason := fmt.Sprintf("the target bucket \"%v\" has been deleted and recreated", spec.TargetBucketName)
		err = top_detect_svc.DelReplicationSpec(spec, reason)
		return 0, nil, err
	}

	if allFieldsFound {
		return targetClusterCompatibility, targetServerVBMap, nil
	} else {
		return 0, nil, fmt.Errorf("%v Error retrieving target cluster compatibility and target server map from the target bucket %v", spec.Id, spec.TargetBucketName)
	}
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

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings map[string]interface{}) error {
	return nil
}

// restart pipeline to handle topology change
func (top_detect_svc *TopologyChangeDetectorSvc) restartPipeline(err error) {
	top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
}
