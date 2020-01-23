package pipeline_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"sync"
	"time"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")
var target_topology_changedErr = errors.New("Topology has changed on target cluster")
var target_cluster_version_changed_for_rbac_and_xattr_err = errors.New("Target cluster version has moved to 5.0 or above and started to support rbac and xattr.")

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
	// number of nodes in source cluster
	number_of_source_nodes int

	// key = hostname; value = https address of hostname
	httpsAddrMap map[string]string

	// whether replication is of capi type
	capi bool

	// whether needs to check target version for RBAC and Xattr support
	check_target_version_for_rbac_and_xattr bool

	utils utilities.UtilsIface
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	repl_spec_svc service_def.ReplicationSpecSvc,
	target_has_rbac_and_xattr_support bool,
	logger_ctx *log.LoggerContext,
	utilsIn utilities.UtilsIface) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("TopoChangeDet", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:                        cluster_info_svc,
		remote_cluster_svc:                      remote_cluster_svc,
		repl_spec_svc:                           repl_spec_svc,
		AbstractComponent:                       comp.NewAbstractComponentWithLogger("TopoChangeDet", logger),
		pipeline:                                nil,
		finish_ch:                               make(chan bool, 1),
		wait_grp:                                &sync.WaitGroup{},
		logger:                                  logger,
		vblist_last:                             make([]uint16, 0),
		httpsAddrMap:                            make(map[string]string),
		check_target_version_for_rbac_and_xattr: !target_has_rbac_and_xattr_support,
		utils:                                   utilsIn,
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipeline = pipeline
	top_detect_svc.capi = pipeline.Specification().Settings.IsCapi()
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Start(metadata.ReplicationSettingsMap) error {
	//register itself with pipeline supervisor
	supervisor := top_detect_svc.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return fmt.Errorf("Error starting TopologyChangeDetectorSvc for pipeline %v since pipeline supervisor does not exist", top_detect_svc.pipeline.Topic())
	}
	err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}
	top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v Starting...", top_detect_svc.pipeline.Topic())

	//initialize source vb list to set up a baseline for source topology change detection
	top_detect_svc.vblist_original = pipeline_utils.GetSourceVBListPerPipeline(top_detect_svc.pipeline)
	base.SortUint16List(top_detect_svc.vblist_original)

	//initialize target vb server map to set up a baseline for target topology change detection
	_, target_server_vb_map, err := top_detect_svc.getTargetBucketInfo()
	if err != nil {
		return err
	}
	top_detect_svc.target_vb_server_map_original = base.ConstructVbServerMap(top_detect_svc.vblist_original, target_server_vb_map)

	top_detect_svc.number_of_source_nodes, err = top_detect_svc.xdcr_topology_svc.NumberOfKVNodes()
	if err != nil {
		return err
	}

	top_detect_svc.wait_grp.Add(1)

	go top_detect_svc.watch(top_detect_svc.finish_ch, top_detect_svc.wait_grp)

	top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v has started", top_detect_svc.pipeline.Topic())
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Stop() error {
	close(top_detect_svc.finish_ch)
	top_detect_svc.wait_grp.Wait()
	top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v has stopped", top_detect_svc.pipeline.Topic())
	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) watch(fin_ch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	//run it once right at the beginning
	top_detect_svc.validate()
	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fin_ch:
			top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v received finish signal and is exitting", top_detect_svc.pipeline.Topic())
			return
		case <-ticker.C:
			if !pipeline_utils.IsPipelineRunning(top_detect_svc.pipeline.State()) {
				//pipeline is no longer running, kill itself
				top_detect_svc.logger.Infof("Pipeline %v is no longer running. TopologyChangeDetectorSvc is exitting.", top_detect_svc.pipeline.Topic())
				return
			}
			top_detect_svc.validate()
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validate() {
	vblist_supposed, number_of_source_nodes, err := top_detect_svc.validateSourceTopology()
	if err != nil {
		top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc for pipeline %v received error when validating source topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
	}

	err = top_detect_svc.handleSourceTopologyChange(vblist_supposed, number_of_source_nodes, err)
	if err != nil {
		top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc for pipeline %v received error when handling source topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
	}

	diff_vb_list, target_vb_server_map, err := top_detect_svc.validateTargetTopology()
	if err == target_cluster_version_changed_for_rbac_and_xattr_err {
		// restart pipeline if target begins to support ssl or rbac or xattr
		top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
	} else {
		if err != nil {
			top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc for pipeline %v received error when validating target topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
		}
		err = top_detect_svc.handleTargetTopologyChange(diff_vb_list, target_vb_server_map, err)
		if err != nil {
			top_detect_svc.logger.Warnf("TopologyChangeDetectorSvc for pipeline %v received error when handling target topology change. err=%v", top_detect_svc.pipeline.Topic(), err)
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) handleSourceTopologyChange(vblist_supposed []uint16, number_of_source_nodes int, err_in error) error {
	defer top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v handleSourceTopologyChange completed", top_detect_svc.pipeline.Topic())

	var err error
	if vblist_supposed != nil {
		vblist_removed, vblist_new := base.ComputeDeltaOfUint16Lists(top_detect_svc.vblist_original, vblist_supposed, false)
		if len(vblist_removed) > 0 || len(vblist_new) > 0 {
			top_detect_svc.logger.Infof("Source topology changed for pipeline %v: vblist_removed=%v, vblist_new=%v\n", top_detect_svc.pipeline.Topic(), vblist_removed, vblist_new)
		}

		// first check if relevant problematic vbs in pipeline are due to source topology changes.
		err = top_detect_svc.validateVbErrors(vblist_removed, true /*source*/)
		if err != nil {
			return err
		}
	}

	if err_in == source_topology_changedErr || top_detect_svc.source_topology_change_count > 0 {
		top_detect_svc.source_topology_change_count++
		top_detect_svc.logger.Infof("Number of source topology changes seen by pipeline %v is %v\n", top_detect_svc.pipeline.Topic(), top_detect_svc.source_topology_change_count)
		// restart pipeline if consecutive topology changes reaches limit -- cannot wait any longer
		if top_detect_svc.source_topology_change_count >= base.MaxTopologyChangeCountBeforeRestart {
			sourceTopoChangeRestartString := "Restarting pipeline due to source topology change..."
			top_detect_svc.logger.Warnf("Pipeline %v: %v", top_detect_svc.pipeline.Topic(), sourceTopoChangeRestartString)
			err = errors.New(sourceTopoChangeRestartString)
			top_detect_svc.restartPipeline(err)
			return err
		}

		if vblist_supposed != nil {
			if base.AreSortedUint16ListsTheSame(top_detect_svc.vblist_last, vblist_supposed) {
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

			// if number of source nodes has changed since last topology change check,
			// the bandwith limit assigned to the current node needs to be changed as well
			// update pipeline settings to get bandwith throttler updated
			if number_of_source_nodes != top_detect_svc.number_of_source_nodes {
				top_detect_svc.logger.Infof("Number of source nodes for pipeline %v has changed from %v to %v. Updating bandwidth throttler setting.",
					top_detect_svc.pipeline.Topic(), top_detect_svc.number_of_source_nodes, number_of_source_nodes)
				settings := make(map[string]interface{})
				settings[NUMBER_OF_SOURCE_NODES] = number_of_source_nodes
				top_detect_svc.pipeline.UpdateSettings(settings)

				top_detect_svc.number_of_source_nodes = number_of_source_nodes
			}
		}
	}

	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) handleTargetTopologyChange(diff_vb_list []uint16, target_vb_server_map map[uint16]string, err_in error) error {
	defer top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v handleTargetTopologyChange completed", top_detect_svc.pipeline.Topic())

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
			var targetTopoChangeRestartString = "Restarting pipeline due to target topology change..."
			top_detect_svc.logger.Warnf("Pipeline %v: %v", top_detect_svc.pipeline.Topic(), targetTopoChangeRestartString)
			err = errors.New(targetTopoChangeRestartString)
			top_detect_svc.restartPipeline(err)
			return err
		}

		if target_vb_server_map != nil {
			if base.AreVBServerMapsTheSame(top_detect_svc.target_vb_server_map_last, target_vb_server_map) {
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
		_, found := base.SearchVBInSortedList(vbno, diff_vb_list)
		if !found {
			top_detect_svc.logger.Errorf("Vbucket %v for pipeline %v saw an error, %v, that had not been caused by topology changes. diff_vb_list=%v", vbno, top_detect_svc.pipeline.Topic(), vb_err, diff_vb_list)
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, vb_err))
			return vb_err
		}
	}

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() ([]uint16, int, error) {
	defer top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v validateSourceTopology completed", top_detect_svc.pipeline.Topic())

	vblist_supposed := []uint16{}
	kv_vb_map, number_of_nodes, err := pipeline_utils.GetSourceVBMap(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification().SourceBucketName, top_detect_svc.logger)
	if err != nil {
		return nil, 0, err
	}

	for _, vblist := range kv_vb_map {
		vblist_supposed = append(vblist_supposed, vblist...)
	}

	base.SortUint16List(vblist_supposed)

	if !base.AreSortedUint16ListsTheSame(top_detect_svc.vblist_original, vblist_supposed) {
		top_detect_svc.logger.Infof("Source topology has changed for pipeline %v\n", top_detect_svc.pipeline.Topic())
		top_detect_svc.logger.Debugf("Pipeline %v - vblist_supposed=%v, vblist_now=%v\n", top_detect_svc.pipeline.Topic(), vblist_supposed, top_detect_svc.vblist_original)
		return vblist_supposed, number_of_nodes, source_topology_changedErr
	}

	return vblist_supposed, number_of_nodes, nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetTopology() ([]uint16, map[uint16]string, error) {
	defer top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v validateTargetTopology completed", top_detect_svc.pipeline.Topic())

	targetClusterCompatibility, targetServerVBMap, err := top_detect_svc.getTargetBucketInfo()

	if err != nil {
		top_detect_svc.logger.Warnf("Skipping target check since received error retrieving target bucket info for %v. err=%v", top_detect_svc.pipeline.Topic(), err)
		return nil, nil, err
	}

	if top_detect_svc.check_target_version_for_rbac_and_xattr {
		if base.IsClusterCompatible(targetClusterCompatibility, base.VersionForRBACAndXattrSupport) {
			top_detect_svc.logger.Infof("TopologyChangeDetectorSvc for pipeline %v detected that target cluster has been upgraded to 5.0 or above and is now supporting RBAC and xattr", top_detect_svc.pipeline.Topic())
			return nil, nil, target_cluster_version_changed_for_rbac_and_xattr_err
		}
	}

	// check for target topology changes
	target_vb_server_map := base.ConstructVbServerMap(top_detect_svc.vblist_original, targetServerVBMap)

	diff_vb_list := base.GetDiffVBList(top_detect_svc.vblist_original, top_detect_svc.target_vb_server_map_original, target_vb_server_map)

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

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := targetClusterRef.MyCredentials()
	if err != nil {
		return 0, nil, err
	}

	useExternal, err := top_detect_svc.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
	if err != nil {
		return 0, nil, err
	}

	bucketName := spec.TargetBucketName
	var targetBucketUUID string
	var targetClusterCompatibility int
	var targetServerVBMap map[string][]uint16
	allFieldsFound := false

	targetBucketInfo, err := top_detect_svc.utils.GetBucketInfo(connStr, bucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, top_detect_svc.logger)

	if err == nil {
		targetBucketUUID, err = top_detect_svc.utils.GetBucketUuidFromBucketInfo(bucketName, targetBucketInfo, top_detect_svc.logger)
		if err == nil {
			targetServerVBMap, err = top_detect_svc.utils.GetRemoteServerVBucketsMap(connStr, bucketName, targetBucketInfo, useExternal)
			if err == nil {
				if !top_detect_svc.capi {
					targetClusterCompatibility, err = top_detect_svc.utils.GetClusterCompatibilityFromBucketInfo(targetBucketInfo, top_detect_svc.logger)
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

	if err != nil {
		errMsg := fmt.Sprintf("Skipping target bucket check for spec %v since failed to get bucket info for %v. err=%v", top_detect_svc.pipeline.Topic(), bucketName, err)
		top_detect_svc.logger.Warn(errMsg)
		return 0, nil, errors.New(errMsg)
	}

	// validate target bucket uuid
	if spec.TargetBucketUUID != "" && targetBucketUUID != "" && spec.TargetBucketUUID != targetBucketUUID {
		/*When target bucket uuid does not match, there are two possibilities:
		4. target node has been moved to a different cluster, which happens to have bucket with the same name
		5. target bucket has been deleted and re-created. in this case we need to delete repl spec
		We need to make an additional call to retrieve target cluster uuid to differentiate between these two cases
		D. if the call returns a different cluster uuid as that in repl spec, it is case #4
		E. if the call returns the same cluster uuid as that in repl spec, it is case #5
		F. if the call returns error, we have to play safe and skip the current target bucket check */

		curTargetClusterUUID, err := top_detect_svc.utils.GetClusterUUID(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, top_detect_svc.logger)
		if err != nil {
			// target node not accessible, skip target check
			logMessage := fmt.Sprintf("%v skipping target bucket check since %v is not accessible. err=%v\n", spec.Id, connStr, err)
			top_detect_svc.logger.Warn(logMessage)
			return 0, nil, errors.New(logMessage)
		}
		if curTargetClusterUUID != spec.TargetClusterUUID {
			// case 4, target node has been moved to a different cluster. skip target check
			logMessage := fmt.Sprintf("%v skipping target bucket check since %v has been moved to a different cluster %v.\n", spec.Id, connStr, curTargetClusterUUID)
			top_detect_svc.logger.Warn(logMessage)
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

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}

// restart pipeline to handle topology change
func (top_detect_svc *TopologyChangeDetectorSvc) restartPipeline(err error) {
	top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
}
