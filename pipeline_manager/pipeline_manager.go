// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_manager

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/peerToPeer"
	"github.com/couchbase/goxdcr/v8/pipeline"
	"github.com/couchbase/goxdcr/v8/pipeline_svc"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

var ReplicationStatusNotFound error = errors.New("Error: Replication Status is missing when starting pipeline.")
var UpdaterStoppedError error = errors.New("Updater already stopped")
var MainPipelineNotRunning error = errors.New("Main pipeline is not running")
var ErrorExplicitMappingWoRules = errors.New("specified explicit mapping but no rule is found")
var ErrorCAPIReplicationDeprecated = errors.New("CAPI replication is now deprecated. The pipeline will be paused. Please edit the replication via REST and change the replication type to XMEM")
var ErrorBackfillSpecHasNoVBTasks = errors.New("backfill spec VBTasksMap is empty")
var ErrorNoKVService = errors.New("this node does not have KV service")

var default_failure_restart_interval = 10

// Maximum entries in an error map or array
const MaxErrorDataEntries = 50

type func_report_fixed func(topic string)

type PipelineManager struct {
	pipeline_factory common.PipelineFactory

	repl_spec_svc          service_def.ReplicationSpecSvc
	xdcr_topology_svc      service_def.XDCRCompTopologySvc
	remote_cluster_svc     service_def.RemoteClusterSvc
	checkpoint_svc         service_def.CheckpointsService
	uilog_svc              service_def.UILogSvc
	collectionsManifestSvc service_def.CollectionsManifestSvc
	backfillReplSvc        service_def.BackfillReplSvc
	getBackfillMgr         func() service_def.BackfillMgrIface

	once       sync.Once
	logger     *log.CommonLogger
	serializer PipelineOpSerializerIface
	utils      utilities.UtilsIface

	eventIdWell *int64
}

// External APIs
type PipelineMgrIface interface {
	AllReplicationsForBucket(bucket string) []string
	AllReplications() []string
	AllReplicationsForTargetCluster(targetClusterUuid string) []string
	AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification
	InitiateRepStatus(pipelineName string) error
	ReInitStreams(pipelineName string) error
	UpdatePipeline(pipelineName string, cur_err error) error
	DeletePipeline(pipelineName string) error
	CheckPipelines()
	ReplicationStatusMap() map[string]pipeline.ReplicationStatusIface
	ReplicationStatus(topic string) (pipeline.ReplicationStatusIface, error)
	OnExit() error
	UpdatePipelineWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error
	DismissEventForPipeline(pipelineName string, eventId int) error
	HandleClusterEncryptionLevelChange(old, new service_def.EncryptionSettingIface)
	HandlePeerCkptPush(fullTopic, sender string, dynamicEvt interface{}) error
}

type PipelineMgrInternalIface interface {
	StopAllUpdaters()
	StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap
	StartPipeline(topic string) base.ErrorMap
	Update(topic string, cur_err error) error
	RemoveReplicationStatus(topic string) error
	GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error)
	GetLastUpdateResult(topic string) bool // whether or not the last update was successful
	GetRemoteClusterSvc() service_def.RemoteClusterSvc
	GetLogSvc() service_def.UILogSvc
	GetReplSpecSvc() service_def.ReplicationSpecSvc
	GetXDCRTopologySvc() service_def.XDCRCompTopologySvc
	AutoPauseReplication(topic string) error
	ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error
	PostTopologyStatus()
}

type PipelineMgrForUpdater interface {
	PipelineMgrInternalIface
	StartBackfillPipeline(topic string) base.ErrorMap
	StopBackfillPipeline(topic string, skipCkpt bool) base.ErrorMap
}

type PipelineMgrForSerializer interface {
	PipelineMgrInternalIface
	PauseReplication(topic string) error
	CleanupPipeline(topic string) error
	StartBackfill(topic string) error
	StopBackfill(topic string, skipCkpt bool) error
	StopBackfillWithStoppedCb(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error
	CleanupBackfillPipeline(topic string) error
	UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error
	DismissEvent(eventId int) error
	BackfillMappingUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error
}

// Specifically APIs used by backfill manager
type PipelineMgrBackfillIface interface {
	GetMainPipelineThroughSeqnos(topic string) (map[uint16]uint64, error)
	RequestBackfill(topic string) error

	// The following calls will be blocking
	HaltBackfill(topic string) error
	HaltBackfillWithCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error
	CleanupBackfillCkpts(topic string) error
	WaitForMainPipelineCkptMgrToStop(topic, internalID string)

	ReInitStreams(pipelineName string) error
	BackfillMappingStatusUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestDelta []*metadata.CollectionsManifest) error
}

func NewPipelineManager(factory common.PipelineFactory, repl_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc, remote_cluster_svc service_def.RemoteClusterSvc, checkpoint_svc service_def.CheckpointsService, uilog_svc service_def.UILogSvc, logger_context *log.LoggerContext, utilsIn utilities.UtilsIface, collectionsManifestSvc service_def.CollectionsManifestSvc, backfillReplSvc service_def.BackfillReplSvc, eventIdWell *int64, getBackfillMgr func() service_def.BackfillMgrIface) *PipelineManager {
	if eventIdWell == nil {
		// Possible for unit test
		eventId := int64(-1)
		eventIdWell = &eventId
	}

	pipelineMgrRetVar := &PipelineManager{
		pipeline_factory:       factory,
		repl_spec_svc:          repl_spec_svc,
		xdcr_topology_svc:      xdcr_topology_svc,
		remote_cluster_svc:     remote_cluster_svc,
		checkpoint_svc:         checkpoint_svc,
		logger:                 log.NewLogger("PipelineMgr", logger_context),
		uilog_svc:              uilog_svc,
		utils:                  utilsIn,
		collectionsManifestSvc: collectionsManifestSvc,
		backfillReplSvc:        backfillReplSvc,
		eventIdWell:            eventIdWell,
		getBackfillMgr:         getBackfillMgr,
	}
	pipelineMgrRetVar.logger.Info("Pipeline Manager is constructed")

	//initialize the expvar storage for replication status
	pipeline.RootStorage()

	// initialize serializer - this is the front-end of all user requests
	pipelineMgrRetVar.serializer = NewPipelineOpSerializer(pipelineMgrRetVar, pipelineMgrRetVar.logger)

	pipelineMgrRetVar.pipeline_factory.SetPipelineStopCallback(pipelineMgrRetVar.UpdateWithStoppedCb)

	return pipelineMgrRetVar
}

func (pipelineMgr *PipelineManager) ReplicationStatus(topic string) (pipeline.ReplicationStatusIface, error) {
	obj, err := pipelineMgr.repl_spec_svc.GetDerivedObj(topic)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, pipelineMgr.utils.ReplicationStatusNotFoundError(topic)
	}
	return obj.(pipeline.ReplicationStatusIface), nil
}

// Should be called only from serializer to prevent race
func (pipelineMgr *PipelineManager) RemoveReplicationStatus(topic string) error {
	rs, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil {
		return err
	}

	if rs != nil {
		updaterObj := rs.Updater()

		pipelineMgr.logger.Infof("Stopping pipeline %v since the replication spec has been deleted\n", topic)
		if updaterObj == nil {
			pipelineMgr.logger.Errorf("Updater object is nil for pipeline %v, may be leaking an updater somewhere\n", topic)
		} else {
			updater := updaterObj.(*PipelineUpdater)
			err = updater.stop()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (pipelineMgr *PipelineManager) UpdatePipeline(topic string, cur_err error) error {
	return pipelineMgr.serializer.Update(topic, cur_err)
}

func (pipelineMgr *PipelineManager) DeletePipeline(pipelineName string) error {
	return pipelineMgr.serializer.Delete(pipelineName)
}

func (pipelineMgr *PipelineManager) InitiateRepStatus(pipelineName string) error {
	return pipelineMgr.serializer.Init(pipelineName)
}

func (pipelineMgr *PipelineManager) AutoPauseReplication(pipelineName string) error {
	return pipelineMgr.serializer.Pause(pipelineName)
}

func (pipelineMgr *PipelineManager) RequestBackfill(pipelineName string) error {
	return pipelineMgr.serializer.StartBackfill(pipelineName)
}

func (pipelineMgr *PipelineManager) HaltBackfill(pipelineName string) error {
	return pipelineMgr.serializer.StopBackfill(pipelineName)
}

// If backfill pipeline dne or is stopped, callbacks will be called
// Only if backfill pipeline is not stopped, will the callback be skipped
func (pipelineMgr *PipelineManager) HaltBackfillWithCb(pipelineName string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error {
	return pipelineMgr.serializer.StopBackfillWithCb(pipelineName, cb, errCb, skipCkpt)
}

func (pipelineMgr *PipelineManager) CleanupBackfillCkpts(topic string) error {
	return pipelineMgr.serializer.CleanBackfill(topic)
}

// This should really be the method to be used. This is considered part of the interface
// and can be easily unit tested
func (pipelineMgr *PipelineManager) ReplicationStatusMap() map[string]pipeline.ReplicationStatusIface {
	ret := make(map[string]pipeline.ReplicationStatusIface)
	specId_list, err := pipelineMgr.repl_spec_svc.AllReplicationSpecIds()
	if err == nil {
		for _, specId := range specId_list {
			repStatus, _ := pipelineMgr.ReplicationStatus(specId)
			if repStatus != nil {
				ret[specId] = repStatus
			}
		}
	}
	return ret
}

func (pipelineMgr *PipelineManager) LogStatusSummary() {
	pipelineMgr.logger.Infof("Replication Status = %v\n", pipelineMgr.ReplicationStatusMap())
}

func (pipelineMgr *PipelineManager) AllReplicationsForBucket(bucket string) []string {
	var repIds []string
	for _, key := range pipelineMgr.topics() {
		// there should not be any errors since topics() should return valid replication ids
		match, _ := metadata.IsReplicationIdForSourceBucket(key, bucket)
		if match {
			repIds = append(repIds, key)
		}
	}
	return repIds
}

func (pipelineMgr *PipelineManager) AllReplicationSpecsForTargetCluster(targetClusterUuid string) map[string]*metadata.ReplicationSpecification {
	ret := make(map[string]*metadata.ReplicationSpecification)
	for topic, rep_status := range pipelineMgr.ReplicationStatusMap() {
		spec := rep_status.Spec()
		if spec != nil && spec.TargetClusterUUID == targetClusterUuid {
			ret[topic] = spec
		}
	}

	return ret
}

func (pipelineMgr *PipelineManager) AllReplicationsForTargetCluster(targetClusterUuid string) []string {
	ret := make([]string, 0)
	for topic, rep_status := range pipelineMgr.ReplicationStatusMap() {
		spec := rep_status.Spec()
		if spec != nil && spec.TargetClusterUUID == targetClusterUuid {
			ret = append(ret, topic)
		}
	}

	return ret
}

func (pipelineMgr *PipelineManager) AllReplications() []string {
	return pipelineMgr.topics()
}

func (pipelineMgr *PipelineManager) CheckPipelines() {
	pipelineMgr.validateAndGCSpecs()
	pipelineMgr.checkRemoteClusterSvcForChangedConfigs()
	pipelineMgr.checkAndHandleRemoteClusterAuthErrs()
	pipelineMgr.LogStatusSummary()
	pipelineMgr.PostTopologyStatus()
}

func (pipelineMgr *PipelineManager) HandleClusterEncryptionLevelChange(old, new service_def.EncryptionSettingIface) {
	if new.IsStrictEncryption() == false || old.IsStrictEncryption() == new.IsStrictEncryption() {
		return
	}
	// Cluster encryption level has changed to strict. All pipelines not full encryption cannot run
	refList, err := pipelineMgr.remote_cluster_svc.RemoteClusters()
	if err != nil {
		pipelineMgr.logger.Errorf("Cannot get list of remote clusters. err=%v", err)
		return
	}
	for _, ref := range refList {
		if ref.DemandEncryption() == true && ref.EncryptionType() == metadata.EncryptionType_Full {
			// Full encryption remote ref works with strict cluster encryption
			continue
		}

		replSpecsList, err := pipelineMgr.repl_spec_svc.AllActiveReplicationSpecsWithRemote(ref)
		if err != nil {
			pipelineMgr.logger.Errorf("Unable to retrieve specs for remote cluster %v ... user should manually restart pipelines replicating to this remote cluster", ref.Id())
			continue
		}
		for _, spec := range replSpecsList {
			pipelineMgr.UpdatePipeline(spec.Id, base.ErrorPipelineRestartDueToEncryptionChange)
		}
	}
}
func (pipelineMgr *PipelineManager) validateAndGCSpecs() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		//validate replication spec
		spec := rep_status.Spec()
		if spec != nil {
			pipelineMgr.logger.Infof("checking pipeline spec=%v, source bucket uuid=%v", spec, spec.SourceBucketUUID)
			pipelineMgr.repl_spec_svc.ValidateAndGC(spec)
		}
	}
}

func (pipelineMgr *PipelineManager) checkRemoteClusterSvcForChangedConfigs() {
	refList, err := pipelineMgr.remote_cluster_svc.GetRefListForRestartAndClearState()
	if err != nil {
		pipelineMgr.logger.Warnf("Checkpipeline got %v when asking remote cluster reference for config changes that require pipeline restarts", err)
		return
	}
	for _, ref := range refList {
		replSpecsList, err := pipelineMgr.repl_spec_svc.AllActiveReplicationSpecsWithRemote(ref)
		if err != nil {
			pipelineMgr.logger.Warnf("Unable to retrieve specs for remote cluster %v ... user should manually restart pipelines replicating to this remote cluster", ref.Id())
			continue
		}
		for _, spec := range replSpecsList {
			pipelineMgr.UpdatePipeline(spec.Id, base.ErrorPipelineRestartDueToClusterConfigChange)
		}
	}
}

func getCredentialErrMsg(rcName, pipelineName string) string {
	return fmt.Sprintf("Remote Cluster %v has changed its credentials and the entered credentials on this cluster is no longer valid for pipeline %v", rcName, pipelineName)
}

func getCredentialErrPauseMsg(rcName, pipelineName string) string {
	return fmt.Sprintf("%v. It will now be paused. Please re-enter the correct remote cluster reference credentials and resume the replication", getCredentialErrMsg(rcName, pipelineName))
}

func (pipelineMgr *PipelineManager) checkAndHandleRemoteClusterAuthErrs() {
	refList, err := pipelineMgr.remote_cluster_svc.GetRefListForFirstTimeBadAuths()
	if err != nil {
		pipelineMgr.logger.Warnf("Checkpipeline got %v when asking for any remote cluster reference that experienced auth error", err)
	}
	for _, ref := range refList {
		replSpecsList, err := pipelineMgr.repl_spec_svc.AllActiveReplicationSpecsWithRemote(ref)
		if err != nil {
			pipelineMgr.logger.Warnf("Unable to retrieve specs for remote cluster %v ... user should manually pause pipelines replicating to this remote cluster because remote side authentication has changed", ref.Id())
			continue
		}
		for _, spec := range replSpecsList {
			shouldRetry := spec.Settings.GetRetryOnRemoteAuthErr()
			if shouldRetry {
				pipelineMgr.Update(spec.Id, fmt.Errorf(getCredentialErrMsg(ref.Name(), spec.Id)))
			} else {
				pipelineMgr.AutoPauseReplication(spec.Id)
				msg := getCredentialErrPauseMsg(ref.Name(), spec.Id)
				pipelineMgr.uilog_svc.Write(msg)
				pipelineMgr.logger.Errorf(msg)
			}
		}
	}
}

func (pipelineMgr *PipelineManager) OnExit() error {
	//send finish signal to all updater to stop the pipelines and exit
	pipelineMgr.StopSerializer()
	pipelineMgr.StopAllUpdaters()

	pipelineMgr.logger.Infof("All running repairers stopped")
	return nil

}

func (pipelineMgr *PipelineManager) StopAllUpdaters() {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for _, rep_status := range rep_status_map {
		updater := rep_status.Updater()
		if updater != nil {
			updater.(*PipelineUpdater).stop()
		}
	}
}

func (pipelineMgr *PipelineManager) StopSerializer() {
	if pipelineMgr.serializer != nil {
		pipelineMgr.serializer.Stop()
	}
}

func (pipelineMgr *PipelineManager) StartPipeline(topic string) base.ErrorMap {
	var err error
	errMap := make(base.ErrorMap)
	pipelineMgr.logger.Infof("Starting the pipeline %s\n", topic)

	rep_status, _ := pipelineMgr.ReplicationStatus(topic)
	if rep_status == nil {
		// This should not be nil as updater should be the only one calling this and
		// it would have created a rep_status way before here
		errMap[fmt.Sprintf("pipelineMgr.ReplicationStatus(%v)", topic)] = ReplicationStatusNotFound
		return errMap
	}

	// When persistent events are raised, usually they are fatal and pipelines would have paused and the
	// persistent messages remain while pipelins are paused
	// Thus, when pipelines are restarted, clear them. If they come back, they'll come back again by themselves
	rep_status.GetEventsManager().ClearPersistentEvents()

	if rep_status.RuntimeStatus(true) == pipeline.Replicating {
		//the pipeline is already running
		pipelineMgr.logger.Infof("The pipeline asked to be started, %v, is already running", topic)
		return errMap
	}

	// validate the pipeline before starting it
	err = pipelineMgr.validatePipeline(topic)
	if err != nil {
		errMap[fmt.Sprintf("pipelineMgr.validatePipeline(%v)", topic)] = err
		return errMap
	}

	pipelineMgr.setP2PWaitTime(rep_status)

	rep_status.RecordProgress(common.ProgressStartConstruction)

	p, err := pipelineMgr.pipeline_factory.NewPipeline(topic, rep_status.RecordProgress)
	if p != nil {
		// Regardless of error, remember the pipeline so that if there's an error condition, the retry will stop it
		rep_status.SetPipeline(p)
	}
	if err != nil {
		errMap[fmt.Sprintf("pipelineMgr.pipeline_factory.NewPipeline(%v)", topic)] = err
		return errMap
	}
	rep_status.RecordProgress(common.ProgressConstructionDone)

	pipelineMgr.logger.Infof("Pipeline %v is constructed. Starting it.", p.InstanceId())
	p.SetProgressRecorder(rep_status.RecordProgress)
	errMap = p.Start(rep_status.SettingsMap())
	if len(errMap) > 0 {
		pipelineMgr.logger.Errorf("Failed to start the pipeline %v", p.InstanceId())
	} else {
		backfillSpec, _ := pipelineMgr.backfillReplSvc.BackfillReplSpec(topic)
		if backfillSpec != nil {
			// Has backfill pipeline waiting
			pipelineMgr.logger.Infof("%s: backfill pipeline started by main pipeline", backfillSpec.Id)
			backfillStartQueueErr := pipelineMgr.StartBackfill(topic)
			if backfillStartQueueErr != nil {
				pipelineMgr.logger.Errorf("Unable to queue backfill pipeline start for %v", topic)
			}
		}
	}
	return errMap
}

func (pipelineMgr *PipelineManager) setP2PWaitTime(rep_status pipeline.ReplicationStatusIface) {
	numKVNodes, err := pipelineMgr.xdcr_topology_svc.NumberOfKVNodes()
	if err != nil {
		numKVNodes = 1
	}
	// Subtract itself
	numKVNodes = numKVNodes - 1
	if numKVNodes < 0 {
		// odd
		numKVNodes = 0
	}
	if numKVNodes > 0 {
		delayMap := make(map[string]interface{})
		// waitTime is a guesstimate allowance
		waitTime := time.Duration(numKVNodes) * 30 * time.Second
		// OpaqueTimeout is the timeout + some buffer
		// When rebalance happens and source nodes get added, pipeline will restart and so this section will be called
		// when a pipeline restarts, and the value will get updated accordingly
		atomic.StoreUint32(&peerToPeer.P2POpaqueTimeoutAtomicMin, uint32(numKVNodes+1))
		delayMap[metadata.P2PDynamicWaitDurationKey] = waitTime
		rep_status.SetCustomSettings(delayMap)
	} else {
		rep_status.ClearCustomSetting(metadata.P2PDynamicWaitDurationKey)
	}
}

// validate that a pipeline has valid configuration and can be started before starting it
func (pipelineMgr *PipelineManager) validatePipeline(topic string) error {
	pipelineMgr.logger.Infof("Validating pipeline %v\n", topic)

	spec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		pipelineMgr.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return err
	}

	// Deprecate CAPI replication
	if spec.Settings.RepType == metadata.ReplicationTypeCapi {
		return ErrorCAPIReplicationDeprecated
	}

	// Check if this node is KV node or not
	isMyNodeKVNode, err := pipelineMgr.xdcr_topology_svc.IsKVNode()
	if err == nil && !isMyNodeKVNode {
		return ErrorNoKVService
	}

	// Explicit mapping mode on means that there should be rules accompanying it
	collectionsMode := spec.Settings.GetCollectionModes()
	collectionsMappingRules := spec.Settings.GetCollectionsRoutingRules()
	if !collectionsMode.IsImplicitMapping() && len(collectionsMappingRules) == 0 {
		errString := fmt.Sprintf("spec %v - %v", spec.Id, ErrorExplicitMappingWoRules.Error())
		pipelineMgr.logger.Error(errString)
		return fmt.Errorf(errString)
	}

	// refresh remote cluster reference when retrieving it, hence making sure that all fields,
	// especially the security settings like sanInCertificate, are up to date
	targetClusterRef, err := pipelineMgr.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		pipelineMgr.logger.Errorf("Error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, topic, err)
		return err
	}

	err = pipelineMgr.remote_cluster_svc.ValidateRemoteCluster(targetClusterRef)
	if err != nil {
		pipelineMgr.logger.Errorf("Error validating remote cluster with uuid %v for pipeline %v. err=%v\n", spec.TargetClusterUUID, topic, err)
		return err
	}

	return nil
}

func (pipelineMgr *PipelineManager) getPipelineFromMap(topic string) common.Pipeline {
	rep_status, _ := pipelineMgr.ReplicationStatus(topic)
	if rep_status != nil {
		return rep_status.Pipeline()
	}
	return nil
}

func (pipelineMgr *PipelineManager) removePipelineFromReplicationStatus(p common.Pipeline) error {
	//	return nil
	if p != nil {
		rep_status, _ := pipelineMgr.ReplicationStatus(p.Topic())
		if rep_status != nil {
			rep_status.RemovePipeline(p)
		} else {
			return fmt.Errorf("Replication %v hasn't been registered with PipelineManager yet", p.Topic())

		}
	}
	return nil

}

// Called internally from updater only
func (pipelineMgr *PipelineManager) StopPipeline(rep_status pipeline.ReplicationStatusIface) base.ErrorMap {
	var errMap base.ErrorMap = make(base.ErrorMap)

	if rep_status == nil {
		errMap["pipelineMgr.StopPipeline"] = errors.New("Invalid parameter value rep_status=nil")
		return errMap
	}

	replId := rep_status.RepId()

	pipelineMgr.logger.Infof("Trying to stop the pipeline %s", replId)

	p := rep_status.Pipeline()
	bp := rep_status.BackfillPipeline()
	if p != nil {
		state := p.State()
		if state != common.Pipeline_Stopping && state != common.Pipeline_Stopped {
			var bgWaitGrp *sync.WaitGroup
			var bgErrMap base.ErrorMap
			var bgErrMapMtx sync.RWMutex
			if bp != nil {
				bgWaitGrp = &sync.WaitGroup{}
				stopBackfillPipelineInBg := func() {
					defer bgWaitGrp.Done()
					tempMap := pipelineMgr.StopBackfillPipeline(replId, false)
					bgErrMapMtx.Lock()
					bgErrMap = tempMap
					bgErrMapMtx.Unlock()
				}
				bgWaitGrp.Add(1)
				go stopBackfillPipelineInBg()
			}
			errMap = p.Stop()
			if bgWaitGrp != nil {
				bgWaitGrp.Wait()
			}
			bgErrMapMtx.RLock()
			if bgErrMap != nil {
				for k, v := range bgErrMap {
					errMap[k] = v
				}
			}
			bgErrMapMtx.RUnlock()
			if len(errMap) > 0 {
				pipelineMgr.logger.Errorf("Received error(s) when stopping pipeline %v - %v\n", replId, base.FlattenErrorMap(errMap))
				//pipeline failed to stopped gracefully in time. ignore the error.
				//the parts of the pipeline will eventually commit suicide.
			} else {
				pipelineMgr.logger.Infof("Pipeline %v has been stopped\n", replId)
			}
			pipelineMgr.removePipelineFromReplicationStatus(p)
			pipelineMgr.logger.Infof("Replication Status=%v\n", rep_status)
		} else {
			pipelineMgr.logger.Infof("Pipeline %v is not in the right state to be stopped. state=%v\n", replId, state)
		}
	} else {
		pipelineMgr.logger.Infof("Pipeline %v is not running\n", replId)
	}

	// Remove all dismissed events
	rep_status.GetEventsManager().ResetDismissedHistory()
	rep_status.GetEventsManager().ClearNonBrokenMapOrPersistentEvents()

	// if replication spec has been deleted
	// or deleted and recreated, which is signaled by change in spec internal id
	// perform clean up
	spec, _ := pipelineMgr.repl_spec_svc.ReplicationSpec(replId)
	if spec == nil || (rep_status.GetSpecInternalId() != "" && rep_status.GetSpecInternalId() != spec.InternalId) {
		if spec == nil {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted.\n", replId)
		} else {
			pipelineMgr.logger.Infof("%v Cleaning up replication status since repl spec has been deleted and recreated. oldSpecInternalId=%v, newSpecInternalId=%v\n", replId, rep_status.GetSpecInternalId(), spec.InternalId)
		}

		if bp != nil {
			// reset stats for backfill pipeline
			// checkpoints for backfill pipeline will be deleted as part of replication spec change callbacks of backfill components in postDeleteBackfillRepl
			rep_status.ResetStorage(common.BackfillPipeline)
		}

		err := pipelineMgr.checkpoint_svc.DelCheckpointsDocs(replId)
		if err != nil {
			pipelineMgr.logger.Errorf("MainPipeline %v saw an error while deleting checkpoints as part of StopPipeline. err=%v", replId, err)
		}

		rep_status.ResetStorage(common.MainPipeline)
		pipelineMgr.repl_spec_svc.SetDerivedObj(replId, nil)

		//close the connection pool for the replication
		pools := base.ConnPoolMgr().FindPoolNamesByPrefix(replId)
		for _, poolName := range pools {
			base.ConnPoolMgr().RemovePool(poolName)
		}
	}

	return errMap
}

// Stops pipeline and cleans all checkpoints with it, results in a restream
// Add retry mechanism here because failure will not be ideal
// Should be called only from serializer
// Also deletes any backfill replications because restreaming takes care of backfill cases
func (pipelineMgr *PipelineManager) CleanupPipeline(topic string) error {
	var rep_status *pipeline.ReplicationStatus
	var err error
	defer pipelineMgr.logger.Infof("%v CleanupPipeline including checkpoints removal finished (err = %v)", topic, err)
	getOp := func() error {
		rep_status, err = pipelineMgr.GetOrCreateReplicationStatus(topic, nil)
		return err
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("GetOrCreateReplicationStatus %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, getOp)

	if err != nil {
		return err
	}

	updater := rep_status.Updater().(*PipelineUpdater)
	replId := rep_status.RepId()

	// Stop the updater to stop the pipeline so it will not handle any more jobs before we remove the checkpoints
	updater.stop()

	retryOp := func() error {
		return pipelineMgr.checkpoint_svc.DelCheckpointsDocs(replId)
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("DelCheckpointsDocs %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, retryOp)
	if err != nil {
		pipelineMgr.logger.Warnf("Removing checkpoint resulting in error: %v\n")
	}

	// When pipeline is "cleaned up", we don't need anymore backfill specs
	retryOp = func() error {
		_, err := pipelineMgr.backfillReplSvc.DelBackfillReplSpec(replId)
		return err
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("DelBackfillReplSpec %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, retryOp)
	if err != nil {
		pipelineMgr.logger.Warnf("Removing backfill replication spec resulting in error: %v\n")
	}

	// regardless of err above, we should restart updater
	err = pipelineMgr.launchUpdater(topic, nil, rep_status)

	return err
}

// When a backfill pipeline is stopped, it can choose to cleanup the checkpoints for the backfill pipeline
// so that when a new backfill task starts, it will start cleanly
func (pipelineMgr *PipelineManager) CleanupBackfillPipeline(topic string) error {
	var rep_status *pipeline.ReplicationStatus
	var err error
	defer pipelineMgr.logger.Infof("%v CleanupBackfillPipeline including checkpoints removal finished (err = %v)", topic, err)
	getOp := func() error {
		rep_status, err = pipelineMgr.GetOrCreateReplicationStatus(topic, nil)
		return err
	}
	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("GetOrCreateReplicationStatus %v", topic), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, getOp)

	if err != nil {
		return err
	}

	replId := rep_status.RepId()

	backfillSpecId := common.ComposeFullTopic(replId, common.BackfillPipeline)
	retryOp := func() error {
		return pipelineMgr.checkpoint_svc.DelCheckpointsDocs(backfillSpecId)
	}
	rep_status.ResetStorage(common.BackfillPipeline)

	err = pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("DelCheckpointsDocs %v", backfillSpecId), base.PipelineSerializerRetryWaitTime, base.PipelineSerializerMaxRetry, base.PipelineSerializerRetryFactor, retryOp)
	if err != nil {
		pipelineMgr.logger.Warnf("Removing backfill checkpoint resulting in error: %v\n")
	}
	return err
}

func (pipelineMgr *PipelineManager) pipeline(topic string) common.Pipeline {
	pipeline := pipelineMgr.getPipelineFromMap(topic)
	return pipeline
}

func (pipelineMgr *PipelineManager) liveTopics() []string {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			topics = append(topics, topic)
		}
	}
	return topics
}

func (pipelineMgr *PipelineManager) topics() []string {
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	topics := make([]string, 0, len(rep_status_map))
	for topic, _ := range rep_status_map {
		topics = append(topics, topic)
	}

	return topics
}

func (pipelineMgr *PipelineManager) livePipelines() map[string]common.Pipeline {
	ret := make(map[string]common.Pipeline)
	rep_status_map := pipelineMgr.ReplicationStatusMap()
	for topic, rep_status := range rep_status_map {
		if rep_status.RuntimeStatus(true) == pipeline.Replicating {
			ret[topic] = rep_status.Pipeline()
		}
	}

	return ret
}

// Should be only called by GetOrCreateReplicationStatus
func (pipelineMgr *PipelineManager) launchUpdater(topic string, cur_err error, rep_status *pipeline.ReplicationStatus) error {
	settingsMap := rep_status.SettingsMap()
	retry_interval_obj := settingsMap[metadata.FailureRestartIntervalKey]
	// retry_interval_obj may be null in abnormal scenarios, e.g., when replication spec has been deleted
	// default retry_interval to 10 seconds in such cases
	retry_interval := default_failure_restart_interval
	if retry_interval_obj != nil {
		retry_interval = settingsMap[metadata.FailureRestartIntervalKey].(int)
	}

	updater := newPipelineUpdater(topic, retry_interval, cur_err, rep_status, pipelineMgr.logger, pipelineMgr)

	// SetUpdater should not fail
	err := rep_status.SetUpdater(updater)
	if err != nil {
		return err
	}

	go updater.run()
	pipelineMgr.logger.Infof("Pipeline updater %v is launched with retry_interval=%v\n", topic, retry_interval)

	return nil
}

// Should be used internally from serializer only, or only from an internal update op
func (pipelineMgr *PipelineManager) GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error) {
	var repStatus *pipeline.ReplicationStatus
	repStatusIface, _ := pipelineMgr.ReplicationStatus(topic)
	if repStatusIface != nil {
		return repStatusIface.(*pipeline.ReplicationStatus), nil
	} else if _, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic); err == base.ReplNotFoundErr {
		pipelineMgr.logger.Warnf("ReplicationStatus %v is not there and spec is not present\n", topic)
		return nil, err
	} else {
		var retErr error
		repStatus = pipeline.NewReplicationStatus(topic, pipelineMgr.repl_spec_svc.ReplicationSpec, pipelineMgr.logger, pipelineMgr.eventIdWell, pipelineMgr.utils)
		pipelineMgr.repl_spec_svc.SetDerivedObj(topic, repStatus)
		pipelineMgr.logger.Infof("ReplicationStatus is created and set with %v\n", topic)
		if repStatus.Updater() != nil {
			panic("non-nill updater with a new rep_status")
		}
		retErr = pipelineMgr.launchUpdater(topic, cur_err, repStatus)
		return repStatus, retErr
	}
}

// Should be called internally from serializer's Update()
func (pipelineMgr *PipelineManager) Update(topic string, cur_err error) error {
	updater, rep_status, err := pipelineMgr.getUpdater(topic, cur_err)
	if err != nil {
		return err
	}

	// if update is not initiated by error
	// trigger updater to update immediately, not to wait for the retry interval
	if cur_err != nil {
		rep_status.AddError(cur_err)
		updater.refreshPipelineDueToErr(cur_err)
	} else {
		updater.refreshPipelineManually()
	}
	return nil
}

// This is a specialized version of Update(), in which the update call will ensure that the updater
// execute a specialized version of update where the "callback" is called once pipelines have stopped, and before they
// start up again
// If the callback fails with a non-nil error, then the errCb() function is called as a result
func (pipelineMgr *PipelineManager) UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	updater, _, err := pipelineMgr.getUpdater(topic, nil)
	if err != nil {
		return err
	}
	err = updater.registerPipelineStoppedCb(updaterIntermediateCb{
		callback: callback,
		errCb:    errCb,
	})
	if err != nil {
		// Unable to register the callback - this means the callback will not get to execute
		// and thus the errCb() will need to be called
		errCb(err, false)
	}
	return err
}

// Should be called internally from serializer's Pause()
func (pipelineMgr *PipelineManager) PauseReplication(topic string) error {
	currentSpec, err := pipelineMgr.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		return fmt.Errorf("Pipeline %v unable to retrieve spec: %v", topic, err)
	}

	pauseMap := make(metadata.ReplicationSettingsMap)
	pauseMap[metadata.ActiveKey] = false
	_, errorMap := currentSpec.Settings.UpdateSettingsFromMap(pauseMap)
	if len(errorMap) > 0 {
		err = fmt.Errorf(base.FlattenErrorMap(errorMap))
		return fmt.Errorf("Pipeline %v updateSettings: %v", topic, err)
	}

	err = pipelineMgr.repl_spec_svc.SetReplicationSpec(currentSpec)
	if err != nil {
		err = fmt.Errorf("Pipeline %v unable to setReplicationSpec: %v", topic, err)
	}
	return err
}

func (pipelineMgr *PipelineManager) ForceTargetRefreshManifest(spec *metadata.ReplicationSpecification) error {
	return pipelineMgr.collectionsManifestSvc.ForceTargetManifestRefresh(spec)
}

func (pipelineMgr *PipelineManager) StartBackfill(topic string) error {
	updater, _, err := pipelineMgr.getUpdater(topic, nil)
	if err != nil {
		return err
	}

	updater.startBackfillPipeline()
	return nil
}

func (pipelineMgr *PipelineManager) StopBackfill(topic string, skipCkpt bool) error {
	updater, _, err := pipelineMgr.getUpdater(topic, nil)
	if err != nil {
		return err
	}
	updater.stopBackfillPipeline(skipCkpt)
	return nil
}

func (pipelineMgr *PipelineManager) getUpdater(topic string, curErr error) (*PipelineUpdater, *pipeline.ReplicationStatus, error) {
	repStatus, _ := pipelineMgr.GetOrCreateReplicationStatus(topic, curErr)
	if repStatus == nil {
		return nil, nil, fmt.Errorf("replication status for %v does not exist", topic)
	}

	var updater *PipelineUpdater
	updaterObj := repStatus.Updater()
	if updaterObj == nil {
		errorStr := fmt.Sprintf("Failed to get updater from replication status in topic %v in StopBackfill()", topic)
		return nil, repStatus, errors.New(errorStr)
	} else {
		var ok bool
		updater, ok = updaterObj.(*PipelineUpdater)
		if updater == nil || !ok {
			errorStr := fmt.Sprintf("Unable to cast updaterObj as type PipelineUpdater in topic %v", topic)
			return nil, repStatus, errors.New(errorStr)
		}
	}
	return updater, repStatus, nil
}

func (pipelineMgr *PipelineManager) StopBackfillWithStoppedCb(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error {
	updater, _, err := pipelineMgr.getUpdater(topic, nil)
	if err != nil {
		return err
	}
	err = updater.registerBackfillPipelineStoppedCb(updaterIntermediateCb{
		callback: cb,
		errCb:    errCb,
	})
	if err != nil {
		// Unable to register the callback - this means the callback will not get to execute
		// and thus the errCb() will need to be called
		errCb(err, false)
	}
	if err != nil {
		return err
	}

	updater.stopBackfillPipeline(skipCkpt)
	return nil
}

func (pipelineMgr *PipelineManager) StartBackfillPipeline(topic string) base.ErrorMap {
	var err error
	errMap := make(base.ErrorMap)
	pipelineMgr.logger.Infof("Starting the backfill pipeline %s\n", topic)
	repStatusKey := fmt.Sprintf("pipelineMgr.ReplicationStatus(%v)", topic)
	rep_status, _ := pipelineMgr.ReplicationStatus(topic)

	if rep_status == nil {
		// This should not be nil as updater should be the only one calling this and
		// it would have created a rep_status way before here
		errMap[repStatusKey] = ReplicationStatusNotFound
		return errMap
	}

	mainPipeline := rep_status.Pipeline()
	if mainPipeline == nil || (mainPipeline.State() != common.Pipeline_Running && mainPipeline.State() != common.Pipeline_Starting) {
		pipelineMgr.logger.Warnf("Replication %v's main pipeline is not running. Backfill pipeline will not start", topic)
		errMap[repStatusKey] = MainPipelineNotRunning
		return errMap
	}

	// Before construction, check to ensure there are actually tasks to run
	backfillSpecRO, err := pipelineMgr.backfillReplSvc.BackfillReplSpec(topic)
	if backfillSpecRO == nil || err != nil {
		errMap["pipelineMgr.StartBackfillPipeline"] = err
		return errMap
	}

	// It is possible that the "cached" version of the backfill spec is being updated since read
	// Clone a copy of it here to ensure that all the checks below are valid and data won't be changed
	// once the checks are done
	backfillSpec := backfillSpecRO.Clone()

	var mainPipelineVbs []uint16
	srcNozzles := mainPipeline.Sources()
	for _, srcNozzle := range srcNozzles {
		mainPipelineVbs = append(mainPipelineVbs, srcNozzle.ResponsibleVBs()...)
	}

	if backfillSpec.VBTasksMap.Len() == 0 || backfillSpec.VBTasksMap.LenWithVBs(mainPipelineVbs) == 0 {
		pipelineMgr.logger.Infof("No tasks for backfill pipeline mainVBs: %v, specTasks: %v", mainPipelineVbs, backfillSpec.VBTasksMap.GetVBs())
		errMap["pipelineMgr.StartBackfillPipeline"] = ErrorBackfillSpecHasNoVBTasks
		return errMap
	}

	// First stop any latched backfill pipeline instances that have failed to start before
	errMap = pipelineMgr.StopBackfillPipeline(mainPipeline.Topic(), false)
	if len(errMap) > 0 {
		// Show warnings but continue
		pipelineMgr.logger.Warnf("Stopping backfill pipeline prior to starting it had error(s): %v - continue to create next iteration of backfill pipeline", errMap)
		errMap = make(base.ErrorMap)
	}

	rep_status.RecordBackfillProgress("Start backfill pipeline construction")

	bp, err := pipelineMgr.pipeline_factory.NewSecondaryPipeline(topic, mainPipeline,
		rep_status.RecordBackfillProgress, common.BackfillPipeline)
	if bp != nil {
		// Regardless of error, remember the pipeline so that if there's an error condition, the retry will stop it
		rep_status.SetPipeline(bp)
	}
	if err != nil {
		if errMap == nil {
			errMap = make(base.ErrorMap)
		}
		errMap[fmt.Sprintf("pipelineMgr.pipeline_factory.NewSecondaryPipeline(%v)", topic)] = err
		return errMap
	}
	rep_status.RecordBackfillProgress("Backfill Pipeline is constructed")

	// Requesting a backfill pipeline means that a pipeline will start and for the top task in each VB
	// of the VBTasksMap will be sent to DCP to be run and backfilled
	// Once all the VB Task's top task is done, then the backfill pipeline will be considered finished
	// (DCP will let Backfill Request Handler will know when each VB is done so the top task for the vb is removed)
	// And if there are more VBTasks, then another new pipeline will be launched start to handle the next sets of tasks
	bpCustomSettingMap := make(map[string]interface{})
	clonedTaskMap := backfillSpec.VBTasksMap.CloneWithSubsetVBs(mainPipelineVbs)
	bpCustomSettingMap[parts.DCP_VBTasksMap] = clonedTaskMap
	rep_status.SetCustomSettings(bpCustomSettingMap)

	pipelineMgr.logger.Infof("Backfill Pipeline %v is constructed. Starting it.", bp.InstanceId())
	bp.SetProgressRecorder(rep_status.RecordBackfillProgress)

	// Backfill pipeline should always run in low priority
	settings := rep_status.SettingsMap()
	// The settings here is a clone of the original setting + the custom settings called above
	settings[metadata.PriorityKey] = base.PriorityTypeLow

	errMap = bp.Start(settings)

	// Clear the settings once passed to backfill pipeline to prevent from going to main pipeline
	rep_status.ClearCustomSetting(parts.DCP_VBTasksMap)
	if len(errMap) > 0 {
		pipelineMgr.logger.Errorf("Failed to start the backfill pipeline %v", bp.InstanceId())
	}
	return errMap
}

func (pipelineMgr *PipelineManager) StopBackfillPipeline(topic string, skipCkpt bool) base.ErrorMap {
	errMap := make(base.ErrorMap)

	updater, rep_status, err := pipelineMgr.getUpdater(topic, nil)
	if err != nil {
		errMap["StopBackfillPipeline"] = err
		return errMap
	}

	// Execute callback regardless from this point on
	defer updater.executeQueuedBackfillCallbacks()

	bp := rep_status.BackfillPipeline()
	if bp == nil {
		pipelineMgr.logger.Infof("%v had no previously latched instance of backfill pipeline", topic)
		return errMap
	}

	pipelineMgr.logger.Infof("Stopping the backfill pipeline %s (skipCkpt? %v)", topic, skipCkpt)
	state := bp.State()
	if state != common.Pipeline_Stopping && state != common.Pipeline_Stopped {
		if skipCkpt {
			settingsMap := make(metadata.ReplicationSettingsMap)
			settingsMap[metadata.CkptMgrBypassCkpt] = true
			ckptMgr := bp.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
			if ckptMgr == nil {
				pipelineMgr.logger.Warnf("Unable to find ckptmgr to bypass ckpt for backfill pipeline %v", topic)
			} else {
				ckptMgr.UpdateSettings(settingsMap)
			}
		}

		errMap = bp.Stop()
		if len(errMap) > 0 {
			pipelineMgr.logger.Errorf("Received error(s) when stopping backfill pipeline %v - %v\n", topic, base.FlattenErrorMap(errMap))
			//pipeline failed to stopped gracefully in time. ignore the error.
			//the parts of the pipeline will eventually commit suicide.
		} else {
			pipelineMgr.logger.Infof("Backfill pipeline %v has been stopped\n", topic)
		}
		pipelineMgr.removePipelineFromReplicationStatus(bp)
		pipelineMgr.logger.Infof("Replication Status=%v\n", rep_status)
	} else {
		pipelineMgr.logger.Infof("Backfill Pipeline %v is not in the right state to be stopped. state=%v\n", topic, state)
	}

	return errMap
}

// Use this only for unit test
func (pipelineMgr *PipelineManager) GetLastUpdateResult(topic string) bool {
	repStatus, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil || repStatus == nil || repStatus.Updater() == nil {
		return false
	} else {
		updater := repStatus.Updater().(*PipelineUpdater)
		return updater.getLastResult()
	}
}

func (pipelineMgr *PipelineManager) ReInitStreams(pipelineName string) error {
	return pipelineMgr.serializer.ReInit(pipelineName)
}

// Bunch of getters
func (pipelineMgr *PipelineManager) GetRemoteClusterSvc() service_def.RemoteClusterSvc {
	return pipelineMgr.remote_cluster_svc
}

func (pipelineMgr *PipelineManager) GetLogSvc() service_def.UILogSvc {
	return pipelineMgr.uilog_svc
}

func (pipelineMgr *PipelineManager) GetReplSpecSvc() service_def.ReplicationSpecSvc {
	return pipelineMgr.repl_spec_svc
}

func (pipelineMgr *PipelineManager) GetXDCRTopologySvc() service_def.XDCRCompTopologySvc {
	return pipelineMgr.xdcr_topology_svc
}

func (pipelineMgr *PipelineManager) GetMainPipelineThroughSeqnos(topic string) (map[uint16]uint64, error) {
	pipeline := pipelineMgr.getPipelineFromMap(topic)
	if pipeline == nil {
		// If cannot find pipeline, this function is called when pipeline is stopping or starting up
		return nil, fmt.Errorf("%v - Unable to find pipeline %v", parts.PartStoppedError, topic)
	}
	runtimeCtx := pipeline.RuntimeContext()
	if runtimeCtx == nil {
		return nil, fmt.Errorf("Unable to find runtimeContext for %v", topic)
	}
	statsMgrSvc := runtimeCtx.Service(base.STATISTICS_MGR_SVC)
	statsMgr, ok := statsMgrSvc.(service_def.StatsMgrIface)
	if !ok {
		return nil, fmt.Errorf("Unable to cast stats mgr to StatsMgrIface")
	}
	return statsMgr.GetThroughSeqnosFromTsService(), nil
}

// In certain situations where ckpt mgr is stopping, and other routines need to read
// the checkpoint documents, call this method to make sure ckptMgr is stopped before reading
// checkpoints to prevent races
func (pipelineMgr *PipelineManager) WaitForMainPipelineCkptMgrToStop(topic, internalID string) {
	// Give it a safety timeout
	var safetTimerOnce sync.Once
	var timeoutFired uint32

	for {
		if atomic.LoadUint32(&timeoutFired) == 1 {
			return
		}

		replStatus, err := pipelineMgr.ReplicationStatus(topic)
		if err != nil {
			return
		}

		checkPipeline := replStatus.Pipeline()
		backfillPipeline := replStatus.BackfillPipeline()

		if (checkPipeline == nil || checkPipeline.Specification() == nil) &&
			(backfillPipeline == nil || backfillPipeline.Specification() == nil) {
			// No pipeline, means ckptMgr is stopped as of now
			return
		}

		var replSpec *metadata.ReplicationSpecification
		if checkPipeline != nil {
			replSpec = checkPipeline.Specification().GetReplicationSpec()
		} else {
			replSpec = backfillPipeline.Specification().GetReplicationSpec()
		}

		safetTimerOnce.Do(func() {
			// Get the current sample of how long it takes to commit to get a safety timer
			var milliSecondsToWait = int64(base.TimeoutPartsStop.Milliseconds()) // Default if we can't find any
			statsMgr := checkPipeline.RuntimeContext().Service(base.STATISTICS_MGR_SVC).(*pipeline_svc.StatisticsManager)
			committingTimeMs, err := statsMgr.GetCountMetrics(service_def.TIME_COMMITING_METRIC)
			if err == nil && committingTimeMs > milliSecondsToWait {
				milliSecondsToWait = committingTimeMs
			}
			// double the time for safety buffer
			milliSecondsToWait = 2 * milliSecondsToWait
			go func() {
				time.Sleep(time.Duration(milliSecondsToWait) * time.Millisecond)
				atomic.StoreUint32(&timeoutFired, 1)
			}()
		})

		if replSpec.InternalId != internalID {
			// Different instance of pipeline than requested, most likely a newer pipeline has started
			return
		}

		// Pipeline still present means ckptmgr still is running - check quickly again
		time.Sleep(base.PipelineSerializerRetryWaitTime)
	}
}

func (pipelineMgr *PipelineManager) UpdatePipelineWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	return pipelineMgr.serializer.UpdateWithStoppedCb(topic, callback, errCb)
}

// External
func (pipelineMgr *PipelineManager) DismissEventForPipeline(pipelineName string, eventId int) error {
	return pipelineMgr.serializer.DismissEvent(pipelineName, eventId)
}

// Internal
func (pipelineMgr *PipelineManager) DismissEvent(eventId int) error {
	repStatusMap := pipelineMgr.ReplicationStatusMap()
	for _, replStatus := range repStatusMap {
		if replStatus.GetEventsManager().ContainsEvent(eventId) {
			return replStatus.GetEventsManager().DismissEvent(eventId)
		}
	}
	return base.ErrorNotFound
}

// External
func (pipelineMgr *PipelineManager) BackfillMappingStatusUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestDelta []*metadata.CollectionsManifest) error {
	return pipelineMgr.serializer.BackfillMappingStatusUpdate(topic, diffPair, srcManifestDelta)
}

// Internal
func (pipelineMgr *PipelineManager) BackfillMappingUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error {
	repStatusMap := pipelineMgr.ReplicationStatusMap()
	replStatus, ok := repStatusMap[topic]
	if !ok {
		pipelineMgr.logger.Errorf("%v unable to find replication status %v to send backfill mapping update", topic)
		return base.ErrorNotFound
	}
	pipeline := replStatus.Pipeline()
	if pipeline != nil && pipeline.Type() == common.MainPipeline && pipeline.RuntimeContext() != nil {
		ckptMgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
		if ckptMgr != nil {
			settingsMap := make(metadata.ReplicationSettingsMap)
			settingsMap[metadata.CkptMgrBrokenmapIdleUpdateDiffPair] = diffPair
			settingsMap[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta] = srcManifestsDelta
			ckptMgr.UpdateSettings(settingsMap)
		}
	}
	return replStatus.GetEventsManager().BackfillUpdateCb(diffPair, srcManifestsDelta)
}

func (pipelineMgr *PipelineManager) HandlePeerCkptPush(fullTopic, sender string, dynamicEvt interface{}) error {
	// When peers send push requests, use the main pipeline's checkpoint manager to merge them
	// A peer node should only send a push request if the pipeline is running
	// If there's no source side network partition, this should not fail
	// so try for a bit before failing
	var mainPipelineCkptMgr pipeline_svc.CheckpointMgrSvc
	var backfillMgrSvc common.PipelineService
	var opFuncErr error
	opFunc := func() error {
		mainPipelineCkptMgr, backfillMgrSvc, opFuncErr = pipelineMgr.handlePeerCkptGetMergeManagers(fullTopic)
		return opFuncErr
	}

	ckptErr := pipelineMgr.utils.ExponentialBackoffExecutor(fmt.Sprintf("pipelineMgr.HandlePeerCkptPushGetMergeManagers(%v)", fullTopic),
		base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor, opFunc)
	if ckptErr == nil {
		ckptErr = mainPipelineCkptMgr.MergePeerNodesCkptInfo(dynamicEvt)
		if ckptErr != nil {
			pipelineMgr.logger.Errorf("%v - Unable to merge peer nodes ckpt %v", fullTopic, ckptErr)
		}
	}

	// Backfill replication can be sent while pipeline is paused, such as when mappings change
	mainPipelineTopic, _ := common.DecomposeFullTopic(fullTopic)
	settingsMap := make(metadata.ReplicationSettingsMap)
	settingsMap[base.NameKey] = mainPipelineTopic
	settingsMap[peerToPeer.PeriodicPushSenderKey] = sender
	settingsMap[peerToPeer.MergeBackfillKey] = dynamicEvt
	if backfillMgrSvc == nil {
		backfillMgrSvc = pipelineMgr.getBackfillMgr().GetPipelineSvc()
	}
	backfillMergeErr := backfillMgrSvc.UpdateSettings(settingsMap)

	if ckptErr == nil && backfillMergeErr == nil {
		return nil
	} else {
		return fmt.Errorf("%v - ckptMergeErr %v backfillMergeErr %v", fullTopic, ckptErr, backfillMergeErr)
	}
}

func (pipelineMgr *PipelineManager) handlePeerCkptGetMergeManagers(fullTopic string) (pipeline_svc.CheckpointMgrSvc, common.PipelineService, error) {
	topic, _ := common.DecomposeFullTopic(fullTopic)

	repStatus, err := pipelineMgr.ReplicationStatus(topic)
	if err != nil {
		// This shouldn't happen as repStatus should exist if spec exists. If this error shows up, it's a clue that
		// there is network partition and replicationSpec isn't sync'ed correctly
		return nil, nil, fmt.Errorf("Peer sent push req for pipeline %v but topic does not exist - %v", topic, err)
	}

	// Always get the main pipeline for merging purposes
	repStatusPipeline := repStatus.Pipeline()

	if repStatusPipeline == nil {
		// pipeline could be nil in between pipeline restarts restarts
		return nil, nil, base.ErrorNilPipeline
	}

	checkpointMgr, ok := repStatusPipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC).(pipeline_svc.CheckpointMgrSvc)
	if !ok {
		return nil, nil, base.ErrorNilPipeline
	}

	backfillMgrPipelineSvc := repStatusPipeline.RuntimeContext().Service(base.BACKFILL_MGR_SVC)
	if backfillMgrPipelineSvc == nil {
		return nil, nil, base.ErrorNilPipeline
	}
	return checkpointMgr, backfillMgrPipelineSvc, nil
}

// When topology changes occur on either source or target, this will update the event framework with
// a message that topology change has been detected, alongside an estimated time of pipeline restart
// This is useful because end users often do not see replication moving, but in reality we're just waiting
// for pipeline to restart. This is *particularly* useful for target side, as console UI won't know target status
func (pipelineMgr *PipelineManager) PostTopologyStatus() {
	var sourceHint = fmt.Sprintf("Hint for sourceTopology")
	var targetHint = fmt.Sprintf("Hint for targetTopology")

	replStatusMap := pipelineMgr.ReplicationStatusMap()
	for _, replStatus := range replStatusMap {
		// Main pipeline's GetRebalanceProgress is the source of truth
		// Check the replStatus event to ensure that it is lined up with the source
		// of truth. And if not, ensure that the right message is put forth
		// Once the pipeline restarts, the events will be reinitialized and so
		// there is no need to remove events, but rather just either
		// insert a new event when the first rebalance progress is shown
		// or update an existing event if the rebalance progress has been updated
		mainPipeline := replStatus.Pipeline()
		if mainPipeline == nil {
			continue
		}
		srcProgress, tgtProgress := mainPipeline.GetRebalanceProgress()
		if srcProgress == "" && tgtProgress == "" {
			// nothing to report
			continue
		}

		needToUpdateSrc := srcProgress != ""
		needToUpdateTgt := tgtProgress != ""
		var srcEventFound bool
		var srcEventId int64
		var tgtEventFound bool
		var tgtEventId int64

		eventMgr := replStatus.GetEventsManager()
		allEvents := eventMgr.GetCurrentEvents()
		allEvents.Mutex.RLock()
		for i := 0; i < allEvents.LenNoLock(); i++ {
			eventToCheck := allEvents.EventInfos[i]
			if hintStr, ok := eventToCheck.GetHint().(string); ok && hintStr == sourceHint {
				srcEventFound = true
				srcEventId = eventToCheck.EventId
				// Found source topology progress
				if srcProgress == eventToCheck.EventDesc {
					// Nothing has been updated since last posted
					needToUpdateSrc = false
				}
			}
			if hintStr, ok := eventToCheck.GetHint().(string); ok && hintStr == targetHint {
				tgtEventFound = true
				tgtEventId = eventToCheck.EventId
				// Found target topology progress
				if tgtProgress == eventToCheck.EventDesc {
					// Nothing has been updated since last posted
					needToUpdateTgt = false
				}
			}
		}
		allEvents.Mutex.RUnlock()

		if needToUpdateSrc {
			if srcEventFound {
				err := eventMgr.UpdateEvent(srcEventId, srcProgress, nil)
				if err != nil {
					pipelineMgr.logger.Errorf("Unable to update source topology rebalance progress: %v - %v", srcProgress, err)
				}
			} else {
				eventMgr.AddEvent(base.HighPriorityMsg, srcProgress, base.NewEventsMap(), sourceHint)
			}
		}
		if needToUpdateTgt {
			// Update existing event
			if tgtEventFound {
				err := eventMgr.UpdateEvent(tgtEventId, tgtProgress, nil)
				if err != nil {
					pipelineMgr.logger.Errorf("Unable to update target topology rebalance progress: %v - %v", tgtProgress, err)
				}
			} else {
				eventMgr.AddEvent(base.HighPriorityMsg, tgtProgress, base.NewEventsMap(), targetHint)
			}
		}
	}
}

var updaterStateErrorStr = "Can't move update state from %v to %v"

// unit test injection flags
const (
	pipelineUpdaterErrInjNil         int32 = iota
	pipelineUpdaterErrInjOfflineFail int32 = iota
)

// Masks for the disabledFeatures Flag
const (
	disabledCompression uint8 = 1
)

// This context records the last replication setting's revision. If it has changed from the last recorded time,
// it means that the replication setting has been updated and the disabled Features should be discarded
// Since only the updater (which is a singleton per replication) calls is in a single routine, no need for synchronization
type replSettingsRevContext struct {
	rep_status          pipeline.ReplicationStatusIface
	recordedRevision    interface{}
	recordedRevIsActive bool
}

func (h *replSettingsRevContext) Clear() {
	h.recordedRevIsActive = false
	h.recordedRevision = nil
}

func (h *replSettingsRevContext) GetCurrentSettingsRevision() interface{} {
	if h.rep_status != nil {
		spec := h.rep_status.Spec()
		if spec != nil {
			return spec.Settings.Revision
		}
	}
	return nil
}

// Sets the revision copy and also turns on a flag
func (h *replSettingsRevContext) Record() {
	h.recordedRevision = h.GetCurrentSettingsRevision()
	h.recordedRevIsActive = true
}

// Returns true if a recorded revision is active and the replSettings has changed since the last recorded revision
func (h *replSettingsRevContext) HasChanged() bool {
	if h.recordedRevIsActive {
		revToCheck := h.GetCurrentSettingsRevision()
		return !(reflect.DeepEqual(revToCheck, h.recordedRevision))
	}
	return false
}

type DisableCompressionReason int

const (
	DCNotDisabled  DisableCompressionReason = iota
	DCIncompatible DisableCompressionReason = iota
	DCDecompress   DisableCompressionReason = iota
	DCInvalid      DisableCompressionReason = iota
)

type updaterIntermediateCb struct {
	callback base.StoppedPipelineCallback
	errCb    base.StoppedPipelineErrCallback
}

type backfillStopChOpts struct {
	skipCheckpointing bool
	waitGrp           *sync.WaitGroup
}

// pipelineRepairer is responsible to repair a failing pipeline
// it will retry after the retry_interval
type PipelineUpdater struct {
	//the name of the pipeline to be repaired
	pipeline_name string
	//the interval to wait after the failure for next retry
	retry_interval time.Duration
	//finish channel
	fin_ch chan bool
	//update-now channel
	update_now_ch chan bool
	// update-error-map channel
	update_err_map_ch chan base.ErrorMap
	// Specialized request channel for cosumers asking for callback during updates
	updateWithCb chan updaterIntermediateCb
	// Specialized request channel for cosumers asking for callback during updates for backfill pipelines only
	backfillUpdateWithCb chan updaterIntermediateCb
	// channel indicating whether updater is really done
	done_ch chan bool
	// startBackfillChannel
	backfillStartCh chan bool
	// stopBackfillChannel
	backfillStopCh chan backfillStopChOpts
	// Backfill ErrorMap
	backfillErrMapCh chan base.ErrorMap

	//the current errors
	currentErrors *pmErrMapType
	// temporary map to catch any other errors while currentErrors are being processed
	// Note - lock ordering not to be violated: 1. overflowErrors 2. currentErrors
	overflowErrors *pmErrMapType

	// Pipeline Manager that created this updater
	pipelineMgr PipelineMgrForUpdater

	// should modify it via scheduleFutureRefresh() and cancelFutureRefresh()
	// Null if nothing is being scheduled
	scheduledTimerLock         sync.RWMutex
	scheduledTimer             *time.Timer
	backfillScheduledTimerLock sync.RWMutex
	backfillScheduledTimer     *time.Timer

	// boolean to store whether or not the last update was successful
	lastSuccessful bool

	rep_status          pipeline.ReplicationStatusIface
	logger              *log.CommonLogger
	pipelineUpdaterLock sync.RWMutex

	// A counter for keeping track how many times this updater has executed with allowable error codes
	runCounter uint64

	// Disabled Features - with a monitor context to go along with it
	disabledFeatures          uint8
	replSpecSettingsHelper    *replSettingsRevContext
	disabledCompressionReason DisableCompressionReason

	// Unit test only - should be in milliseconds - should be only read by unit tests
	testCustomScheduleTime      time.Duration
	testInjectionError          int32
	testCurrentDisabledFeatures uint8

	stopped     bool
	stoppedLock sync.Mutex
	runOnce     sync.Once

	humanRecoveryThresholdMtx   sync.Mutex
	humanRecoveryThresholdTimer *time.Timer

	updateRCStatusInterval time.Duration
	updateRCStatusTicker   *time.Ticker

	// The number of times that pipeline fails to start due to remote cluster auth error
	rcAuthErrCnt int
	// The number of times that the pipeline fails to start due to all the errors except the remote cluster auth error
	errCntExceptAuthErr int
}

type pmErrMapType struct {
	errMap    base.ErrorMap
	curErrMtx sync.RWMutex
	logger    *log.CommonLogger
}

func (em *pmErrMapType) IsEmpty() bool {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return len(em.errMap) == 0
}

func (em *pmErrMapType) Size() int {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return len(em.errMap)
}

func (em *pmErrMapType) Clear() {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	em.errMap = make(base.ErrorMap)
}

func (em *pmErrMapType) String() string {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return base.FlattenErrorMap(em.errMap)
}

func (em *pmErrMapType) LoadErrMap(newMap base.ErrorMap) {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	em.errMap = base.DeepCopyErrorMap(newMap)
}

func (em *pmErrMapType) ConcatenateErrors(incomingMap base.ErrorMap) {
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	base.ConcatenateErrors(em.errMap, incomingMap, MaxErrorDataEntries, em.logger)
}

// The behavior is to not overwrite keys - since traditionally Pipeline Manager's error handling
// is to register the first type of error and ignore the subsequent onces
func (em *pmErrMapType) AddError(source string, err error) error {
	if len(source) == 0 {
		return errors.New("Empty string")
	}
	em.curErrMtx.Lock()
	defer em.curErrMtx.Unlock()
	if _, ok := em.errMap[source]; !ok {
		if len(em.errMap) < MaxErrorDataEntries {
			em.errMap[source] = err
		} else {
			em.logger.Warnf("Failed to add error to error map since the error map is full. error discarded = %v", err)
		}
	}
	return nil
}

func (em *pmErrMapType) ContainsError(checkErr error, exactMatch bool) bool {
	em.curErrMtx.RLock()
	defer em.curErrMtx.RUnlock()
	return base.CheckErrorMapForError(em.errMap, checkErr, exactMatch)
}

func (em *pmErrMapType) ContainsMustStopError(replId string) (bool, string) {
	if em.ContainsError(ErrorCAPIReplicationDeprecated, false) {
		return true, fmt.Sprintf("Replication %v cannot continue - %v", replId, ErrorCAPIReplicationDeprecated.Error())
	}
	return false, ""
}

func newPipelineUpdater(pipeline_name string, retry_interval int, cur_err error, rep_status_in pipeline.ReplicationStatusIface, logger *log.CommonLogger, pipelineMgr_in *PipelineManager) *PipelineUpdater {
	if rep_status_in == nil {
		panic("nil ReplicationStatus")
	}
	repairer := &PipelineUpdater{pipeline_name: pipeline_name,
		retry_interval:         time.Duration(retry_interval) * time.Second,
		fin_ch:                 make(chan bool, 1),
		done_ch:                make(chan bool, 1),
		update_now_ch:          make(chan bool, 1),
		update_err_map_ch:      make(chan base.ErrorMap, 1),
		updateWithCb:           make(chan updaterIntermediateCb, MaxNonblockingQueueJobs),
		backfillUpdateWithCb:   make(chan updaterIntermediateCb, MaxNonblockingQueueJobs),
		backfillStartCh:        make(chan bool, 1),
		backfillStopCh:         make(chan backfillStopChOpts, 1),
		backfillErrMapCh:       make(chan base.ErrorMap, 1),
		rep_status:             rep_status_in,
		logger:                 logger,
		pipelineMgr:            pipelineMgr_in,
		lastSuccessful:         true,
		currentErrors:          &pmErrMapType{errMap: make(base.ErrorMap), logger: logger},
		overflowErrors:         &pmErrMapType{errMap: make(base.ErrorMap), logger: logger},
		replSpecSettingsHelper: &replSettingsRevContext{rep_status: rep_status_in},
		updateRCStatusInterval: base.RefreshRemoteClusterRefInterval,
		rcAuthErrCnt:           -1, // start with -1 as the first authErr does not require exp backoff
		errCntExceptAuthErr:    -1,
	}

	if cur_err != nil {
		repairer.currentErrors.AddError("newPipelineUpdater", cur_err)
	}
	return repairer
}

func (r *PipelineUpdater) getLastResult() bool {
	r.pipelineUpdaterLock.RLock()
	defer r.pipelineUpdaterLock.RUnlock()
	return r.lastSuccessful
}

// start the repairer
func (r *PipelineUpdater) run() {
	r.runOnce.Do(func() {
		defer close(r.done_ch)

		var retErr error
		var retErrMap base.ErrorMap

		r.stoppedLock.Lock()
		r.updateRCStatusTicker = time.NewTicker(r.updateRCStatusInterval)
		r.stoppedLock.Unlock()

		for {
			select {
			case <-r.fin_ch:
				r.logger.Infof("Quit updating pipeline %v after updating a total of %v times\n", r.pipeline_name, atomic.LoadUint64(&r.runCounter))
				r.logger.Infof("Replication %v's status is to be closed, shutting down\n", r.pipeline_name)
				r.pipelineMgr.StopPipeline(r.rep_status)
				// Reset runCounter to 0 for unit test
				atomic.StoreUint64(&r.runCounter, 0)
				r.logger.Infof("Replication %v status is finished shutting down\n", r.pipeline_name)
				return
			case <-r.update_now_ch:
				r.logger.Infof("Replication %v's status is changed, update now\n", r.pipeline_name)
				retErrMap = r.update()
				r.cancelFutureRefresh()
				if len(retErrMap) > 0 {
					r.logger.Infof("Replication %v update experienced error(s): %v. Scheduling a redo.\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
					r.sendUpdateErrMap(retErrMap)
				} else if r.callbacksStillRemain() {
					r.logger.Infof("There are still callback requests that need to run. Will need to update pipeline %v again", r.pipeline_name)
					r.sendUpdateNow()
				}
			case retErrMap = <-r.update_err_map_ch:
				r.currentErrors.ConcatenateErrors(retErrMap)
				updateAgain := r.checkIfNeedToReUpdate(retErrMap, retErr)
				if updateAgain {
					r.scheduleFutureRefresh()
				}
			case <-r.backfillStartCh:
				r.logger.Infof("Replication %v's backfill Pipeline is starting\n", r.pipeline_name)
				r.cancelFutureBackfillStart()
				retErrMap = r.pipelineMgr.StartBackfillPipeline(r.pipeline_name)
				if !backfillStartSuccessful(retErrMap, r.logger, r.pipeline_name) {
					r.logger.Infof("Replication %v backfill start experienced error(s): %v. Scheduling a redo.\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
					r.setLastUpdateFailure(retErrMap)
					r.sendBackfillStartErrMap(retErrMap)
				} else {
					r.setLastUpdateSuccess()
				}
			case opts := <-r.backfillStopCh:
				if r.rep_status == nil || r.rep_status.BackfillPipeline() == nil {
					r.logger.Warnf("Backfill pipeline for %v is already stopped", r.pipeline_name)
					if r.checkReplicationActiveness() == base.ErrorReplicationSpecNotActive {
						// Do not let any potential restart timers fire after a permanent stop has been issued
						stoppedInTime := r.cancelFutureBackfillStart()
						if !stoppedInTime {
							// This run() is currently executing this StopCh. Soak up the backfillStartCh
							// to ensure that this pipeline doesn't go rogue and start since the replication
							// has indicated that it shouldn't run
							r.soakUpBackfillStartCh()
						}
					}
					// With P2P, callbacks are registered in a channel and called after backfill pipeline stops.
					// Here backfill pipeline is already stopped. We need to clear the callbacks in the channel.
					r.executeQueuedCallbacks()
				} else {
					r.logger.Infof("Replication %v's backfill Pipeline is stopping (skipCkpt? %v)", r.pipeline_name, opts.skipCheckpointing)
					retErrMap = r.pipelineMgr.StopBackfillPipeline(r.pipeline_name, opts.skipCheckpointing)
					if len(retErrMap) > 0 {
						r.logger.Infof("Replication %v backfill stop experienced error(s): %v. Will let it die\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
					}
				}
				opts.waitGrp.Done()
			case retErrMap = <-r.backfillErrMapCh:
				var updateAgain bool
				if r.getLastResult() {
					// Last time update succeeded, so this error then triggers an immediate update
					r.logger.Infof("Backfill Replication %v's status experienced changes or errors (%v), re-starting now\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
					retErrMap = r.pipelineMgr.StartBackfillPipeline(r.pipeline_name)
					r.cancelFutureBackfillStart()
					if backfillStartSuccessful(retErrMap, r.logger, r.pipeline_name) {
						r.setLastUpdateSuccess()
					} else {
						updateAgain = true
						r.setLastUpdateFailure(retErrMap)
					}
				} else {
					r.logger.Infof("Backfill Replication %v's status experienced changes or errors (%v). However, last update resulted in failure, so will reschedule a future update\n", r.pipeline_name, retErr)
					// Last time update failed, so we should wait for stablization
					updateAgain = true
				}
				if updateAgain {
					r.scheduleFutureBackfillStart()
				}
			case <-r.updateRCStatusTicker.C:
				// Only check connectivity status for KV nodes
				isMyNodeKVNode, err := r.pipelineMgr.GetXDCRTopologySvc().IsKVNode()
				if err == nil && isMyNodeKVNode {
					// Check if connectivity has issues - and if so, raise an error if it's not there already
					spec := r.rep_status.Spec()
					// Spec may be nil when pipeline is stopping
					if spec != nil {
						ref, err := r.pipelineMgr.GetRemoteClusterSvc().RemoteClusterByUuid(spec.TargetClusterUUID, false)
						if err != nil {
							r.logger.Errorf("Pipeline updater received err %v when retrieving its remote cluster")
						} else {
							connectivityStatus, err := r.pipelineMgr.GetRemoteClusterSvc().GetConnectivityStatus(ref)
							if err != nil {
								r.logger.Errorf("Pipeline updater received err %v when retrieving its remote cluster's connectivity status")
							} else {
								r.checkAndPublishRCError(connectivityStatus, ref)
								if connectivityStatus == metadata.ConnIniting || connectivityStatus == metadata.ConnValid {
									ref.ClearConnErrs()
								}
								ref.PrintConnErrs(r.logger)
							}
						}
					}
				}
			}
		}
	})
}

func (r *PipelineUpdater) soakUpBackfillStartCh() {
	select {
	case <-r.backfillStartCh:
	// do nothing
	default:
		break
	}
}

func backfillStartSuccessful(retErrMap base.ErrorMap, logger *log.CommonLogger, pipelineName string) bool {
	var successful = true
	if len(retErrMap) > 0 {
		if retErrMap.HasError(MainPipelineNotRunning) {
			logger.Infof("Replication %v backfill pipeline is not starting because the main pipeline is currently paused", pipelineName)
		} else if retErrMap.HasError(base.ReplNotFoundErr) {
			logger.Infof("Replication %v backfill pipeline is not starting because there is currently no backfill spec", pipelineName)
		} else if retErrMap.HasError(ErrorBackfillSpecHasNoVBTasks) {
			logger.Infof("Replication %v's backfill spec does has no tasks to run", pipelineName)
		} else if len(retErrMap) == 1 && retErrMap.HasError(base.ErrorNoBackfillNeeded) {
			logger.Infof("Replication %v backfill pipeline is not starting because backfill tasks have all been done", pipelineName)
		} else if len(retErrMap) == 1 && retErrMap.HasError(ReplicationStatusNotFound) {
			logger.Infof("Replication %v backfill pipeline is not starting spec is deleted", pipelineName)
		} else {
			successful = false
		}
	}
	return successful
}

func (r *PipelineUpdater) checkIfNeedToReUpdate(retErrMap base.ErrorMap, retErr error) bool {
	var updateAgain bool

	remoteClusterConfigChanged := retErrMap.HasError(base.ErrorPipelineRestartDueToClusterConfigChange)

	if r.getLastResult() || remoteClusterConfigChanged {
		// Last time update succeeded, so this error then triggers an immediate update
		r.logger.Infof("Replication %v's status experienced changes or errors (%v), updating now\n", r.pipeline_name, base.FlattenErrorMap(retErrMap))
		retErrMap = r.update()
		r.cancelFutureRefresh()
		if len(retErrMap) > 0 {
			updateAgain = true
		}
	} else {
		r.logger.Infof("Replication %v's status experienced changes or errors (%v). However, last update resulted in failure, so will reschedule a future update\n", r.pipeline_name, retErr)
		// Last time update failed, so we should wait for stablization
		updateAgain = true
	}
	if r.callbacksStillRemain() {
		r.logger.Infof("There are still callback requests that need to run. Will need to update pipeline %v again", r.pipeline_name)
		updateAgain = true
	}
	return updateAgain
}

func (r *PipelineUpdater) callbacksStillRemain() bool {
	return len(r.updateWithCb) > 0 || len(r.backfillUpdateWithCb) > 0
}

func allErrorsAreAllowed(errMap base.ErrorMap) bool {
	for _, v := range errMap {
		if !allowableErrorCodes(v) {
			return false
		}
	}
	return true
}

func allowableErrorCodes(err error) bool {
	if err == nil ||
		err == base.ErrorReplicationSpecNotActive ||
		err == service_def.MetadataNotFoundErr ||
		err == ErrorNoKVService {
		return true
	}
	return false
}

// If supported, one by one disable the features that are allowed to be disabled so that
// the pipeline can eventually start. These temporary changes are cleared when clearErrors() is called later
func (r *PipelineUpdater) checkAndDisableProblematicFeatures() {
	// First make sure that no one has changed replicationSettings from underneath us
	if r.replSpecSettingsHelper.HasChanged() {
		r.replSpecSettingsHelper.Clear()
	} else if r.currentErrors != nil {
		if r.currentErrors.ContainsError(base.ErrorCompressionNotSupported, true /*exactMatch*/) {
			r.disableCompression(base.ErrorCompressionNotSupported)
		} else if r.currentErrors.ContainsError(base.ErrorCompressionUnableToInflate, false /*exactMatch*/) {
			r.disableCompression(base.ErrorCompressionUnableToInflate)

		}

		// Not exactly disable, but logic here dictates some action here
		if r.currentErrors.ContainsError(base.ErrorXmemCollectionSubErr, false /*exactMatch*/) ||
			r.currentErrors.ContainsError(parts.ErrorXmemIsStuck, false /*exactMatch*/) {
			// Xmem is stuck or collection error can only be declared after an elongated period of xmem timeout
			// It's more frequent than regular refresh interval, but not too frequent to bombard target ns_server
			// May need to revisit if XDCR number of replication scales to a lot and all of them have the same issue
			spec := r.rep_status.Spec()
			if spec != nil {
				err := r.pipelineMgr.ForceTargetRefreshManifest(spec)
				if err != nil {
					r.logger.Errorf("Unable to force target to refresh collections manifest: %v", err)
				}
			}
		}
	}
}

func (r *PipelineUpdater) disableCompression(reason error) {
	if r.rep_status == nil {
		r.logger.Errorf(fmt.Sprintf("Unable to disable compression for pipeline %v - rep_status is nil", r.pipeline_name))
		return
	}

	r.logger.Infof("Temporarily disabling compression for pipeline: %v", r.pipeline_name)
	settings := make(map[string]interface{})
	settings[base.CompressionTypeKey] = (int)(base.CompressionTypeForceUncompress)
	r.rep_status.SetCustomSettings(settings)

	switch reason {
	case base.ErrorCompressionNotSupported:
		r.disabledCompressionReason = DCIncompatible
	case base.ErrorCompressionUnableToInflate:
		r.disabledCompressionReason = DCDecompress
	default:
		r.disabledCompressionReason = DCInvalid
	}

	r.disabledFeatures |= disabledCompression
	// For unit test only
	r.testCurrentDisabledFeatures = r.disabledFeatures
}

func (r *PipelineUpdater) resetDisabledFeatures() {
	r.rep_status.ClearTemporaryCustomSettings()
	if r.disabledFeatures > 0 {
		r.logger.Infof("Restoring compression for pipeline: %v the next time it restarts", r.pipeline_name)
		r.disabledFeatures = 0
		r.disabledCompressionReason = DCNotDisabled
	}
}

// update the pipeline
func (r *PipelineUpdater) update() base.ErrorMap {
	var err error
	var errMap base.ErrorMap
	var checkReplicationActivenessKey string = "r.CheckReplicationActiveness"

	r.pickupOverflowErrors()

	if r.currentErrors.IsEmpty() {
		r.logger.Infof("Try to start/restart Pipeline %v. \n", r.pipeline_name)
	} else if containsMustStopErr, errStr := r.currentErrors.ContainsMustStopError(r.pipeline_name); containsMustStopErr {
		r.logger.Errorf(errStr)
		r.pipelineMgr.GetLogSvc().Write(errStr)
		r.pipelineMgr.AutoPauseReplication(r.pipeline_name)
		r.clearErrors()
		return nil
	} else {
		r.logger.Infof("Try to fix Pipeline %v. Current error(s)=%v \n", r.pipeline_name, r.currentErrors.String())
		r.checkAndDisableProblematicFeatures()
	}

	// Store the current revision of the actual replicationSpec's setting. If things changed from underneath,
	// before we kick-start another update, we'll know at the start of our next update
	r.replSpecSettingsHelper.Record()

	r.logger.Infof("Try to stop pipeline %v\n", r.pipeline_name)
	errMap = r.pipelineMgr.StopPipeline(r.rep_status)

	// unit test error injection
	if atomic.LoadInt32(&r.testInjectionError) == pipelineUpdaterErrInjOfflineFail {
		r.logger.Infof("Error injected... this should only happen during unit test")
		// make a new map so we don't modify the mock PM's errMap reference above
		injectedErrMap := make(base.ErrorMap)
		injectedErrMap["InjectedError"] = errors.New("Injected Offline failure")
		errMap = injectedErrMap
	}
	// end unit test error injection

	if len(errMap) > 0 {
		// If pipeline fails to stop, everything within it should "commit suicide" eventually
		goto RE
	}

	// For unit test, perhaps sleep to induce some scheduling
	time.Sleep(time.Duration(100) * time.Nanosecond)

	err = r.checkReplicationActiveness()
	if err != nil {
		errMap[checkReplicationActivenessKey] = err
		goto RE
	}

	// If callbacks need to be called before pipeline start again, call them
	r.executeQueuedCallbacks()

	errMap = r.pipelineMgr.StartPipeline(r.pipeline_name)

RE:
	if len(errMap) == 0 {
		r.logger.Infof("Replication %v has been updated. Back to business\n", r.pipeline_name)
	} else if base.CheckErrorMapForError(errMap, base.ErrorReplicationSpecNotActive, true /*exactMatch*/) {
		r.logger.Infof("Replication %v has been paused. no need to update\n", r.pipeline_name)
		// It is possible that pipeline is set "paused" and won't be started again so now is the time
		// to execute callbacks if any
		r.executeQueuedCallbacks()
	} else if base.CheckErrorMapForError(errMap, service_def.MetadataNotFoundErr, true /*exactMatch */) {
		r.logger.Infof("Replication %v has been deleted. no need to update\n", r.pipeline_name)
	} else if base.CheckErrorMapForError(errMap, ErrorNoKVService, true) {
		// MB-15357 documents the inability to dynamically add KV service to a node without a KV service
		// If it gets fixed in the future and KV service can be added, any vb rebalancing should trigger pipeline restart
		// and there should not be any need to fix anything
		msg := fmt.Sprintf("Replication %v is unable to start on this node because it does not have data service", r.pipeline_name)
		r.logger.Warnf(msg)
		if r.rep_status != nil {
			// Set this to nil will enforce a clean overview "running" stats
			r.rep_status.SetOverviewStats(nil, common.MainPipeline)
		}
	} else if base.CheckErrorMapForError(errMap, service_def.ErrorSourceDefaultCollectionDNE, false /*exactMatch*/) {
		dneErr := fmt.Sprintf("Replication %v cannot continue because target does not support collections and the source bucket's default collection is removed. Replication will automatically be paused.\n", r.pipeline_name)
		r.logger.Errorf(dneErr)
		r.pipelineMgr.GetLogSvc().Write(dneErr)
		r.pipelineMgr.AutoPauseReplication(r.pipeline_name)
		return nil
	} else if base.CheckErrorMapForError(errMap, errors.New(base.RemoteClusterAuthErrString), false) {
		shouldRetry, remoteClusterName := r.shouldRetryOnRemoteAuthErrAndRemoteClusterName()
		if !shouldRetry {
			stopErr := getCredentialErrPauseMsg(remoteClusterName, r.pipeline_name)
			r.logger.Errorf(stopErr)
			r.pipelineMgr.GetLogSvc().Write(stopErr)
			r.pipelineMgr.AutoPauseReplication(r.pipeline_name)
		} else {
			replaceErrWithHumanReadableString(errMap, r.pipeline_name, remoteClusterName)
			r.rcAuthErrCnt++
		}
	} else {
		r.errCntExceptAuthErr++
		r.logger.Errorf("Failed to update pipeline %v, err=%v\n", r.pipeline_name, base.FlattenErrorMap(errMap))
	}

	if allErrorsAreAllowed(errMap) {
		r.logger.Infof("Pipeline %v has been updated successfully\n", r.pipeline_name)
		r.setLastUpdateSuccess()
		if len(errMap) == 0 {
			r.raiseWarningsIfNeeded()
		}
		if r.rep_status != nil {
			r.rep_status.ClearErrors()
			r.rep_status.GetEventsManager().ClearNonBrokenMapEventsWithString(common.ProgressP2PComm)
		}
		r.clearErrors()
		errMap = make(base.ErrorMap) // clear errMap
		atomic.AddUint64(&r.runCounter, 1)
		r.resetDisabledFeatures() // also clears custom settings
	} else {
		r.setLastUpdateFailure(errMap)
		r.logger.Errorf("Update of pipeline %v failed with errors=%v\n", r.pipeline_name, base.FlattenErrorMap(errMap))
	}
	r.reportStatus()

	return errMap
}

func replaceErrWithHumanReadableString(errMap base.ErrorMap, pipelineName string, rcName string) {
	for k, v := range errMap {
		if strings.Contains(v.Error(), base.RemoteClusterAuthErrString) {
			errMap[k] = errors.New(getCredentialErrMsg(rcName, pipelineName))
		}
	}
}

// pick up overflow errors that were not captured by the channel
func (r *PipelineUpdater) pickupOverflowErrors() {
	r.overflowErrors.curErrMtx.Lock()
	defer r.overflowErrors.curErrMtx.Unlock()
	// If current errors map is too full, then these overflow errors may be missed
	r.currentErrors.ConcatenateErrors(r.overflowErrors.errMap)
	r.overflowErrors.errMap = make(base.ErrorMap)
}

func (r *PipelineUpdater) reportStatus() {
	if r.rep_status != nil {
		r.currentErrors.curErrMtx.RLock()
		defer r.currentErrors.curErrMtx.RUnlock()
		r.rep_status.AddErrorsFromMap(r.currentErrors.errMap)
	}
}

func (r *PipelineUpdater) raiseWarningsIfNeeded() {
	r.raiseCompressionWarningIfNeeded()
	r.raiseRemoteClusterRestartReasonsIfNeeded()
	r.raiseNonCollectionTargetIfNeeded()
}

func (r *PipelineUpdater) raiseRemoteClusterRestartReasonsIfNeeded() {
	if !r.currentErrors.ContainsError(base.ErrorPipelineRestartDueToClusterConfigChange, true /*exactMatch*/) {
		return
	}
	spec := r.rep_status.Spec()
	if spec == nil {
		return
	}
	if spec.Settings.IsCapi() {
		return
	}
	targetClusterRef, err := r.pipelineMgr.GetRemoteClusterSvc().RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil || targetClusterRef == nil {
		return
	}
	errMsg := fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' on cluster '%v' has been restarted due to remote-cluster configuration changes\n", spec.SourceBucketName, spec.TargetBucketName, targetClusterRef.Name())
	r.logger.Warnf(errMsg)
	r.pipelineMgr.GetLogSvc().Write(errMsg)
}

func (r *PipelineUpdater) raiseNonCollectionTargetIfNeeded() {
	spec := r.rep_status.Spec()
	if spec == nil {
		return
	}

	targetClusterRef, err := r.pipelineMgr.GetRemoteClusterSvc().RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil || targetClusterRef == nil {
		return
	}

	capability, err := r.pipelineMgr.GetRemoteClusterSvc().GetCapability(targetClusterRef)
	if err != nil {
		return
	}
	if capability.HasCollectionSupport() {
		// Target has collection support, no warnings needed
		return
	}
	// Raise the following only if verified that target has no collection support
	errMsg := fmt.Sprintf("Only default collection from source bucket '%v' to target bucket '%v' on cluster '%v' is being replicated because '%v' does not have collections capability\n", spec.SourceBucketName, spec.TargetBucketName, targetClusterRef.Name(), targetClusterRef.Name())
	r.logger.Warnf(errMsg)
	r.pipelineMgr.GetLogSvc().Write(errMsg)
	r.rep_status.GetEventsManager().AddEvent(base.HighPriorityMsg, errMsg, base.NewEventsMap(), nil)
}

func (r *PipelineUpdater) raiseCompressionWarningIfNeeded() {
	if r.disabledFeatures > 0 && (r.disabledFeatures&disabledCompression > 0) {
		spec := r.rep_status.Spec()
		if spec == nil {
			return
		}
		var errMsg string
		switch r.disabledCompressionReason {
		case DCIncompatible:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to errors that occurred during clusters handshaking. This may result in more data bandwidth usage. Please ensure that both the source and target clusters are at or above version %v.%v, and restart the replication.",
				spec.SourceBucketName, spec.TargetBucketName, base.VersionForCompressionSupport[0], base.VersionForCompressionSupport[1])
		case DCDecompress:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to errors that occurred while attempting to decompress data for filtering. This may result in more data bandwidth usage. Please check the XDCR error logs for more information.",
				spec.SourceBucketName, spec.TargetBucketName)
		default:
			errMsg = fmt.Sprintf("Replication from source bucket '%v' to target bucket '%v' has been started without compression enabled due to unknown errors. This may result in more data bandwidth usage.",
				spec.SourceBucketName, spec.TargetBucketName)
		}
		r.logger.Warn(errMsg)
		r.pipelineMgr.GetLogSvc().Write(errMsg)
	}
}

func (r *PipelineUpdater) checkReplicationActiveness() (err error) {
	spec, err := r.pipelineMgr.GetReplSpecSvc().ReplicationSpec(r.pipeline_name)
	if err != nil || spec == nil || !spec.Settings.Active {
		err = base.ErrorReplicationSpecNotActive
	}
	return
}

func (r *PipelineUpdater) stop() error {
	err := r.setStopped()
	if err != nil {
		return err
	}

	close(r.fin_ch)

	// wait for updater to really stop
	<-r.done_ch
	return nil
}

func (r *PipelineUpdater) setStopped() error {
	r.stoppedLock.Lock()
	defer r.stoppedLock.Unlock()
	if r.stopped {
		return UpdaterStoppedError
	}
	r.stopped = true
	if r.updateRCStatusTicker != nil {
		r.updateRCStatusTicker.Stop()
	}
	return nil
}

// This method will update the replicationStatus immediately
func (r *PipelineUpdater) refreshPipelineManually() {
	r.sendUpdateNow()
}

func (r *PipelineUpdater) refreshPipelineDueToErr(err error) {
	r.sendUpdateErr(err)
}

func (r *PipelineUpdater) startBackfillPipeline() {
	r.sendStartBackfillPipeline()
}

func (r *PipelineUpdater) stopBackfillPipeline(skipCkpt bool) {
	r.sendStopBackfillPipeline(skipCkpt)
}

// Lock must be held
func (r *PipelineUpdater) sendUpdateNow() {
	select {
	case r.update_now_ch <- true:
	default:
		r.logger.Infof("Update-now message is already delivered for %v\n", r.pipeline_name)
	}
	r.logger.Infof("Replication status is updated now, current status=%v\n", r.rep_status)
}

func (r *PipelineUpdater) sendStartBackfillPipeline() {
	select {
	case r.backfillStartCh <- true:
	default:
		r.logger.Infof("BackfillStart-now message is already delivered for %v\n", r.pipeline_name)
	}
	r.logger.Infof("Replication status received startBackfill, current status=%v\n", r.rep_status)
}

func (r *PipelineUpdater) sendStopBackfillPipeline(skipCkpt bool) {
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	opts := backfillStopChOpts{
		skipCheckpointing: skipCkpt,
		waitGrp:           waitGrp,
	}
	select {
	case r.backfillStopCh <- opts:
	}
	r.logger.Infof("Replication status received stopBackfill, current status=%v", r.rep_status)
	waitGrp.Wait()
}

func (r *PipelineUpdater) sendUpdateErr(err error) {
	oneErrorInMap := make(base.ErrorMap)
	oneErrorInMap["r.update_err_ch"] = err
	r.sendUpdateErrMap(oneErrorInMap)
}

func (r *PipelineUpdater) sendUpdateErrMap(errMap base.ErrorMap) {
	select {
	case r.update_err_map_ch <- errMap:
	default:
		r.overflowErrors.ConcatenateErrors(errMap)
		r.logger.Infof("Updater is currently running. Errors are captured in overflow map, and may be picked up if the current pipeline restart fails")
	}
	r.logger.Infof("Replication status is updated with error(s) %v, current status=%v\n", base.FlattenErrorMap(errMap), r.rep_status)
}

func (r *PipelineUpdater) sendBackfillStartErrMap(errMap base.ErrorMap) {
	select {
	case r.backfillErrMapCh <- errMap:
	default:
		r.overflowErrors.ConcatenateErrors(errMap)
		r.logger.Infof("Updater is currently running. Errors are captured in overflow map, and may be picked up if the current pipeline restart fails")
	}
	r.logger.Infof("Replication status is updated with backfill error(s) %v, current status=%v\n", base.FlattenErrorMap(errMap), r.rep_status)
}

func (r *PipelineUpdater) isScheduledTimerNil() bool {
	r.scheduledTimerLock.RLock()
	defer r.scheduledTimerLock.RUnlock()
	return r.scheduledTimer == nil
}

func getErrCntAndMaxWait(rcAuthErrCnt int, errCntExceptAuthErr int, spec *metadata.ReplicationSpecification) (int, time.Duration) {
	maxWaitForAuthErr := spec.Settings.GetRetryOnRemoteAuthErrMaxWait()
	maxWaitForErrExceptAuthErr := spec.Settings.GetRetryOnErrExceptAuthErrMaxWait()
	if rcAuthErrCnt >= 0 && errCntExceptAuthErr >= 0 {
		// returns the maximum wait time out of both
		if maxWaitForAuthErr > maxWaitForErrExceptAuthErr {
			return rcAuthErrCnt, maxWaitForAuthErr
		}
		return errCntExceptAuthErr, maxWaitForErrExceptAuthErr
	} else if rcAuthErrCnt >= 0 {
		return rcAuthErrCnt, maxWaitForAuthErr
	} else if errCntExceptAuthErr >= 0 {
		return errCntExceptAuthErr, maxWaitForErrExceptAuthErr
	}
	return -1, maxWaitForAuthErr
}

func (r *PipelineUpdater) getFutureRefreshDuration() time.Duration {
	spec := r.rep_status.Spec()
	if spec == nil {
		// Doesn't really matter as spec is deleted
		return r.retry_interval
	}

	errCnt, maxWaitTime := getErrCntAndMaxWait(r.rcAuthErrCnt, r.errCntExceptAuthErr, spec)
	if errCnt <= 0 {
		return r.retry_interval
	}

	// Do exponential backoff of 2 until a largest retry constant
	backoffRetryInterval := r.retry_interval
	for i := 0; i < errCnt; i++ {
		if backoffRetryInterval*2 < maxWaitTime {
			backoffRetryInterval = backoffRetryInterval * 2
		} else {
			break
		}
	}
	return backoffRetryInterval
}

func (r *PipelineUpdater) scheduleFutureRefresh() {
	scheduledTimeMs := r.getFutureRefreshDuration()
	r.scheduledTimerLock.Lock()
	defer r.scheduledTimerLock.Unlock()

	if r.testCustomScheduleTime > 0 {
		scheduledTimeMs = r.testCustomScheduleTime
		r.logger.Infof("Running unit test with custom sleep time of: %v\n", scheduledTimeMs)
	}

	if r.scheduledTimer == nil {
		r.logger.Infof("Pipeline updater scheduled to update in %v\n", scheduledTimeMs)
		r.scheduledTimer = time.AfterFunc(scheduledTimeMs, func() {
			r.logger.Infof("Pipeline updater scheduled time is up. Executing pipeline updater\n")
			r.sendUpdateNow()
		})
	}
}

func (r *PipelineUpdater) scheduleFutureBackfillStart() {
	var scheduledTimeMs time.Duration = r.retry_interval
	r.backfillScheduledTimerLock.Lock()
	defer r.backfillScheduledTimerLock.Unlock()

	if r.testCustomScheduleTime > 0 {
		scheduledTimeMs = r.testCustomScheduleTime
		r.logger.Infof("Running unit test with custom sleep time of: %v\n", scheduledTimeMs)
	}

	if r.backfillScheduledTimer == nil {
		r.logger.Infof("Pipeline updater scheduled to start backfill pipeline in %v\n", scheduledTimeMs)
		r.backfillScheduledTimer = time.AfterFunc(scheduledTimeMs, func() {
			r.logger.Infof("Backfill Pipeline updater scheduled time is up. Executing pipeline updater\n")
			r.sendStartBackfillPipeline()
		})
	}
}

func (r *PipelineUpdater) setLastUpdateSuccess() {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()
	r.lastSuccessful = true
	r.rcAuthErrCnt = -1
	r.errCntExceptAuthErr = -1
	r.clearHumanRecoveryTimer()
}

func (r *PipelineUpdater) clearHumanRecoveryTimer() {
	r.humanRecoveryThresholdMtx.Lock()
	if r.humanRecoveryThresholdTimer != nil {
		stoppedInTime := r.humanRecoveryThresholdTimer.Stop()
		r.humanRecoveryThresholdTimer = nil
		if !stoppedInTime {
			r.logger.Warnf("%v human recovery timer did not stop in time. Will need to restart pipeline manually")
		}
	}
	r.humanRecoveryThresholdMtx.Unlock()
}

func (r *PipelineUpdater) setLastUpdateFailure(errs base.ErrorMap) {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()
	r.lastSuccessful = false
	r.currentErrors.LoadErrMap(errs)
	if r.humanRecoverableTransitionalErrors(errs) {
		r.humanRecoveryThresholdMtx.Lock()
		if r.humanRecoveryThresholdTimer == nil {
			r.humanRecoveryThresholdTimer = time.AfterFunc(base.HumanRecoveryThreshold, func() {
				errMsg := fmt.Sprintf("Replication %v cannot continue after %v because of the errors that require manual recovery (%v). Please fix them and then restart the replication",
					r.pipeline_name, base.HumanRecoveryThreshold, base.FlattenErrorMap(errs))
				r.logger.Warnf(errMsg)
				r.pipelineMgr.GetLogSvc().Write(errMsg)
				r.rep_status.GetEventsManager().AddEvent(base.PersistentMsg, errMsg, base.NewEventsMap(), nil)
				r.pipelineMgr.AutoPauseReplication(r.pipeline_name)
			})
		}
		r.humanRecoveryThresholdMtx.Unlock()
	} else if r.casPoisonErrors(errs) {
		errMsg := base.FlattenErrorMap(errs)
		r.pipelineMgr.GetLogSvc().Write(errMsg)
		r.rep_status.GetEventsManager().AddEvent(base.PersistentMsg, errMsg, base.NewEventsMap(), nil)
		r.pipelineMgr.AutoPauseReplication(r.pipeline_name)
	}
}

// Stopped == true means that we stopped it in time
// If stopped is false it means that the timer fired during the window of when we're trying to check
func (r *PipelineUpdater) cancelFutureRefresh() bool {
	var stopped bool
	r.scheduledTimerLock.Lock()
	defer r.scheduledTimerLock.Unlock()
	if r.scheduledTimer != nil {
		stopped = r.scheduledTimer.Stop()
		r.scheduledTimer = nil
	} else {
		// If no timer, pretend we stopped it in time
		stopped = true
	}
	return stopped
}

func (r *PipelineUpdater) cancelFutureBackfillStart() bool {
	var stopped bool
	r.backfillScheduledTimerLock.Lock()
	defer r.backfillScheduledTimerLock.Unlock()
	if r.backfillScheduledTimer != nil {
		stopped = r.backfillScheduledTimer.Stop()
		r.backfillScheduledTimer = nil
	} else {
		// If no timer, pretend we stopped it in time
		stopped = true
	}
	return stopped
}

func (r *PipelineUpdater) clearErrors() {
	r.pipelineUpdaterLock.Lock()
	defer r.pipelineUpdaterLock.Unlock()

	r.currentErrors.Clear()
	r.replSpecSettingsHelper.Clear()
	r.overflowErrors.Clear()
}

func getRequestPoolSize(rep_status *pipeline.ReplicationStatus, numOfTargetNozzles int) int {
	return rep_status.Spec().Settings.BatchCount * 52 * numOfTargetNozzles
}

func (r *PipelineUpdater) registerPipelineStoppedCb(cb updaterIntermediateCb) error {
	select {
	case r.updateWithCb <- cb:
		r.logger.Infof("pipeline %v updater received callback registration. Current callback count: %v", r.pipeline_name, len(r.updateWithCb))
		r.sendUpdateNow()
		return nil
	default:
		r.logger.Errorf("pipeline %v updater unable to register callback because channel is full", r.pipeline_name)
		return base.ErrorMaxReached
	}
}

func (r *PipelineUpdater) registerBackfillPipelineStoppedCb(cb updaterIntermediateCb) error {
	select {
	case r.backfillUpdateWithCb <- cb:
		r.logger.Infof("pipeline %v updater received backfill callback registration. Current backfill callback count: %v", r.pipeline_name, len(r.backfillUpdateWithCb))
		return nil
	default:
		r.logger.Errorf("pipeline %v updater unable to register callback because channel is full")
		return base.ErrorMaxReached
	}
}

func (r *PipelineUpdater) executeQueuedCallbacks() {
	defer r.executeQueuedBackfillCallbacks()
	for {
		select {
		case cb := <-r.updateWithCb:
			r.logger.Infof("%v calling one callback...", r.pipeline_name)
			err := cb.callback()
			if err != nil {
				cb.errCb(err, true)
			}
		default:
			return
		}
	}
}

func (r *PipelineUpdater) executeQueuedBackfillCallbacks() {
	for {
		select {
		case cb := <-r.backfillUpdateWithCb:
			r.logger.Infof("%v calling one backfill callback...", r.pipeline_name)
			err := cb.callback()
			if err != nil {
				cb.errCb(err, true)
			}
		default:
			return
		}
	}
}

// Only if errors are recoverable by humans, and that these errors could be part of management transitions
// For example, users could turn explicit mapping on without rules, and then take some time to actually get
// the mappings set. If the human doesn't recover these actions after a period of time, then stop the pipeline
func (r *PipelineUpdater) humanRecoverableTransitionalErrors(errMap base.ErrorMap) bool {
	if base.CheckErrorMapForError(errMap, ErrorExplicitMappingWoRules, false) {
		return true
	}
	return false
}

func (r *PipelineUpdater) casPoisonErrors(errMap base.ErrorMap) bool {
	casPoisonErr := fmt.Errorf(base.PreCheckCASDriftDetected)
	return base.CheckErrorMapForError(errMap, casPoisonErr, false)
}

func (r *PipelineUpdater) shouldRetryOnRemoteAuthErrAndRemoteClusterName() (bool, string) {
	replSpecSvc := r.pipelineMgr.GetReplSpecSvc()
	remoteClusterSvc := r.pipelineMgr.GetRemoteClusterSvc()
	spec, err := replSpecSvc.ReplicationSpec(r.pipeline_name)
	if err != nil {
		r.logger.Errorf("Unable to get spec for %v\n", r.pipeline_name)
		return false, ""
	}
	var remoteClusterName string
	ref, err := remoteClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err == nil {
		remoteClusterName = ref.Name()
	}
	return spec.Settings.GetRetryOnRemoteAuthErr(), remoteClusterName
}

const (
	ConnAuthErrorString   = "experienced authentication issues"
	ConnDegradedString    = "experienced connectivity issues to one or more target cluster node(s)"
	ConnErrorString       = "experienced connectivity issues to ALL target cluster nodes"
	ConnErrorCommonSuffix = ". User intervention may be required"
)

func (r *PipelineUpdater) checkAndPublishRCError(status metadata.ConnectivityStatus, ref *metadata.RemoteClusterReference) {
	var needToAddErr bool

	if status == metadata.ConnIniting || status == metadata.ConnValid {
		// Clear any previous errors and that's it
		r.rep_status.ClearErrorsWithString(ConnErrorCommonSuffix)
		return
	}

	// Else, only add an error if it hasn't been shown before to prevent error overrun
	allErrors := r.rep_status.Errors()
	if len(allErrors) == 0 {
		needToAddErr = true
	} else {
		allErrorsString := allErrors.String()
		switch status {
		case metadata.ConnAuthErr:
			needToAddErr = !strings.Contains(allErrorsString, ConnAuthErrorString)
		case metadata.ConnDegraded:
			needToAddErr = !strings.Contains(allErrorsString, ConnDegradedString)
		case metadata.ConnError:
			needToAddErr = !strings.Contains(allErrorsString, ConnErrorString)
		}
	}

	if needToAddErr {
		errPrefix := fmt.Sprintf("Replication %v to remote cluster %v ", r.pipeline_name, ref.Name())
		switch status {
		case metadata.ConnAuthErr:
			r.rep_status.AddError(fmt.Errorf("%v%v%v", errPrefix, ConnAuthErrorString, ConnErrorCommonSuffix))
		case metadata.ConnDegraded:
			r.rep_status.AddError(fmt.Errorf("%v%v%v", errPrefix, ConnDegradedString, ConnErrorCommonSuffix))
		case metadata.ConnError:
			r.rep_status.AddError(fmt.Errorf("%v%v%v", errPrefix, ConnErrorString, ConnErrorCommonSuffix))
		}
	}
}
