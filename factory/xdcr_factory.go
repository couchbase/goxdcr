package factory

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/capi_utils"
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pp "github.com/couchbase/goxdcr/pipeline"
	pctx "github.com/couchbase/goxdcr/pipeline_ctx"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/supervisor"
	"github.com/couchbase/goxdcr/utils"
	"math"
	"strconv"
	"time"
)

const (
	PART_NAME_DELIMITER     = "_"
	DCP_NOZZLE_NAME_PREFIX  = "dcp"
	XMEM_NOZZLE_NAME_PREFIX = "xmem"
	CAPI_NOZZLE_NAME_PREFIX = "capi"
)

// errors
var ErrorNoSourceNozzle = errors.New("Invalid configuration. No source nozzle can be constructed since the source kv nodes are not the master for any vbuckets.")
var ErrorNoTargetNozzle = errors.New("Invalid configuration. No target nozzle can be constructed.")

// Factory for XDCR pipelines
type XDCRFactory struct {
	repl_spec_svc      service_def.ReplicationSpecSvc
	remote_cluster_svc service_def.RemoteClusterSvc
	cluster_info_svc   service_def.ClusterInfoSvc
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	checkpoint_svc     service_def.CheckpointsService
	capi_svc           service_def.CAPIService
	uilog_svc          service_def.UILogSvc
	//bucket settings service
	bucket_settings_svc service_def.BucketSettingsSvc

	default_logger_ctx         *log.LoggerContext
	pipeline_failure_handler   common.SupervisorFailureHandler
	logger                     *log.CommonLogger
	pipeline_master_supervisor *supervisor.GenericSupervisor
}

// set call back functions is done only once
func NewXDCRFactory(repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
	uilog_svc service_def.UILogSvc,
	bucket_settings_svc service_def.BucketSettingsSvc,
	pipeline_default_logger_ctx *log.LoggerContext,
	factory_logger_ctx *log.LoggerContext,
	pipeline_failure_handler common.SupervisorFailureHandler,
	pipeline_master_supervisor *supervisor.GenericSupervisor) *XDCRFactory {
	return &XDCRFactory{repl_spec_svc: repl_spec_svc,
		remote_cluster_svc:         remote_cluster_svc,
		cluster_info_svc:           cluster_info_svc,
		xdcr_topology_svc:          xdcr_topology_svc,
		checkpoint_svc:             checkpoint_svc,
		capi_svc:                   capi_svc,
		uilog_svc:                  uilog_svc,
		bucket_settings_svc:        bucket_settings_svc,
		default_logger_ctx:         pipeline_default_logger_ctx,
		pipeline_failure_handler:   pipeline_failure_handler,
		pipeline_master_supervisor: pipeline_master_supervisor,
		logger: log.NewLogger("XDCRFactory", factory_logger_ctx)}
}

func (xdcrf *XDCRFactory) NewPipeline(topic string, progress_recorder common.PipelineProgressRecorder) (common.Pipeline, error) {
	spec, err := xdcrf.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return nil, err
	}
	xdcrf.logger.Debugf("replication specification = %v\n", spec)

	logger_ctx := log.CopyCtx(xdcrf.default_logger_ctx)
	logger_ctx.Log_level = spec.Settings.LogLevel

	extMetaSupported, err := xdcrf.isExtMetaSupported(spec)
	if err != nil {
		return nil, err
	}
	sourceCRMode, err := xdcrf.getSourceConflictResolutionMode(spec.SourceBucketName)
	if err != nil {
		return nil, err
	}
	xdcrf.logger.Infof("%v extMetaSupported=%v, sourceCRMode=%v\n", topic, extMetaSupported, sourceCRMode)

	// popuplate pipeline using config
	sourceNozzles, kv_vb_map, err := xdcrf.constructSourceNozzles(spec, topic, extMetaSupported, logger_ctx)
	if err != nil {
		return nil, err
	}
	if len(sourceNozzles) == 0 {
		// no pipeline is constructed if there is no source nozzle
		return nil, ErrorNoSourceNozzle
	}

	progress_recorder(fmt.Sprintf("%v source nozzles have been constructed", len(sourceNozzles)))

	xdcrf.logger.Infof("%v kv_vb_map=%v\n", topic, kv_vb_map)
	outNozzles, vbNozzleMap, err := xdcrf.constructOutgoingNozzles(spec, kv_vb_map, extMetaSupported, sourceCRMode, logger_ctx)
	if err != nil {
		return nil, err
	}
	progress_recorder(fmt.Sprintf("%v target nozzles have been constructed", len(outNozzles)))

	// TODO construct queue parts. This will affect vbMap in router. may need an additional outNozzle -> downStreamPart/queue map in constructRouter

	// connect parts
	for _, sourceNozzle := range sourceNozzles {
		vblist := sourceNozzle.(*parts.DcpNozzle).GetVBList()
		downStreamParts := make(map[string]common.Part)
		for _, vb := range vblist {
			targetNozzleId, ok := vbNozzleMap[vb]
			if !ok {
				return nil, fmt.Errorf("Error constructing pipeline %v since there is no target nozzle for vb=%v", topic, vb)
			}

			outNozzle, ok := outNozzles[targetNozzleId]
			if !ok {
				panic(fmt.Sprintf("%v There is no corresponding target nozzle for vb=%v, targetNozzleId=%v", topic, vb, targetNozzleId))
			}
			downStreamParts[targetNozzleId] = outNozzle
		}

		router, err := xdcrf.constructRouter(sourceNozzle.Id(), spec, downStreamParts, vbNozzleMap, extMetaSupported, logger_ctx)
		if err != nil {
			return nil, err
		}
		sourceNozzle.SetConnector(router)
	}
	progress_recorder("Source nozzles are wired to target nozzles")

	// construct pipeline
	pipeline := pp.NewPipelineWithSettingConstructor(topic, sourceNozzles, outNozzles, spec, xdcrf.ConstructSettingsForPart, xdcrf.ConstructSSLPortMap, xdcrf.ConstructUpdateSettingsForPart, xdcrf.SetStartSeqno, xdcrf.remote_cluster_svc.RemoteClusterByUuid, logger_ctx)

	xdcrf.registerAsyncListenersOnSources(pipeline, logger_ctx)
	xdcrf.registerAsyncListenersOnTargets(pipeline, logger_ctx)

	if pipelineContext, err := pctx.NewWithSettingConstructor(pipeline, xdcrf.ConstructSettingsForService, logger_ctx); err != nil {
		return nil, err
	} else {

		//register services
		pipeline.SetRuntimeContext(pipelineContext)
		err = xdcrf.registerServices(pipeline, logger_ctx, kv_vb_map)
		if err != nil {
			return nil, err
		}
	}

	progress_recorder("Pipeline is constructed")

	xdcrf.logger.Infof("XDCR pipeline %v constructed", topic)
	return pipeline, nil
}

// extended metadata is supported by replication only if
// 1. replication is not of CAPI type
// and 2. extended metadata is supported by target cluster
// there is no need to check if extended metadata is supported by source cluster since
// source cluser is always post-sherlock
// when ext metadata is not supported, replication would not request ext metadata from dcp
// or add ext metadata to MCRequest
func (xdcrf *XDCRFactory) isExtMetaSupported(spec *metadata.ReplicationSpecification) (bool, error) {
	// first check if replication is of CAPI type, which does not support lww
	targetClusterRef, err := xdcrf.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		xdcrf.logger.Errorf("Error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, spec.Id, err)
		return false, err
	}

	nozzleType, err := xdcrf.getOutNozzleType(targetClusterRef, spec)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get the out nozzle type for pipeline %v, err=%v\n", spec.Id, err)
		return false, err
	}

	if nozzleType == base.Capi {
		return false, nil
	}

	// then check if target cluster supports extended metadata
	extMetaSupportedByTarget, err := pipeline_utils.HasExtMetadataSupport(xdcrf.cluster_info_svc, targetClusterRef)
	if err != nil {
		xdcrf.logger.Errorf("Received error when checking whether target cluster supports extended metadata for pipeline %v, err=%v\n", spec.Id, err)
		return false, err
	}

	return extMetaSupportedByTarget, nil

}

func (xdcrf *XDCRFactory) getSourceConflictResolutionMode(bucketName string) (base.ConflictResolutionMode, error) {
	hostAddr, err := xdcrf.xdcr_topology_svc.MyHostAddr()
	if err != nil {
		return base.CRMode_RevId, err
	}

	var out interface{}
	err, _ = utils.QueryRestApi(hostAddr, base.DefaultPoolBucketsPath+bucketName, true, base.MethodGet, "", nil, 0, &out, xdcrf.logger)
	if err != nil {
		return base.CRMode_RevId, utils.NewEnhancedError(fmt.Sprintf("Error retrieving bucket info for %v\n", bucketName), err)
	}

	infoMap, ok := out.(map[string]interface{})
	if !ok {
		xdcrf.logger.Errorf("Error parsing bucket info for %v. bucket info = %v", bucketName, out)
		return base.CRMode_RevId, fmt.Errorf("Error parsing bucket info for %v\n", bucketName)
	}

	timeSynchronized := false
	timeSynchronizationObj, ok := infoMap[base.TimeSynchronizationKey]
	if ok {
		timeSynchronizationStr, ok := timeSynchronizationObj.(string)
		if !ok {
			xdcrf.logger.Errorf("Error parsing time synchronization from bucket info for %v. timeSynchronization = %v, bucket info = %v", bucketName, timeSynchronizationObj, out)
			return base.CRMode_RevId, fmt.Errorf("Error parsing time synchronization from bucket info for %v\n", bucketName)
		}
		timeSynchronized = (timeSynchronizationStr != base.TimeSynchronization_Disabled)
	}

	if timeSynchronized {
		return base.CRMode_LWW, nil
	} else {
		return base.CRMode_RevId, nil
	}

}

func min(num1 int, num2 int) int {
	return int(math.Min(float64(num1), float64(num2)))
}

// evenly distribute load across workers
// assumes that num_of_worker <= num_of_load
// returns load_distribution [][]int, where
//     load_distribution[i][0] is the start index, inclusive, of load for ith worker
//     load_distribution[i][1] is the end index, exclusive, of load for ith worker
// note that load is zero indexed, i.e., indexed as 0, 1, .. N-1 for N loads
func balanceLoad(num_of_worker int, num_of_load int) [][]int {
	load_distribution := make([][]int, 0)

	max_load_per_worker := int(math.Ceil(float64(num_of_load) / float64(num_of_worker)))
	num_of_worker_with_max_load := num_of_load - (max_load_per_worker-1)*num_of_worker

	index := 0
	var num_of_load_per_worker int
	for i := 0; i < num_of_worker; i++ {
		if i < num_of_worker_with_max_load {
			num_of_load_per_worker = max_load_per_worker
		} else {
			num_of_load_per_worker = max_load_per_worker - 1
		}

		load_for_worker := make([]int, 2)
		load_for_worker[0] = index
		index += num_of_load_per_worker
		load_for_worker[1] = index

		load_distribution = append(load_distribution, load_for_worker)
	}

	if index != num_of_load {
		panic(fmt.Sprintf("number of load processed %v does not match total number of load %v", index, num_of_load))
	}

	return load_distribution
}

// get nozzle list from nozzle map
func getNozzleList(nozzle_map map[string]common.Nozzle) []common.Nozzle {
	nozzle_list := make([]common.Nozzle, 0)
	for _, nozzle := range nozzle_map {
		nozzle_list = append(nozzle_list, nozzle)
	}
	return nozzle_list
}

// construct and register async componet event listeners on source nozzles
func (xdcrf *XDCRFactory) registerAsyncListenersOnSources(pipeline common.Pipeline, logger_ctx *log.LoggerContext) {
	sources := getNozzleList(pipeline.Sources())

	num_of_sources := len(sources)
	num_of_listeners := min(num_of_sources, base.MaxNumberOfAsyncListeners)
	load_distribution := balanceLoad(num_of_listeners, num_of_sources)
	xdcrf.logger.Infof("topic=%v, num_of_sources=%v, num_of_listeners=%v, load_distribution=%v\n", pipeline.Topic(), num_of_sources, num_of_listeners, load_distribution)

	for i := 0; i < num_of_listeners; i++ {
		data_received_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataReceivedEventListener, i),
			pipeline.Topic(), logger_ctx)
		data_processed_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataProcessedEventListener, i),
			pipeline.Topic(), logger_ctx)
		data_filtered_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataFilteredEventListener, i),
			pipeline.Topic(), logger_ctx)

		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			dcp_part := sources[index]

			dcp_part.RegisterComponentEventListener(common.DataReceived, data_received_event_listener)
			dcp_part.RegisterComponentEventListener(common.DataProcessed, data_processed_event_listener)

			conn := dcp_part.Connector()
			conn.RegisterComponentEventListener(common.DataFiltered, data_filtered_event_listener)
		}
	}
}

// construct and register async componet event listeners on target nozzles
func (xdcrf *XDCRFactory) registerAsyncListenersOnTargets(pipeline common.Pipeline, logger_ctx *log.LoggerContext) {
	targets := getNozzleList(pipeline.Targets())
	num_of_targets := len(targets)
	num_of_listeners := min(num_of_targets, base.MaxNumberOfAsyncListeners)
	load_distribution := balanceLoad(num_of_listeners, num_of_targets)
	xdcrf.logger.Infof("topic=%v, num_of_targets=%v, num_of_listeners=%v, load_distribution=%v\n", pipeline.Topic(), num_of_targets, num_of_listeners, load_distribution)

	for i := 0; i < num_of_listeners; i++ {
		data_failed_cr_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataFailedCREventListener, i),
			pipeline.Topic(), logger_ctx)
		data_sent_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataSentEventListener, i),
			pipeline.Topic(), logger_ctx)
		get_meta_received_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.GetMetaReceivedEventListener, i),
			pipeline.Topic(), logger_ctx)

		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			out_nozzle := targets[index]
			out_nozzle.RegisterComponentEventListener(common.DataSent, data_sent_event_listener)
			out_nozzle.RegisterComponentEventListener(common.DataFailedCRSource, data_failed_cr_event_listener)
			out_nozzle.RegisterComponentEventListener(common.GetMetaReceived, get_meta_received_event_listener)
		}
	}
}

// construct source nozzles for the requested/current kv node
func (xdcrf *XDCRFactory) constructSourceNozzles(spec *metadata.ReplicationSpecification,
	topic string,
	extMetaSupported bool,
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[string][]uint16, error) {
	sourceNozzles := make(map[string]common.Nozzle)

	bucketName := spec.SourceBucketName

	bucket, err := xdcrf.cluster_info_svc.GetBucket(xdcrf.xdcr_topology_svc, bucketName)
	if err != nil {
		xdcrf.logger.Errorf("Error getting bucket %v. err=%v\n", bucketName, err)
		return nil, nil, err
	}
	bucketPassword := bucket.Password
	bucket.Close()

	maxNozzlesPerNode := spec.Settings.SourceNozzlePerNode

	kv_vb_map, err := pipeline_utils.GetSourceVBMap(xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, spec.SourceBucketName, xdcrf.logger)
	if err != nil {
		return nil, nil, err
	}

	for kvaddr, vbnos := range kv_vb_map {

		numOfVbs := len(vbnos)
		if numOfVbs == 0 {
			continue
		}

		// the number of dcpNozzle nodes to construct is the smaller of vbucket list size and source connection size
		numOfDcpNozzles := min(numOfVbs, maxNozzlesPerNode)
		load_distribution := balanceLoad(numOfDcpNozzles, numOfVbs)
		xdcrf.logger.Infof("topic=%v, numOfDcpNozzles=%v, numOfVbs=%v, load_distribution=%v\n", spec.Id, numOfDcpNozzles, numOfVbs, load_distribution)

		for i := 0; i < numOfDcpNozzles; i++ {
			// construct vbList for the dcpNozzle
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among dcpNozzles
			vbList := make([]uint16, 0, 15)
			for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
				vbList = append(vbList, vbnos[index])
			}

			// construct dcpNozzles
			// partIds of the dcpNozzle nodes look like "dcpNozzle_$kvaddr_1"
			id := xdcrf.partId(DCP_NOZZLE_NAME_PREFIX, spec.Id, kvaddr, i)
			dcpNozzle := parts.NewDcpNozzle(id,
				bucketName, bucketPassword, vbList, xdcrf.xdcr_topology_svc, extMetaSupported, logger_ctx)
			sourceNozzles[dcpNozzle.Id()] = dcpNozzle
			xdcrf.logger.Debugf("Constructed source nozzle %v with vbList = %v \n", dcpNozzle.Id(), vbList)
		}

		xdcrf.logger.Infof("Constructed %v source nozzles for %v vbs on %v\n", len(sourceNozzles), numOfVbs, kvaddr)
	}

	return sourceNozzles, kv_vb_map, nil
}

func (xdcrf *XDCRFactory) partId(prefix string, topic string, kvaddr string, index int) string {
	return prefix + PART_NAME_DELIMITER + topic + PART_NAME_DELIMITER + kvaddr + PART_NAME_DELIMITER + strconv.Itoa(index)
}

func (xdcrf *XDCRFactory) filterVBList(targetkvVBList []uint16, kv_vb_map map[string][]uint16) []uint16 {
	ret := []uint16{}
	for _, vb := range targetkvVBList {
		for _, sourcevblist := range kv_vb_map {
			for _, sourcevb := range sourcevblist {
				if sourcevb == vb {
					ret = append(ret, vb)
				}
			}
		}
	}

	return ret
}

func (xdcrf *XDCRFactory) constructOutgoingNozzles(spec *metadata.ReplicationSpecification, kv_vb_map map[string][]uint16,
	extMetaSupported bool, sourceCRMode base.ConflictResolutionMode, logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[uint16]string, error) {
	outNozzles := make(map[string]common.Nozzle)
	vbNozzleMap := make(map[uint16]string)

	targetBucketName := spec.TargetBucketName
	targetClusterRef, err := xdcrf.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		xdcrf.logger.Errorf("Error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, spec.Id, err)
		return nil, nil, err
	}

	kvVBMap, err := xdcrf.cluster_info_svc.GetServerVBucketsMap(targetClusterRef, targetBucketName)
	if err != nil {
		xdcrf.logger.Errorf("Error getting server vbuckets map, err=%v\n", err)
		return nil, nil, err
	}
	if len(kvVBMap) == 0 {
		return nil, nil, ErrorNoTargetNozzle
	}

	targetBucket, err := xdcrf.cluster_info_svc.GetBucket(targetClusterRef, targetBucketName)
	if err != nil || targetBucket == nil {
		xdcrf.logger.Errorf("Error getting bucket, err=%v\n", err)
		return nil, nil, err
	}
	defer targetBucket.Close()

	bucketPwd := targetBucket.Password
	maxTargetNozzlePerNode := spec.Settings.TargetNozzlePerNode
	xdcrf.logger.Infof("Target topology retrieved. kvVBMap = %v\n", kvVBMap)

	var vbCouchApiBaseMap map[uint16]string

	nozzleType, err := xdcrf.getOutNozzleType(targetClusterRef, spec)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get the nozzle type, err=%v\n", err)
		return nil, nil, err
	}

	for kvaddr, kvVBList := range kvVBMap {
		isCapiNozzle := (nozzleType == base.Capi)
		if isCapiNozzle && len(vbCouchApiBaseMap) == 0 {
			// construct vbCouchApiBaseMap only when nessary and only once
			vbCouchApiBaseMap, err = capi_utils.ConstructVBCouchApiBaseMap(targetBucket, targetClusterRef)
			if err != nil {
				xdcrf.logger.Errorf("Failed to construct vbCouchApiBase map, err=%v\n", err)
				return nil, nil, err
			}
		}

		relevantVBs := xdcrf.filterVBList(kvVBList, kv_vb_map)

		xdcrf.logger.Debugf("kvaddr = %v; kvVbList=%v, relevantVBs=-%v\n", kvaddr, kvVBList, relevantVBs)

		numOfVbs := len(relevantVBs)
		// the number of xmem nozzles to construct is the smaller of vbucket list size and target connection size
		numOfOutNozzles := min(numOfVbs, maxTargetNozzlePerNode)
		load_distribution := balanceLoad(numOfOutNozzles, numOfVbs)
		xdcrf.logger.Infof("topic=%v, numOfOutNozzles=%v, numOfVbs=%v, load_distribution=%v\n", spec.Id, numOfOutNozzles, numOfVbs, load_distribution)

		for i := 0; i < numOfOutNozzles; i++ {
			// construct vb list for the out nozzle, which is needed by capi nozzle
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among out nozzles
			vbList := make([]uint16, 0)
			for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
				vbList = append(vbList, relevantVBs[index])
			}

			// construct outgoing nozzle
			var outNozzle common.Nozzle
			if isCapiNozzle {
				outNozzle, err = xdcrf.constructCAPINozzle(spec.Id, targetClusterRef.UserName, targetClusterRef.Password, targetClusterRef.Certificate, vbList, vbCouchApiBaseMap, i, logger_ctx)
				if err != nil {
					return nil, nil, err
				}
			} else {
				connSize := numOfOutNozzles * 2
				outNozzle = xdcrf.constructXMEMNozzle(spec.Id, kvaddr, targetBucketName, bucketPwd, i, connSize, extMetaSupported, sourceCRMode, logger_ctx)
			}

			outNozzles[outNozzle.Id()] = outNozzle

			// construct vbNozzleMap for the out nozzle, which is needed by the router
			for _, vbno := range vbList {
				vbNozzleMap[vbno] = outNozzle.Id()
			}

			xdcrf.logger.Debugf("Constructed out nozzle %v\n", outNozzle.Id())
		}
	}

	xdcrf.logger.Infof("Constructed %v outgoing nozzles\n", len(outNozzles))
	xdcrf.logger.Debugf("vbNozzleMap = %v\n", vbNozzleMap)
	return outNozzles, vbNozzleMap, nil
}

func (xdcrf *XDCRFactory) constructRouter(id string, spec *metadata.ReplicationSpecification,
	downStreamParts map[string]common.Part,
	vbNozzleMap map[uint16]string,
	extMetaSupported bool,
	logger_ctx *log.LoggerContext) (*parts.Router, error) {
	routerId := "Router" + PART_NAME_DELIMITER + id
	router, err := parts.NewRouter(routerId, spec.Id, spec.Settings.FilterExpression, downStreamParts, vbNozzleMap, logger_ctx, pipeline_manager.NewMCRequestObj, extMetaSupported)
	xdcrf.logger.Infof("Constructed router")
	return router, err
}

func (xdcrf *XDCRFactory) getOutNozzleType(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (base.XDCROutgoingNozzleType, error) {
	switch spec.Settings.RepType {
	case metadata.ReplicationTypeXmem:
		xmemCompatible, err := xdcrf.cluster_info_svc.IsClusterCompatible(targetClusterRef, []int{2, 2})
		if err != nil {
			xdcrf.logger.Errorf("Failed to get cluster version information, err=%v\n", err)
			return -1, err
		}
		if xmemCompatible {
			return base.Xmem, nil
		} else {
			return -1, fmt.Errorf("Invalid configuration. Xmem replication type is specified when the target cluster, %v, is not xmem compatible.\n", targetClusterRef.HostName)
		}
	case metadata.ReplicationTypeCapi:
		return base.Capi, nil
	default:
		// should never get here
		return -1, errors.New(fmt.Sprintf("Invalid replication type %v", spec.Settings.RepType))
	}
}

func (xdcrf *XDCRFactory) constructXMEMNozzle(topic string, kvaddr string,
	bucketName string,
	bucketPwd string,
	nozzle_index int,
	connPoolSize int,
	extMetaSupported bool,
	sourceCRMode base.ConflictResolutionMode,
	logger_ctx *log.LoggerContext) common.Nozzle {
	// partIds of the xmem nozzles look like "xmem_$topic_$kvaddr_1"
	xmemNozzle_Id := xdcrf.partId(XMEM_NOZZLE_NAME_PREFIX, topic, kvaddr, nozzle_index)
	nozzle := parts.NewXmemNozzle(xmemNozzle_Id, topic, topic, connPoolSize, kvaddr, bucketName, bucketPwd, pipeline_manager.RecycleMCRequestObj, extMetaSupported, sourceCRMode, logger_ctx)
	return nozzle
}

func (xdcrf *XDCRFactory) constructCAPINozzle(topic string,
	username string,
	password string,
	certificate []byte,
	vbList []uint16,
	vbCouchApiBaseMap map[uint16]string,
	nozzle_index int,
	logger_ctx *log.LoggerContext) (common.Nozzle, error) {
	if len(vbList) == 0 {
		// should never get here
		xdcrf.logger.Errorf("Skip constructing capi nozzle with index %v since it contains no vbucket", nozzle_index)
	}

	// construct a sub map of vbCouchApiBaseMap with keys in vbList
	subVBCouchApiBaseMap := make(map[uint16]string)
	for _, vbno := range vbList {
		subVBCouchApiBaseMap[vbno] = vbCouchApiBaseMap[vbno]
	}
	// get capi connection string
	couchApiBase := subVBCouchApiBaseMap[vbList[0]]
	capiConnectionStr, err := capi_utils.GetCapiConnectionStrFromCouchApiBase(couchApiBase)
	if err != nil {
		return nil, err
	}
	xdcrf.logger.Debugf("Construct CapiNozzle: topic=%s, kvaddr=%s", topic, capiConnectionStr)
	// partIds of the capi nozzles look like "capi_$topic_$kvaddr_1"
	capiNozzle_Id := xdcrf.partId(CAPI_NOZZLE_NAME_PREFIX, topic, capiConnectionStr, nozzle_index)
	nozzle := parts.NewCapiNozzle(capiNozzle_Id, topic, capiConnectionStr, username, password, certificate, subVBCouchApiBaseMap, pipeline_manager.RecycleMCRequestObj, logger_ctx)
	return nozzle, nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings map[string]interface{},
	ts map[uint16]*base.VBTimestamp, targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16,
	isSSLOverMem bool) (map[string]interface{}, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for XmemNozzle %s", part.Id())
		return xdcrf.constructSettingsForXmemNozzle(pipeline, part, targetClusterRef, settings, ssl_port_map, isSSLOverMem)
	} else if _, ok := part.(*parts.DcpNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for DcpNozzle %s", part.Id())
		return xdcrf.constructSettingsForDcpNozzle(pipeline, part.(*parts.DcpNozzle), settings, ts)
	} else if _, ok := part.(*parts.CapiNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for CapiNozzle %s", part.Id())
		return xdcrf.constructSettingsForCapiNozzle(pipeline, settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) ConstructUpdateSettingsForPart(pipeline common.Pipeline, part common.Part, settings map[string]interface{}) (map[string]interface{}, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct update settings for XmemNozzle %s", part.Id())
		return xdcrf.constructUpdateSettingsForXmemNozzle(pipeline, settings), nil
	} else if _, ok := part.(*parts.CapiNozzle); ok {
		xdcrf.logger.Debugf("Construct update settings for CapiNozzle %s", part.Id())
		return xdcrf.constructUpdateSettingsForCapiNozzle(pipeline, settings), nil
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForXmemNozzle(pipeline common.Pipeline, settings map[string]interface{}) map[string]interface{} {
	xmemSettings := make(map[string]interface{})
	repSettings := pipeline.Specification().Settings

	xmemSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThreshold, repSettings.OptimisticReplicationThreshold)
	return xmemSettings

}

func (xdcrf *XDCRFactory) constructUpdateSettingsForCapiNozzle(pipeline common.Pipeline, settings map[string]interface{}) map[string]interface{} {
	capiSettings := make(map[string]interface{})
	repSettings := pipeline.Specification().Settings

	capiSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThreshold, repSettings.OptimisticReplicationThreshold)
	return capiSettings
}

func (xdcrf *XDCRFactory) SetStartSeqno(pipeline common.Pipeline) error {
	if pipeline == nil {
		return errors.New("pipeline=nil")
	}
	ckpt_mgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckpt_mgr == nil {
		return errors.New(fmt.Sprintf("CheckpoingManager is not attached to pipeline %v", pipeline.Topic()))
	}
	return ckpt_mgr.(*pipeline_svc.CheckpointManager).SetVBTimestamps(pipeline.Topic())
}

func (xdcrf *XDCRFactory) constructSettingsForXmemNozzle(pipeline common.Pipeline, part common.Part,
	targetClusterRef *metadata.RemoteClusterReference, settings map[string]interface{},
	ssl_port_map map[string]uint16, isSSLOverMem bool) (map[string]interface{}, error) {
	xmemSettings := make(map[string]interface{})
	spec := pipeline.Specification()
	repSettings := spec.Settings
	xmemConnStr := part.(*parts.XmemNozzle).ConnStr()

	xmemSettings[parts.SETTING_BATCHCOUNT] = getSettingFromSettingsMap(settings, metadata.BatchCount, repSettings.BatchCount)
	xmemSettings[parts.SETTING_BATCHSIZE] = getSettingFromSettingsMap(settings, metadata.BatchSize, repSettings.BatchSize)
	xmemSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	xmemSettings[parts.SETTING_BATCH_EXPIRATION_TIME] = time.Duration(float64(repSettings.MaxExpectedReplicationLag)*0.7) * time.Millisecond
	xmemSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThreshold, repSettings.OptimisticReplicationThreshold)
	xmemSettings[parts.SETTING_STATS_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsInterval, repSettings.StatsInterval)

	demandEncryption := targetClusterRef.DemandEncryption
	certificate := targetClusterRef.Certificate
	if demandEncryption {
		if isSSLOverMem {
			mem_ssl_port, ok := ssl_port_map[xmemConnStr]
			if !ok {
				return nil, fmt.Errorf("Can't get remote memcached ssl port for %v", xmemConnStr)
			}
			xdcrf.logger.Infof("mem_ssl_port=%v\n", mem_ssl_port)

			xmemSettings[parts.XMEM_SETTING_REMOTE_MEM_SSL_PORT] = mem_ssl_port
			xmemSettings[parts.XMEM_SETTING_CERTIFICATE] = certificate
			xmemSettings[parts.XMEM_SETTING_DEMAND_ENCRYPTION] = demandEncryption
			xdcrf.logger.Infof("xmemSettings=%v\n", xmemSettings)

		} else {
			local_proxy_port, err := xdcrf.xdcr_topology_svc.MyProxyPort()

			if err != nil {
				return nil, err
			}

			remote_proxy_port, ok := ssl_port_map[xmemConnStr]
			if !ok {
				return nil, errors.New(fmt.Sprintf("Can't find remote proxy port for remote bucket %v", spec.TargetBucketName))
			}

			xmemSettings[parts.XMEM_SETTING_DEMAND_ENCRYPTION] = demandEncryption
			xmemSettings[parts.XMEM_SETTING_CERTIFICATE] = certificate
			xmemSettings[parts.XMEM_SETTING_REMOTE_PROXY_PORT] = remote_proxy_port
			xmemSettings[parts.XMEM_SETTING_LOCAL_PROXY_PORT] = local_proxy_port
		}
	}
	return xmemSettings, nil

}

func (xdcrf *XDCRFactory) constructSettingsForCapiNozzle(pipeline common.Pipeline, settings map[string]interface{}) (map[string]interface{}, error) {
	capiSettings := make(map[string]interface{})
	repSettings := pipeline.Specification().Settings

	capiSettings[parts.SETTING_BATCHCOUNT] = getSettingFromSettingsMap(settings, metadata.BatchCount, repSettings.BatchCount)
	capiSettings[parts.SETTING_BATCHSIZE] = getSettingFromSettingsMap(settings, metadata.BatchSize, repSettings.BatchSize)
	capiSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	capiSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThreshold, repSettings.OptimisticReplicationThreshold)
	capiSettings[parts.SETTING_STATS_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsInterval, repSettings.StatsInterval)

	return capiSettings, nil

}

func (xdcrf *XDCRFactory) getTargetTimeoutEstimate(topic string) time.Duration {
	//TODO: implement
	//need to get the tcp ping time for the estimate
	return 100 * time.Millisecond
}

func (xdcrf *XDCRFactory) constructSettingsForDcpNozzle(pipeline common.Pipeline, part *parts.DcpNozzle, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error) {
	xdcrf.logger.Debugf("Construct settings for DcpNozzle ....")
	dcpNozzleSettings := make(map[string]interface{})
	spec := pipeline.Specification()
	repSettings := spec.Settings

	ckpt_svc := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckpt_svc == nil {
		return nil, errors.New("No checkpoint manager is registered with the pipeline")
	}

	vbList := part.GetVBList()
	partTs := make(map[uint16]*base.VBTimestamp)
	for _, vb := range vbList {
		partTs[vb] = ts[vb]
	}
	xdcrf.logger.Debugf("start timestamps is %v\n", ts)
	dcpNozzleSettings[parts.DCP_VBTimestamp] = partTs
	dcpNozzleSettings[parts.DCP_VBTimestampUpdator] = ckpt_svc.(*pipeline_svc.CheckpointManager).UpdateVBTimestamps
	dcpNozzleSettings[parts.DCP_Stats_Interval] = getSettingFromSettingsMap(settings, metadata.PipelineStatsInterval, repSettings.StatsInterval)
	return dcpNozzleSettings, nil
}

func (xdcrf *XDCRFactory) registerServices(pipeline common.Pipeline, logger_ctx *log.LoggerContext, kv_vb_map map[string][]uint16) error {
	through_seqno_tracker_svc := service_impl.NewThroughSeqnoTrackerSvc(logger_ctx)
	through_seqno_tracker_svc.Attach(pipeline)

	ctx := pipeline.RuntimeContext()

	//register pipeline supervisor
	supervisor := pipeline_svc.NewPipelineSupervisor(base.PipelineSupervisorIdPrefix+pipeline.Topic(), logger_ctx,
		xdcrf.pipeline_failure_handler, xdcrf.pipeline_master_supervisor, xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc)
	err := ctx.RegisterService(base.PIPELINE_SUPERVISOR_SVC, supervisor)
	if err != nil {
		return err
	}
	//register pipeline checkpoint manager
	ckptMgr, err := pipeline_svc.NewCheckpointManager(xdcrf.checkpoint_svc, xdcrf.capi_svc,
		xdcrf.remote_cluster_svc, xdcrf.repl_spec_svc, xdcrf.cluster_info_svc,
		xdcrf.xdcr_topology_svc, through_seqno_tracker_svc, kv_vb_map, logger_ctx)
	if err != nil {
		xdcrf.logger.Errorf("Failed to construct CheckpointManager, err=%v ckpt_svc=%v, capi_svc=%v, remote_cluster_svc=%v, repl_spec_svc=%v\n", err, xdcrf.checkpoint_svc, xdcrf.capi_svc,
			xdcrf.remote_cluster_svc, xdcrf.repl_spec_svc)
		return err
	}
	err = ctx.RegisterService(base.CHECKPOINT_MGR_SVC, ckptMgr)
	if err != nil {
		return err
	}
	//register pipeline statistics manager
	bucket_name := pipeline.Specification().SourceBucketName
	err = ctx.RegisterService(base.STATISTICS_MGR_SVC, pipeline_svc.NewStatisticsManager(through_seqno_tracker_svc, xdcrf.cluster_info_svc,
		xdcrf.xdcr_topology_svc, logger_ctx, kv_vb_map, bucket_name))
	if err != nil {
		return err
	}

	//register topology change detect service
	top_detect_svc := pipeline_svc.NewTopologyChangeDetectorSvc(xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, xdcrf.remote_cluster_svc, logger_ctx)
	err = ctx.RegisterService(base.TOPOLOGY_CHANGE_DETECT_SVC, top_detect_svc)
	if err != nil {
		return err
	}
	return nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForService(pipeline common.Pipeline, service common.PipelineService, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error) {
	switch service.(type) {
	case *pipeline_svc.PipelineSupervisor:
		xdcrf.logger.Debug("Construct settings for PipelineSupervisor")
		return xdcrf.constructSettingsForSupervisor(pipeline, settings)
	case *pipeline_svc.StatisticsManager:
		xdcrf.logger.Debug("Construct settings for StatisticsManager")
		return xdcrf.constructSettingsForStatsManager(pipeline, settings, ts)
	case *pipeline_svc.CheckpointManager:
		xdcrf.logger.Debug("Construct update settings for CheckpointManager")
		return xdcrf.constructSettingsForCheckpointManager(pipeline, settings)
	}
	return settings, nil
}

func (xdcrf *XDCRFactory) constructSettingsForSupervisor(pipeline common.Pipeline, settings map[string]interface{}) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	log_level_str := getSettingFromSettingsMap(settings, metadata.PipelineLogLevel, pipeline.Specification().Settings.LogLevel.String())
	log_level, err := log.LogLevelFromStr(log_level_str.(string))
	if err != nil {
		return nil, err
	}
	s[pipeline_svc.PIPELINE_LOG_LEVEL] = log_level
	return s, nil
}

func (xdcrf *XDCRFactory) constructSettingsForStatsManager(pipeline common.Pipeline, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	s[pipeline_svc.PUBLISH_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsInterval, pipeline.Specification().Settings.StatsInterval)

	if ts != nil {
		// this indicates that we are constructing start settings

		//set the start vb sequence no map to statistics manager
		if pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC) == nil {
			return nil, errors.New("No checkpoint manager is registered with the pipeline")
		}

		s[pipeline_svc.VB_START_TS] = ts
	}

	return s, nil
}

func (xdcrf *XDCRFactory) constructSettingsForCheckpointManager(pipeline common.Pipeline, settings map[string]interface{}) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	s[pipeline_svc.CHECKPOINT_INTERVAL] = getSettingFromSettingsMap(settings, metadata.CheckpointInterval, pipeline.Specification().Settings.CheckpointInterval)
	return s, nil
}

func getSettingFromSettingsMap(settings map[string]interface{}, setting_name string, default_value interface{}) interface{} {
	if settings != nil {
		if setting, ok := settings[setting_name]; ok {
			return setting
		}
	}

	return default_value
}

func (xdcrf *XDCRFactory) ConstructSSLPortMap(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, bool, error) {

	var ssl_port_map map[string]uint16
	var hasSSLOverMemSupport bool

	nozzleType, err := xdcrf.getOutNozzleType(targetClusterRef, spec)
	if err != nil {
		return nil, false, err
	}
	// if both xmem nozzles and ssl are involved, populate ssl_port_map
	// if target cluster is post-3.0, the ssl ports in the map are memcached ssl ports
	// otherwise, the ssl ports in the map are proxy ssl ports
	if targetClusterRef.DemandEncryption && nozzleType == base.Xmem {
		hasSSLOverMemSupport, err = pipeline_utils.HasSSLOverMemSupport(xdcrf.cluster_info_svc, targetClusterRef)
		if err != nil {
			return nil, false, err
		}
		xdcrf.logger.Infof("hasSSLOverMemSupport=%v\n", hasSSLOverMemSupport)

		if hasSSLOverMemSupport {
			ssl_port_map, err = utils.GetMemcachedSSLPort(targetClusterRef.HostName, targetClusterRef.UserName, targetClusterRef.Password, spec.TargetBucketName, xdcrf.logger)
			if err != nil {
				xdcrf.logger.Errorf("Failed to get memcached ssl port, err=%v\n", err)
				return nil, false, err
			}
		} else {
			targetBucket, err := xdcrf.cluster_info_svc.GetBucket(targetClusterRef, spec.TargetBucketName)
			if err != nil {
				xdcrf.logger.Errorf("Error getting bucket. err=%v\n", err)
				return nil, false, err
			}
			defer targetBucket.Close()

			ssl_port_map = make(map[string]uint16)
			for _, node := range targetBucket.Nodes() {
				hostname := utils.GetHostName(node.Hostname)
				memcachedPort := uint16(node.Ports[base.DirectPortKey])
				hostAddr := utils.GetHostAddr(hostname, memcachedPort)
				proxyPort := uint16(node.Ports[base.SSLProxyPortKey])
				ssl_port_map[hostAddr] = proxyPort
			}
		}

		xdcrf.logger.Debugf("ssl_port_map=%v\n", ssl_port_map)
	}

	return ssl_port_map, hasSSLOverMemSupport, nil
}
