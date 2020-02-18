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
	utilities "github.com/couchbase/goxdcr/utils"
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

// interface so we can autogenerate mock and do unit test
type XDCRFactoryIface interface {
	NewPipeline(topic string, progress_recorder common.PipelineProgressRecorder) (common.Pipeline, error)
	ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings metadata.ReplicationSettingsMap,
		targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16,
		isSSLOverMem bool) (metadata.ReplicationSettingsMap, error)
	ConstructSettingsForConnector(pipeline common.Pipeline, connector common.Connector, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
	ConstructUpdateSettingsForPart(pipeline common.Pipeline, part common.Part, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
	ConstructUpdateSettingsForConnector(pipeline common.Pipeline, connector common.Connector, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
	SetStartSeqno(pipeline common.Pipeline) error
	CheckpointBeforeStop(pipeline common.Pipeline) error
	ConstructUpdateSettingsForService(pipeline common.Pipeline, service common.PipelineService, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error)
	ConstructSSLPortMap(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, bool, error)
}

// Factory for XDCR pipelines
type XDCRFactory struct {
	repl_spec_svc            service_def.ReplicationSpecSvc
	remote_cluster_svc       service_def.RemoteClusterSvc
	cluster_info_svc         service_def.ClusterInfoSvc
	xdcr_topology_svc        service_def.XDCRCompTopologySvc
	checkpoint_svc           service_def.CheckpointsService
	capi_svc                 service_def.CAPIService
	uilog_svc                service_def.UILogSvc
	bucket_settings_svc      service_def.BucketSettingsSvc
	throughput_throttler_svc service_def.ThroughputThrottlerSvc
	collectionsManifestSvc   service_def.CollectionsManifestSvc

	default_logger_ctx       *log.LoggerContext
	pipeline_failure_handler common.SupervisorFailureHandler
	logger                   *log.CommonLogger
	utils                    utilities.UtilsIface
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
	throughput_throttler_svc service_def.ThroughputThrottlerSvc,
	pipeline_default_logger_ctx *log.LoggerContext,
	factory_logger_ctx *log.LoggerContext,
	pipeline_failure_handler common.SupervisorFailureHandler,
	utilsIn utilities.UtilsIface,
	collectionsManifestSvc service_def.CollectionsManifestSvc) *XDCRFactory {
	return &XDCRFactory{repl_spec_svc: repl_spec_svc,
		remote_cluster_svc:       remote_cluster_svc,
		cluster_info_svc:         cluster_info_svc,
		xdcr_topology_svc:        xdcr_topology_svc,
		checkpoint_svc:           checkpoint_svc,
		capi_svc:                 capi_svc,
		uilog_svc:                uilog_svc,
		bucket_settings_svc:      bucket_settings_svc,
		throughput_throttler_svc: throughput_throttler_svc,
		default_logger_ctx:       pipeline_default_logger_ctx,
		pipeline_failure_handler: pipeline_failure_handler,
		logger:                   log.NewLogger("XDCRFactory", factory_logger_ctx),
		utils:                    utilsIn,
		collectionsManifestSvc:   collectionsManifestSvc}
}

/**
 * This is the method where the majority of the pipeline and its required components are constructed.
 * PipelineManager is currently the only user of this method.
 */
func (xdcrf *XDCRFactory) NewPipeline(topic string, progress_recorder common.PipelineProgressRecorder) (common.Pipeline, error) {
	spec, err := xdcrf.repl_spec_svc.ReplicationSpec(topic)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get replication specification for pipeline %v, err=%v\n", topic, err)
		return nil, err
	}
	xdcrf.logger.Debugf("replication specification = %v\n", spec)

	logger_ctx := log.CopyCtx(xdcrf.default_logger_ctx)
	logger_ctx.SetLogLevel(spec.Settings.LogLevel)

	targetClusterRef, err := xdcrf.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err != nil {
		xdcrf.logger.Errorf("Error getting remote cluster with uuid=%v for pipeline %v, err=%v\n", spec.TargetClusterUUID, spec.Id, err)
		return nil, err
	}

	nozzleType, err := xdcrf.getOutNozzleType(targetClusterRef, spec)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get the nozzle type for %v, err=%v\n", spec.Id, err)
		return nil, err
	}
	isCapiReplication := (nozzleType == base.Capi)

	connStr, err := xdcrf.remote_cluster_svc.GetConnectionStringForRemoteCluster(targetClusterRef, isCapiReplication)
	if err != nil {
		return nil, err
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := targetClusterRef.MyCredentials()
	if err != nil {
		return nil, err
	}

	targetBucketInfo, err := xdcrf.utils.GetBucketInfo(connStr, spec.TargetBucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, xdcrf.logger)
	if err != nil {
		return nil, err
	}

	isTargetES := xdcrf.utils.CheckWhetherClusterIsESBasedOnBucketInfo(targetBucketInfo)

	conflictResolutionType, err := xdcrf.utils.GetConflictResolutionTypeFromBucketInfo(spec.TargetBucketName, targetBucketInfo)
	if err != nil {
		return nil, err
	}

	// sourceCRMode is the conflict resolution mode to use when resolving conflicts for big documents at source side
	// capi replication always uses rev id based conflict resolution
	sourceCRMode := base.CRMode_RevId
	if !isCapiReplication {
		// for xmem replication, sourceCRMode is LWW if and only if target bucket is LWW enabled, so as to ensure that source side conflict
		// resolution and target side conflict resolution yield consistent results
		sourceCRMode = base.GetCRModeFromConflictResolutionTypeSetting(conflictResolutionType)
	}

	xdcrf.logger.Infof("%v sourceCRMode=%v httpAuthMech=%v isCapiReplication=%v isTargetES=%v\n", topic, sourceCRMode, httpAuthMech, isCapiReplication, isTargetES)

	/**
	 * Construct the Source nozzles
	 * sourceNozzles - a map of DCPNozzleID -> *DCPNozzle
	 * kv_vb_map - Map of SourceKVNode -> list of vbucket#'s that it's responsible for
	 */
	sourceNozzles, kv_vb_map, err := xdcrf.constructSourceNozzles(spec, topic, isCapiReplication, logger_ctx)
	if err != nil {
		return nil, err
	}
	if len(sourceNozzles) == 0 {
		// no pipeline is constructed if there is no source nozzle
		return nil, base.ErrorNoSourceNozzle
	}

	progress_recorder(fmt.Sprintf("%v source nozzles have been constructed", len(sourceNozzles)))

	xdcrf.logger.Infof("%v kv_vb_map=%v\n", topic, kv_vb_map)
	/**
	 * Construct the outgoing (Destination) nozzles
	 * 1. outNozzles - map of ID -> actual nozzle
	 * 2. vbNozzleMap - map of VBucket# -> nozzle to be used (to be used by router)
	 * 3. kvVBMap - map of remote KVNodes -> vbucket# responsible for per node
	 */
	outNozzles, vbNozzleMap, target_kv_vb_map, targetUserName, targetPassword, targetClusterVersion, err :=
		xdcrf.constructOutgoingNozzles(spec, kv_vb_map, sourceCRMode, targetBucketInfo, targetClusterRef, isCapiReplication, isTargetES, logger_ctx)

	if err != nil {
		return nil, err
	}
	progress_recorder(fmt.Sprintf("%v target nozzles have been constructed", len(outNozzles)))

	// TODO construct queue parts. This will affect vbMap in router. may need an additional outNozzle -> downStreamPart/queue map in constructRouter

	// construct routers to be able to connect the nozzles
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
				return nil, fmt.Errorf("%v There is no corresponding target nozzle for vb=%v, targetNozzleId=%v", topic, vb, targetNozzleId)
			}
			downStreamParts[targetNozzleId] = outNozzle
		}

		// Construct a router - each Source nozzle has a router.
		router, err := xdcrf.constructRouter(sourceNozzle.Id(), spec, downStreamParts, vbNozzleMap, sourceCRMode, logger_ctx)
		if err != nil {
			return nil, err
		}
		sourceNozzle.SetConnector(router)
	}
	progress_recorder("Source nozzles have been wired to target nozzles")

	// construct and initializes the pipeline
	pipeline := pp.NewPipelineWithSettingConstructor(topic, sourceNozzles, outNozzles, spec, targetClusterRef,
		xdcrf.ConstructSettingsForPart, xdcrf.ConstructSettingsForConnector, xdcrf.ConstructSSLPortMap, xdcrf.ConstructUpdateSettingsForPart,
		xdcrf.ConstructUpdateSettingsForConnector, xdcrf.SetStartSeqno, xdcrf.CheckpointBeforeStop, logger_ctx)

	// These listeners are the driving factors of the pipeline
	xdcrf.registerAsyncListenersOnSources(pipeline, logger_ctx)
	xdcrf.registerAsyncListenersOnTargets(pipeline, logger_ctx)

	// initialize component event listener map in pipeline
	pp.GetAllAsyncComponentEventListeners(pipeline)

	// Create PipelineContext
	if pipelineContext, err := pctx.NewWithSettingConstructor(pipeline, xdcrf.ConstructSettingsForService, xdcrf.ConstructUpdateSettingsForService, logger_ctx); err != nil {

		return nil, err
	} else {
		//register services to the pipeline context, so when pipeline context starts as part of the pipeline starting, these services will start as well
		pipeline.SetRuntimeContext(pipelineContext)
		err = xdcrf.registerServices(pipeline, logger_ctx, kv_vb_map, targetUserName, targetPassword, spec.TargetBucketName, target_kv_vb_map, targetClusterRef, targetClusterVersion, isCapiReplication, isTargetES)
		if err != nil {
			return nil, err
		}
	}

	progress_recorder("Pipeline has been constructed")

	xdcrf.logger.Infof("Pipeline %v has been constructed", topic)
	return pipeline, nil
}

func min(num1 int, num2 int) int {
	return int(math.Min(float64(num1), float64(num2)))
}

// get nozzle list from nozzle map
func getNozzleList(nozzle_map map[string]common.Nozzle) []common.Nozzle {
	nozzle_list := make([]common.Nozzle, len(nozzle_map))
	index := 0
	for _, nozzle := range nozzle_map {
		nozzle_list[index] = nozzle
		index++
	}
	return nozzle_list
}

// construct and register async componet event listeners on source nozzles
func (xdcrf *XDCRFactory) registerAsyncListenersOnSources(pipeline common.Pipeline, logger_ctx *log.LoggerContext) {
	sources := getNozzleList(pipeline.Sources())

	num_of_sources := len(sources)
	num_of_listeners := min(num_of_sources, base.MaxNumberOfAsyncListeners)
	load_distribution := base.BalanceLoad(num_of_listeners, num_of_sources)
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
		data_throughput_throttled_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataThroughputThrottledEventListener, i),
			pipeline.Topic(), logger_ctx)

		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			// Get the source DCP nozzle
			dcp_part := sources[index]

			// Stats manager will handle the data received and processed events
			dcp_part.RegisterComponentEventListener(common.DataReceived, data_received_event_listener)
			dcp_part.RegisterComponentEventListener(common.DataProcessed, data_processed_event_listener)
			dcp_part.RegisterComponentEventListener(common.SystemEventReceived, data_received_event_listener)

			dcp_part.RegisterComponentEventListener(common.DataFiltered, data_filtered_event_listener)
			dcp_part.RegisterComponentEventListener(common.DataUnableToFilter, data_filtered_event_listener)

			conn := dcp_part.Connector()
			conn.RegisterComponentEventListener(common.DataFiltered, data_filtered_event_listener)
			conn.RegisterComponentEventListener(common.DataUnableToFilter, data_filtered_event_listener)
			conn.RegisterComponentEventListener(common.DataThroughputThrottled, data_throughput_throttled_event_listener)
			conn.RegisterComponentEventListener(common.DataNotReplicated, data_filtered_event_listener)
		}
	}
}

// construct and register async componet event listeners on target nozzles
func (xdcrf *XDCRFactory) registerAsyncListenersOnTargets(pipeline common.Pipeline, logger_ctx *log.LoggerContext) {
	targets := getNozzleList(pipeline.Targets())
	num_of_targets := len(targets)
	num_of_listeners := min(num_of_targets, base.MaxNumberOfAsyncListeners)
	load_distribution := base.BalanceLoad(num_of_listeners, num_of_targets)
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
		data_throttled_event_listener := component.NewDefaultAsyncComponentEventListenerImpl(
			pipeline_utils.GetElementIdFromNameAndIndex(pipeline, base.DataThrottledEventListener, i),
			pipeline.Topic(), logger_ctx)

		for index := load_distribution[i][0]; index < load_distribution[i][1]; index++ {
			out_nozzle := targets[index]
			out_nozzle.RegisterComponentEventListener(common.DataSent, data_sent_event_listener)
			out_nozzle.RegisterComponentEventListener(common.DataFailedCRSource, data_failed_cr_event_listener)
			out_nozzle.RegisterComponentEventListener(common.GetMetaReceived, get_meta_received_event_listener)
			out_nozzle.RegisterComponentEventListener(common.DataThrottled, data_throttled_event_listener)
		}
	}
}

/**
 * Construct source nozzles for the requested/current kv node
 * Returns:
 * 1. a map of DCPNozzleID -> DCPNozzle (references/ptr, so only a single copy from here on out)
 * 2. Map of SourceKVNode -> list of vbucket#'s that it's responsible for
 * Currently since XDCR is run on a per node, it should only have 1 source KV node in the map
 */
func (xdcrf *XDCRFactory) constructSourceNozzles(spec *metadata.ReplicationSpecification,
	topic string,
	isCapiReplication bool,
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[string][]uint16, error) {
	sourceNozzles := make(map[string]common.Nozzle)

	maxNozzlesPerNode := spec.Settings.SourceNozzlePerNode

	// Get a map of kvNode -> vBuckets responsibile for
	kv_vb_map, _, err := pipeline_utils.GetSourceVBMap(xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, spec.SourceBucketName, xdcrf.logger)
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
		// load_distribution is used to ensure that every nozzle gets as close # of vbuckets as possible, with a max delta between them of 1
		load_distribution := base.BalanceLoad(numOfDcpNozzles, numOfVbs)
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
				spec.SourceBucketName, spec.TargetBucketName, vbList, xdcrf.xdcr_topology_svc, isCapiReplication, logger_ctx, xdcrf.utils)
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

/**
 * Given a list of target VBlist for a node, and a map of all sourceNode->VBucket lists
 * Filter so that the vbucket number of the target matches the vbucket number of the source,
 * meaning that this pipeline is actually responsible for replication of it.
 * Returns: slice of vbuckets
 */
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

/**
 * Constructs the outgoing nozzles
 * Returns:
 * 1. outNozzles - map of ID -> actual nozzle
 * 2. vbNozzleMap - map of VBucket# -> nozzle to be used (to be used by router)
 * 3. kvVBMap - map of remote KVNodes -> vbucket# responsible for per node
 * 4. targetUserName
 * 5. targetPassword
 * 6. targetVersion - target Cluster Version
 */
func (xdcrf *XDCRFactory) constructOutgoingNozzles(spec *metadata.ReplicationSpecification, kv_vb_map map[string][]uint16,
	sourceCRMode base.ConflictResolutionMode, targetBucketInfo map[string]interface{},
	targetClusterRef *metadata.RemoteClusterReference, isCapiReplication bool, isTargetES bool, logger_ctx *log.LoggerContext) (outNozzles map[string]common.Nozzle,
	vbNozzleMap map[uint16]string, kvVBMap map[string][]uint16, targetUserName string, targetPassword string, targetClusterVersion int, err error) {
	outNozzles = make(map[string]common.Nozzle)
	vbNozzleMap = make(map[uint16]string)

	useExternal, err := xdcrf.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
	if err != nil {
		xdcrf.logger.Errorf("Error getting alternate address preference, err=%v\n", err)
		return
	}

	// Get a Map of Remote kvNode -> vBucket#s it's responsible for
	kvVBMap, err = xdcrf.utils.GetRemoteServerVBucketsMap(targetClusterRef.HostName(), spec.TargetBucketName, targetBucketInfo, useExternal)
	if err != nil {
		xdcrf.logger.Errorf("Error getting server vbuckets map, err=%v\n", err)
		return
	}
	if len(kvVBMap) == 0 {
		err = base.ErrorNoTargetNozzle
		return
	}

	if isTargetES {
		targetUserName = targetClusterRef.UserName()
		targetPassword = targetClusterRef.Password()
		// set target cluster version to 0 so that topology change detector will not listen to target version change
		targetClusterVersion = 0
	} else {
		targetClusterVersion, err = xdcrf.utils.GetClusterCompatibilityFromBucketInfo(targetBucketInfo, xdcrf.logger)
		if err != nil {
			return
		}
		targetHasRBACSupport := base.IsClusterCompatible(targetClusterVersion, base.VersionForRBACAndXattrSupport)
		if targetHasRBACSupport {
			// if target is spock and up, simply use the username and password in remote cluster ref
			targetUserName = targetClusterRef.UserName()
			targetPassword = targetClusterRef.Password()
		} else {
			// if target is pre-spock, use bucket name and bucket password
			targetUserName = spec.TargetBucketName

			// get target bucket password
			bucketPwdObj, ok := targetBucketInfo[base.SASLPasswordKey]
			if !ok {
				err = fmt.Errorf("%v cannot get sasl password from target bucket, %v.", spec.Id, targetBucketInfo)
				return
			}
			bucketPwd, ok := bucketPwdObj.(string)
			if !ok {
				err = fmt.Errorf("%v sasl password on target bucket is of wrong type.", spec.Id, bucketPwdObj)
				return
			}
			targetPassword = bucketPwd
		}
	}
	xdcrf.logger.Infof("%v username for target bucket access=%v%v%v\n", spec.Id, base.UdTagBegin, targetUserName, base.UdTagEnd)

	maxTargetNozzlePerNode := spec.Settings.TargetNozzlePerNode
	xdcrf.logger.Infof("Target topology retrieved. kvVBMap = %v\n", kvVBMap)

	var vbCouchApiBaseMap map[uint16]string

	// For each destination host (kvaddr) and its vbucvket list that it has (kvVBList)
	for kvaddr, kvVBList := range kvVBMap {
		if isCapiReplication && len(vbCouchApiBaseMap) == 0 {
			// construct vbCouchApiBaseMap only when nessary and only once
			vbCouchApiBaseMap, err = capi_utils.ConstructVBCouchApiBaseMap(spec.TargetBucketName, targetBucketInfo, targetClusterRef, xdcrf.utils, useExternal)
			if err != nil {
				xdcrf.logger.Errorf("Failed to construct vbCouchApiBase map, err=%v\n", err)
				return
			}
		}

		// Given current Destination node's list of VBucketList and the map of all source nodes -> vbLists
		// Match the needed vbuckets
		relevantVBs := xdcrf.filterVBList(kvVBList /* Dest */, kv_vb_map /* source */)

		xdcrf.logger.Debugf("kvaddr = %v; kvVbList=%v, relevantVBs=-%v\n", kvaddr, kvVBList, relevantVBs)

		numOfVbs := len(relevantVBs)
		// the number of xmem nozzles to construct is the smaller of vbucket list size and target connection size
		numOfOutNozzles := min(numOfVbs, maxTargetNozzlePerNode)
		load_distribution := base.BalanceLoad(numOfOutNozzles, numOfVbs)
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

			if isCapiReplication {
				outNozzle, err = xdcrf.constructCAPINozzle(spec.Id, targetUserName, targetPassword, targetClusterRef.Certificate(), vbList, vbCouchApiBaseMap, i, logger_ctx)
				if err != nil {
					return
				}
			} else {
				connSize := numOfOutNozzles * 2
				outNozzle = xdcrf.constructXMEMNozzle(spec.Id, spec.TargetClusterUUID, kvaddr, spec.SourceBucketName, spec.TargetBucketName, spec.TargetBucketUUID, targetUserName, targetPassword, i, connSize, sourceCRMode, targetBucketInfo, logger_ctx, vbList)
			}

			// Add the created nozzle to the collective map of outNozzles to be returned
			outNozzles[outNozzle.Id()] = outNozzle

			// construct vbNozzleMap for the out nozzle, which is needed by the router
			// All vbuckets that are relevant are covered at the end of the double for loop
			for _, vbno := range vbList {
				// Each vb that is relevant and filtered through load_distrbution gets assigned to this nozzle
				// This will be used by the router
				vbNozzleMap[vbno] = outNozzle.Id()
			}

			xdcrf.logger.Debugf("Constructed out nozzle %v\n", outNozzle.Id())
		}
	}

	xdcrf.logger.Infof("Constructed %v outgoing nozzles\n", len(outNozzles))
	xdcrf.logger.Debugf("vbNozzleMap = %v\n", vbNozzleMap)
	return
}

func (xdcrf *XDCRFactory) constructRouter(id string, spec *metadata.ReplicationSpecification,
	downStreamParts map[string]common.Part,
	vbNozzleMap map[uint16]string,
	sourceCRMode base.ConflictResolutionMode,
	logger_ctx *log.LoggerContext) (*parts.Router, error) {
	routerId := "Router" + PART_NAME_DELIMITER + id
	// when initializing router, isHighReplication is set to true only if replication priority is High
	// for replications with Medium priority and ongoing flag set, isHighReplication will be updated to true
	// through a UpdateSettings() call to the router in the pipeline startup sequence before parts are started
	router, err := parts.NewRouter(routerId, spec, downStreamParts, vbNozzleMap, sourceCRMode,
		logger_ctx, pipeline_manager.NewMCRequestObj, xdcrf.utils, xdcrf.throughput_throttler_svc,
		spec.Settings.GetPriority() == base.PriorityTypeHigh, spec.Settings.GetExpDelMode(),
		xdcrf.collectionsManifestSvc)
	if err != nil {
		xdcrf.logger.Errorf("Error (%v) constructing router %v", err.Error(), routerId)
	} else {
		xdcrf.logger.Infof("Constructed router %v", routerId)
	}
	return router, err
}

func (xdcrf *XDCRFactory) getOutNozzleType(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (base.XDCROutgoingNozzleType, error) {
	switch spec.Settings.RepType {
	case metadata.ReplicationTypeXmem:
		return base.Xmem, nil
	case metadata.ReplicationTypeCapi:
		return base.Capi, nil
	default:
		// should never get here
		return -1, errors.New(fmt.Sprintf("Invalid replication type %v", spec.Settings.RepType))
	}
}

func (xdcrf *XDCRFactory) constructXMEMNozzle(topic string,
	targetClusterUuid string,
	kvaddr string,
	sourceBucketName string,
	targetBucketName string,
	targetBucketUuid string,
	username string,
	password string,
	nozzle_index int,
	connPoolSize int,
	sourceCRMode base.ConflictResolutionMode,
	targetBucketInfo map[string]interface{},
	logger_ctx *log.LoggerContext,
	vbList []uint16) common.Nozzle {
	// partIds of the xmem nozzles look like "xmem_$topic_$kvaddr_1"
	xmemNozzle_Id := xdcrf.partId(XMEM_NOZZLE_NAME_PREFIX, topic, kvaddr, nozzle_index)
	nozzle := parts.NewXmemNozzle(xmemNozzle_Id, xdcrf.remote_cluster_svc, targetClusterUuid, topic, topic, connPoolSize, kvaddr, sourceBucketName, targetBucketName,
		targetBucketUuid, username, password, pipeline_manager.RecycleMCRequestObj, sourceCRMode, logger_ctx, xdcrf.utils, vbList)
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
	nozzle := parts.NewCapiNozzle(capiNozzle_Id, topic, capiConnectionStr, username, password, certificate, subVBCouchApiBaseMap, pipeline_manager.RecycleMCRequestObj, logger_ctx, xdcrf.utils, vbList)
	return nozzle, nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings metadata.ReplicationSettingsMap,
	targetClusterRef *metadata.RemoteClusterReference, ssl_port_map map[string]uint16) (metadata.ReplicationSettingsMap, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for XmemNozzle %s", part.Id())
		return xdcrf.constructSettingsForXmemNozzle(pipeline, part, targetClusterRef, settings, ssl_port_map)
	} else if _, ok := part.(*parts.DcpNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for DcpNozzle %s", part.Id())
		return xdcrf.constructSettingsForDcpNozzle(pipeline, part.(*parts.DcpNozzle), settings)
	} else if _, ok := part.(*parts.CapiNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for CapiNozzle %s", part.Id())
		return xdcrf.constructSettingsForCapiNozzle(pipeline, settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) ConstructSettingsForConnector(pipeline common.Pipeline, connector common.Connector, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	if _, ok := connector.(*parts.Router); ok {
		xdcrf.logger.Debugf("Construct settings for Router %s", connector.Id())
		return xdcrf.constructSettingsForRouter(pipeline, settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) ConstructUpdateSettingsForPart(pipeline common.Pipeline, part common.Part, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct update settings for XmemNozzle %s", part.Id())
		return xdcrf.constructUpdateSettingsForXmemNozzle(pipeline, settings), nil
	} else if _, ok := part.(*parts.CapiNozzle); ok {
		xdcrf.logger.Debugf("Construct update settings for CapiNozzle %s", part.Id())
		return xdcrf.constructUpdateSettingsForCapiNozzle(pipeline, settings), nil
	} else if _, ok := part.(*parts.DcpNozzle); ok {
		xdcrf.logger.Debugf("Construct update settings for DcpNozzle %s", part.Id())
		return xdcrf.constructUpdateSettingsForDcpNozzle(pipeline, settings), nil
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) ConstructUpdateSettingsForConnector(pipeline common.Pipeline, connector common.Connector, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	if _, ok := connector.(*parts.Router); ok {
		xdcrf.logger.Debugf("Construct update settings for Router %s", connector.Id())
		// use the same constructSettingsForRouter() method as in ConstructSettingsForConnector()
		return xdcrf.constructSettingsForRouter(pipeline, settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForXmemNozzle(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) metadata.ReplicationSettingsMap {
	xmemSettings := make(metadata.ReplicationSettingsMap)

	optiRepThreshold, ok := settings[metadata.OptimisticReplicationThresholdKey]
	if ok {
		xmemSettings[parts.SETTING_OPTI_REP_THRESHOLD] = optiRepThreshold
	}

	return xmemSettings

}

func (xdcrf *XDCRFactory) constructUpdateSettingsForCapiNozzle(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) metadata.ReplicationSettingsMap {
	capiSettings := make(metadata.ReplicationSettingsMap)

	optiRepThreshold, ok := settings[metadata.OptimisticReplicationThresholdKey]
	if ok {
		capiSettings[parts.SETTING_OPTI_REP_THRESHOLD] = optiRepThreshold
	}

	return capiSettings
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForDcpNozzle(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) metadata.ReplicationSettingsMap {
	dcpSettings := make(metadata.ReplicationSettingsMap)

	vbTimestamp, ok := settings[base.VBTimestamps]
	if ok {
		dcpSettings[parts.DCP_VBTimestamp] = vbTimestamp
	}

	statsInterval, ok := settings[metadata.PipelineStatsIntervalKey]
	if ok {
		dcpSettings[parts.DCP_Stats_Interval] = statsInterval
	}

	dcpPriority, ok := settings[parts.DCP_Priority]
	if ok {
		dcpSettings[parts.DCP_Priority] = dcpPriority
	}

	return dcpSettings
}

func (xdcrf *XDCRFactory) SetStartSeqno(pipeline common.Pipeline) error {
	if pipeline == nil {
		return errors.New("pipeline=nil")
	}
	ckpt_mgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckpt_mgr == nil {
		return errors.New(fmt.Sprintf("CheckpointingManager has not been attached to pipeline %v", pipeline.Topic()))
	}
	return ckpt_mgr.(*pipeline_svc.CheckpointManager).SetVBTimestamps(pipeline.Topic())
}

func (xdcrf *XDCRFactory) CheckpointBeforeStop(pipeline common.Pipeline) error {
	if pipeline == nil {
		return errors.New("pipeline=nil")
	}
	ckpt_mgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckpt_mgr == nil {
		return errors.New(fmt.Sprintf("CheckpointingManager has not been attached to pipeline %v", pipeline.Topic()))
	}
	ckpt_mgr.(*pipeline_svc.CheckpointManager).CheckpointBeforeStop()
	return nil
}

func (xdcrf *XDCRFactory) constructSettingsForXmemNozzle(pipeline common.Pipeline, part common.Part,
	targetClusterRef *metadata.RemoteClusterReference, settings metadata.ReplicationSettingsMap,
	ssl_port_map map[string]uint16) (metadata.ReplicationSettingsMap, error) {
	xmemSettings := make(metadata.ReplicationSettingsMap)
	spec := pipeline.Specification()
	repSettings := spec.Settings
	xmemConnStr := part.(*parts.XmemNozzle).ConnStr()

	xmemSettings[parts.SETTING_BATCHCOUNT] = getSettingFromSettingsMap(settings, metadata.BatchCountKey, repSettings.BatchCount)
	xmemSettings[parts.SETTING_BATCHSIZE] = getSettingFromSettingsMap(settings, metadata.BatchSizeKey, repSettings.BatchSize)
	xmemSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	xmemSettings[parts.SETTING_BATCH_EXPIRATION_TIME] = time.Duration(float64(repSettings.MaxExpectedReplicationLag)*0.7) * time.Millisecond
	xmemSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThresholdKey, repSettings.OptimisticReplicationThreshold)
	xmemSettings[parts.SETTING_STATS_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, repSettings.StatsInterval)
	xmemSettings[parts.SETTING_COMPRESSION_TYPE] = base.GetCompressionType(getSettingFromSettingsMap(settings, metadata.CompressionTypeKey, repSettings.CompressionType).(int))

	xmemSettings[parts.XMEM_SETTING_DEMAND_ENCRYPTION] = targetClusterRef.DemandEncryption()
	xmemSettings[parts.XMEM_SETTING_CERTIFICATE] = targetClusterRef.Certificate()
	xmemSettings[parts.XMEM_SETTING_CLIENT_CERTIFICATE] = targetClusterRef.ClientCertificate()
	xmemSettings[parts.XMEM_SETTING_CLIENT_KEY] = targetClusterRef.ClientKey()
	xmemSettings[parts.XMEM_SETTING_ENCRYPTION_TYPE] = targetClusterRef.EncryptionType()
	if targetClusterRef.IsFullEncryption() {
		mem_ssl_port, ok := ssl_port_map[xmemConnStr]
		if !ok {
			return nil, fmt.Errorf("Can't get remote memcached ssl port for %v", xmemConnStr)
		}
		xdcrf.logger.Infof("mem_ssl_port=%v\n", mem_ssl_port)

		xmemSettings[parts.XMEM_SETTING_REMOTE_MEM_SSL_PORT] = mem_ssl_port
		xmemSettings[parts.XMEM_SETTING_SAN_IN_CERITICATE] = targetClusterRef.SANInCertificate()

		xdcrf.logger.Infof("xmemSettings=%v\n", xmemSettings.CloneAndRedact())
	}

	forceCollectionDisable, ok := settings[parts.ForceCollectionDisableKey]
	if ok {
		xmemSettings[parts.ForceCollectionDisableKey] = forceCollectionDisable
	}

	return xmemSettings, nil

}

func (xdcrf *XDCRFactory) constructSettingsForCapiNozzle(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (map[string]interface{}, error) {
	capiSettings := make(metadata.ReplicationSettingsMap)
	repSettings := pipeline.Specification().Settings

	capiSettings[parts.SETTING_BATCHCOUNT] = getSettingFromSettingsMap(settings, metadata.BatchCountKey, repSettings.BatchCount)
	capiSettings[parts.SETTING_BATCHSIZE] = getSettingFromSettingsMap(settings, metadata.BatchSizeKey, repSettings.BatchSize)
	capiSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	capiSettings[parts.SETTING_OPTI_REP_THRESHOLD] = getSettingFromSettingsMap(settings, metadata.OptimisticReplicationThresholdKey, repSettings.OptimisticReplicationThreshold)
	capiSettings[parts.SETTING_STATS_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, repSettings.StatsInterval)

	return capiSettings, nil

}

func (xdcrf *XDCRFactory) getTargetTimeoutEstimate(topic string) time.Duration {
	//TODO: implement
	//need to get the tcp ping time for the estimate
	return 100 * time.Millisecond
}

func (xdcrf *XDCRFactory) constructSettingsForDcpNozzle(pipeline common.Pipeline, part *parts.DcpNozzle, settings metadata.ReplicationSettingsMap) (map[string]interface{}, error) {
	xdcrf.logger.Debugf("Construct settings for DcpNozzle ....")
	dcpNozzleSettings := make(metadata.ReplicationSettingsMap)
	spec := pipeline.Specification()
	repSettings := spec.Settings

	ckpt_svc := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckpt_svc == nil {
		return nil, fmt.Errorf("No checkpoint manager has been registered with the pipeline %v", pipeline.Topic())
	}

	dcpNozzleSettings[parts.DCP_VBTimestampUpdater] = ckpt_svc.(*pipeline_svc.CheckpointManager).UpdateVBTimestamps
	dcpNozzleSettings[parts.DCP_Stats_Interval] = getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, repSettings.StatsInterval)
	if repSettings.IsCapi() {
		// For CAPI nozzle, do not allow DCP to have compression
		dcpNozzleSettings[parts.SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeNone)
		// CAPI nozzle also does not have collections support
		dcpNozzleSettings[parts.ForceCollectionDisableKey] = true
	} else {
		dcpNozzleSettings[parts.SETTING_COMPRESSION_TYPE] = base.GetCompressionType(getSettingFromSettingsMap(settings, metadata.CompressionTypeKey, repSettings.CompressionType).(int))
		getterFunc := func(manifestUid uint64) (*metadata.CollectionsManifest, error) {
			return xdcrf.collectionsManifestSvc.GetSpecificSourceManifest(spec, manifestUid)
		}
		dcpNozzleSettings[parts.DCP_Manifest_Getter] = service_def.CollectionsManifestReqFunc(getterFunc)
		forceCollectionDisable, ok := settings[parts.ForceCollectionDisableKey]
		if ok {
			dcpNozzleSettings[parts.ForceCollectionDisableKey] = forceCollectionDisable
		}
	}

	// dcp priority settings could have been set through replStatus.customSettings.
	dcpPriority, ok := settings[parts.DCP_Priority]
	if ok {
		dcpNozzleSettings[parts.DCP_Priority] = dcpPriority
	}

	return dcpNozzleSettings, nil
}

func (xdcrf *XDCRFactory) constructSettingsForRouter(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	routerSettings := make(metadata.ReplicationSettingsMap)

	isHighReplication, ok := settings[parts.IsHighReplicationKey]
	if ok {
		routerSettings[parts.IsHighReplicationKey] = isHighReplication
	}

	filterExpDelMode, ok := settings[parts.FilterExpDelKey]
	if ok {
		routerSettings[parts.FilterExpDelKey] = filterExpDelMode
	}

	forceCollectionDisable, ok := settings[parts.ForceCollectionDisableKey]
	if ok {
		routerSettings[parts.ForceCollectionDisableKey] = forceCollectionDisable
	}

	return routerSettings, nil
}

func (xdcrf *XDCRFactory) registerServices(pipeline common.Pipeline, logger_ctx *log.LoggerContext,
	kv_vb_map map[string][]uint16, targetUserName, targetPassword string, targetBucketName string,
	target_kv_vb_map map[string][]uint16, targetClusterRef *metadata.RemoteClusterReference,
	targetClusterVersion int, isCapi bool, isTargetES bool) error {

	ctx := pipeline.RuntimeContext()

	//register pipeline supervisor
	supervisor := pipeline_svc.NewPipelineSupervisor(base.PipelineSupervisorIdPrefix+pipeline.Topic(), logger_ctx,
		xdcrf.pipeline_failure_handler, xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, xdcrf.utils)
	err := ctx.RegisterService(base.PIPELINE_SUPERVISOR_SVC, supervisor)
	if err != nil {
		return err
	}

	// through seqno tracker needs to be initialized after pipeline supervisor
	// since it uses the latter as error handler
	through_seqno_tracker_svc := service_impl.NewThroughSeqnoTrackerSvc(logger_ctx)
	through_seqno_tracker_svc.Attach(pipeline)

	//Create pipeline statistics manager.
	bucket_name := pipeline.Specification().SourceBucketName
	actualStatsMgr := pipeline_svc.NewStatisticsManager(through_seqno_tracker_svc, xdcrf.cluster_info_svc,
		xdcrf.xdcr_topology_svc, logger_ctx, kv_vb_map, bucket_name, xdcrf.utils)

	//register pipeline checkpoint manager
	ckptMgr, err := pipeline_svc.NewCheckpointManager(xdcrf.checkpoint_svc, xdcrf.capi_svc,
		xdcrf.remote_cluster_svc, xdcrf.repl_spec_svc, xdcrf.cluster_info_svc,
		xdcrf.xdcr_topology_svc, through_seqno_tracker_svc, kv_vb_map, targetUserName,
		targetPassword, targetBucketName, target_kv_vb_map, targetClusterRef,
		targetClusterVersion, isTargetES, logger_ctx, xdcrf.utils, actualStatsMgr)
	if err != nil {
		xdcrf.logger.Errorf("Failed to construct CheckpointManager for %v. err=%v ckpt_svc=%v, capi_svc=%v, remote_cluster_svc=%v, repl_spec_svc=%v\n", pipeline.Topic(), err, xdcrf.checkpoint_svc, xdcrf.capi_svc,
			xdcrf.remote_cluster_svc, xdcrf.repl_spec_svc)
		return err
	}
	err = ctx.RegisterService(base.CHECKPOINT_MGR_SVC, ckptMgr)
	if err != nil {
		return err
	}
	// Register statistics manager after checkpoint manager is created
	err = ctx.RegisterService(base.STATISTICS_MGR_SVC, actualStatsMgr)
	if err != nil {
		return err
	}
	//register topology change detect service
	targetHasRBACAndXattrSupport := base.IsClusterCompatible(targetClusterVersion, base.VersionForRBACAndXattrSupport)
	top_detect_svc := pipeline_svc.NewTopologyChangeDetectorSvc(xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, xdcrf.remote_cluster_svc, xdcrf.repl_spec_svc, targetHasRBACAndXattrSupport, logger_ctx, xdcrf.utils)
	err = ctx.RegisterService(base.TOPOLOGY_CHANGE_DETECT_SVC, top_detect_svc)
	if err != nil {
		return err
	}

	if !isCapi {
		//register bandwidth throttler service
		bw_throttler_svc := pipeline_svc.NewBandwidthThrottlerSvc(xdcrf.xdcr_topology_svc, logger_ctx)
		err = ctx.RegisterService(base.BANDWIDTH_THROTTLER_SVC, bw_throttler_svc)
		if err != nil {
			return err
		}
		for _, target := range pipeline.Targets() {
			target.(*parts.XmemNozzle).SetBandwidthThrottler(bw_throttler_svc)
		}
	}

	return nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForService(pipeline common.Pipeline, service common.PipelineService, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	switch service.(type) {
	case *pipeline_svc.PipelineSupervisor:
		xdcrf.logger.Debug("Construct settings for PipelineSupervisor")
		return xdcrf.constructSettingsForSupervisor(pipeline, settings)
	case *pipeline_svc.StatisticsManager:
		xdcrf.logger.Debug("Construct settings for StatisticsManager")
		return xdcrf.constructSettingsForStatsManager(pipeline, settings)
	case *pipeline_svc.CheckpointManager:
		xdcrf.logger.Debug("Construct settings for CheckpointManager")
		return xdcrf.constructSettingsForCheckpointManager(pipeline, settings)
	}
	return settings, nil
}

// the major difference between ConstructSettingsForService and ConstructUpdateSettingsForService is that
// when a parameter is not specified, the former sets default value and the latter does nothing
func (xdcrf *XDCRFactory) ConstructUpdateSettingsForService(pipeline common.Pipeline, service common.PipelineService, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	switch service.(type) {
	case *pipeline_svc.PipelineSupervisor:
		xdcrf.logger.Debug("Construct update settings for PipelineSupervisor")
		return xdcrf.constructUpdateSettingsForSupervisor(pipeline, settings)
	case *pipeline_svc.StatisticsManager:
		xdcrf.logger.Debug("Construct update settings for StatisticsManager")
		return xdcrf.constructUpdateSettingsForStatsManager(pipeline, settings)
	case *pipeline_svc.CheckpointManager:
		xdcrf.logger.Debug("Construct update settings for CheckpointManager")
		return xdcrf.constructUpdateSettingsForCheckpointManager(pipeline, settings)
	case *pipeline_svc.BandwidthThrottler:
		xdcrf.logger.Debug("Construct update settings for BandwidthThrottler")
		return xdcrf.constructUpdateSettingsForBandwidthThrottler(pipeline, settings)
	}
	return settings, nil
}

func (xdcrf *XDCRFactory) constructSettingsForSupervisor(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	s := make(metadata.ReplicationSettingsMap)
	s[pipeline_svc.PUBLISH_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, pipeline.Specification().Settings.StatsInterval)
	log_level_str := getSettingFromSettingsMap(settings, metadata.PipelineLogLevelKey, pipeline.Specification().Settings.LogLevel.String())
	log_level, err := log.LogLevelFromStr(log_level_str.(string))
	if err != nil {
		return nil, err
	}
	s[pipeline_svc.PIPELINE_LOG_LEVEL] = log_level
	return s, nil
}

func (xdcrf *XDCRFactory) constructSettingsForStatsManager(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	s := make(metadata.ReplicationSettingsMap)
	s[pipeline_svc.PUBLISH_INTERVAL] = getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, pipeline.Specification().Settings.StatsInterval)
	return s, nil
}

func (xdcrf *XDCRFactory) constructSettingsForCheckpointManager(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	s := make(metadata.ReplicationSettingsMap)
	s[pipeline_svc.CHECKPOINT_INTERVAL] = getSettingFromSettingsMap(settings, metadata.CheckpointIntervalKey, pipeline.Specification().Settings.CheckpointInterval)
	return s, nil
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForSupervisor(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	s := make(metadata.ReplicationSettingsMap)
	log_level_str := getSettingFromSettingsMap(settings, metadata.PipelineLogLevelKey, nil)
	if log_level_str != nil {
		log_level, err := log.LogLevelFromStr(log_level_str.(string))
		if err != nil {
			return nil, err
		}
		s[pipeline_svc.PIPELINE_LOG_LEVEL] = log_level
	}

	publish_interval := getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, nil)
	if publish_interval != nil {
		s[pipeline_svc.PUBLISH_INTERVAL] = publish_interval
	}
	return s, nil
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForStatsManager(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	s := make(metadata.ReplicationSettingsMap)
	publish_interval := getSettingFromSettingsMap(settings, metadata.PipelineStatsIntervalKey, nil)
	if publish_interval != nil {
		s[pipeline_svc.PUBLISH_INTERVAL] = publish_interval
	}
	return s, nil
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForCheckpointManager(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	if xdcrf.logger.GetLogLevel() >= log.LogLevelDebug {
		xdcrf.logger.Debugf("constructUpdateSettingsForCheckpointManager called with settings=%v\n", settings.CloneAndRedact())
	}
	s := make(metadata.ReplicationSettingsMap)
	checkpoint_interval := getSettingFromSettingsMap(settings, metadata.CheckpointIntervalKey, nil)
	if checkpoint_interval != nil {
		s[pipeline_svc.CHECKPOINT_INTERVAL] = checkpoint_interval
	}
	return s, nil
}

func (xdcrf *XDCRFactory) constructUpdateSettingsForBandwidthThrottler(pipeline common.Pipeline, settings metadata.ReplicationSettingsMap) (metadata.ReplicationSettingsMap, error) {
	if xdcrf.logger.GetLogLevel() >= log.LogLevelDebug {
		xdcrf.logger.Debugf("constructUpdateSettingsForBandwidthThrottler called with settings=%v\n", settings.CloneAndRedact())
	}
	s := make(metadata.ReplicationSettingsMap)
	overall_bandwidth_limit := settings[metadata.BandwidthLimitKey]
	if overall_bandwidth_limit != nil {
		s[pipeline_svc.OVERALL_BANDWIDTH_LIMIT] = overall_bandwidth_limit
	}
	number_of_source_nodes := settings[pipeline_svc.NUMBER_OF_SOURCE_NODES]
	if number_of_source_nodes != nil {
		s[pipeline_svc.NUMBER_OF_SOURCE_NODES] = number_of_source_nodes
	}
	return s, nil
}

func getSettingFromSettingsMap(settings metadata.ReplicationSettingsMap, setting_name string, default_value interface{}) interface{} {
	if settings != nil {
		if setting, ok := settings[setting_name]; ok {
			return setting
		}
	}

	return default_value
}

func (xdcrf *XDCRFactory) ConstructSSLPortMap(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (map[string]uint16, error) {

	var ssl_port_map map[string]uint16
	nozzleType, err := xdcrf.getOutNozzleType(targetClusterRef, spec)
	if err != nil {
		return nil, err
	}
	// if both xmem nozzles and ssl are involved, populate ssl_port_map
	// if target cluster is post-3.0, the ssl ports in the map are memcached ssl ports
	// otherwise, the ssl ports in the map are proxy ssl ports
	if targetClusterRef.IsFullEncryption() && nozzleType == base.Xmem {

		username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := targetClusterRef.MyCredentials()
		if err != nil {
			return nil, err
		}
		connStr, err := targetClusterRef.MyConnectionStr()
		if err != nil {
			return nil, err
		}
		useExternal, err := xdcrf.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
		if err != nil {
			return nil, err
		}

		ssl_port_map, err = xdcrf.utils.GetMemcachedSSLPortMap(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey,
			spec.TargetBucketName, xdcrf.logger, useExternal)
		if err != nil {
			xdcrf.logger.Errorf("Failed to get memcached ssl port, err=%v\n", err)
			return nil, err
		}

		xdcrf.logger.Debugf("ssl_port_map=%v\n", ssl_port_map)
	}

	return ssl_port_map, nil
}
