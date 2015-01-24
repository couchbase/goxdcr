package factory

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/capi_utils"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pp "github.com/couchbase/goxdcr/pipeline"
	pctx "github.com/couchbase/goxdcr/pipeline_ctx"
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

	default_logger_ctx         *log.LoggerContext
	pipeline_failure_handler   common.SupervisorFailureHandler
	logger                     *log.CommonLogger
	pipeline_master_supervisor *supervisor.GenericSupervisor
}

//var xdcrf.logger *log.CommonLogger = log.NewLogger("XDCRFactory", log.LogLevelInfo)

// set call back functions is done only once
func NewXDCRFactory(repl_spec_svc service_def.ReplicationSpecSvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	checkpoint_svc service_def.CheckpointsService,
	capi_svc service_def.CAPIService,
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
		default_logger_ctx:         pipeline_default_logger_ctx,
		pipeline_failure_handler:   pipeline_failure_handler,
		pipeline_master_supervisor: pipeline_master_supervisor,
		logger: log.NewLogger("XDCRFactory", factory_logger_ctx)}
}

func (xdcrf *XDCRFactory) NewPipeline(topic string, progress_recorder common.PipelineProgressRecorder) (common.Pipeline, error) {
	spec, err := xdcrf.repl_spec_svc.ReplicationSpec(topic)
	xdcrf.logger.Debugf("replication specification = %v\n", spec)
	if err != nil {
		xdcrf.logger.Errorf("Failed to get replication specification, err=%v\n", err)
		return nil, err
	}

	logger_ctx := log.CopyCtx(xdcrf.default_logger_ctx)
	logger_ctx.Log_level = spec.Settings.LogLevel
	//TODO should we change log level on xdcrf.logger?

	// popuplate pipeline using config
	sourceNozzles, kv_vb_map, err := xdcrf.constructSourceNozzles(spec, topic, logger_ctx)
	if err != nil {
		return nil, err
	}
	if len(sourceNozzles) == 0 {
		// no pipeline is constructed if there is no source nozzle
		return nil, ErrorNoSourceNozzle
	}

	progress_recorder(fmt.Sprintf("%v source nozzles have been constructed", len(sourceNozzles)))

	xdcrf.logger.Infof("kv_vb_map=%v\n", kv_vb_map)
	outNozzles, vbNozzleMap, err := xdcrf.constructOutgoingNozzles(spec, kv_vb_map, logger_ctx)
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
			targetNozzleId := vbNozzleMap[vb]

			downStreamParts[targetNozzleId] = outNozzles[targetNozzleId]
		}

		router, err := xdcrf.constructRouter(sourceNozzle.Id(), spec, downStreamParts, vbNozzleMap, logger_ctx)
		if err != nil {
			return nil, err
		}
		sourceNozzle.SetConnector(router)
	}
	progress_recorder("Source nozzles are wired to target nozzles")

	// construct pipeline
	pipeline := pp.NewPipelineWithSettingConstructor(topic, sourceNozzles, outNozzles, spec, xdcrf.ConstructSettingsForPart, xdcrf.SetStartSeqno, xdcrf.remote_cluster_svc.RemoteClusterByUuid, logger_ctx)
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

	xdcrf.logger.Infof("XDCR pipeline constructed")
	return pipeline, nil
}

// construct source nozzles for the requested/current kv node
func (xdcrf *XDCRFactory) constructSourceNozzles(spec *metadata.ReplicationSpecification,
	topic string,
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[string][]uint16, error) {
	sourceNozzles := make(map[string]common.Nozzle)

	bucketName := spec.SourceBucketName

	maxNozzlesPerNode := spec.Settings.SourceNozzlePerNode

	kv_vb_map, err := pipeline_utils.GetSourceVBListForReplication(xdcrf.cluster_info_svc, xdcrf.xdcr_topology_svc, spec, xdcrf.logger)
	if err != nil {
		return nil, nil, err
	}

	for kvaddr, vbnos := range kv_vb_map {

		numOfVbs := len(vbnos)

		if numOfVbs == 0 {
			continue
		}

		// the number of dcpNozzle nodes to construct is the smaller of vbucket list size and source connection size
		numOfDcpNozzles := int(math.Min(float64(numOfVbs), float64(maxNozzlesPerNode)))

		numOfVbPerDcpNozzle := int(math.Ceil(float64(numOfVbs) / float64(numOfDcpNozzles)))

		var index int
		for i := 0; i < numOfDcpNozzles; i++ {
			// construct vbList for the dcpNozzle
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among dcpNozzles

			//bucket has to be created for each DcpNozzle as it uses its underline
			//connection. Each Upr connection needs a separate socket
			//TODO: look into if different DcpNozzles for the same kv node can share a
			//upr connection
			bucket, err := xdcrf.cluster_info_svc.GetBucket(xdcrf.xdcr_topology_svc, bucketName)
			if err != nil {
				xdcrf.logger.Errorf("Error getting bucket. i=%d, err=%v\n", i, err)
				return nil, nil, err
			}
			vbList := make([]uint16, 0, 15)
			for j := 0; j < numOfVbPerDcpNozzle; j++ {
				if index < numOfVbs {
					vbList = append(vbList, vbnos[index])
					index++
				} else {
					// no more vbs to process
					break
				}
			}

			// construct dcpNozzles
			// partIds of the dcpNozzle nodes look like "dcpNozzle_$kvaddr_1"
			id := xdcrf.partId(DCP_NOZZLE_NAME_PREFIX, spec.Id, kvaddr, i)
			dcpNozzle := parts.NewDcpNozzle(id,
				bucket, vbList, logger_ctx)
			sourceNozzles[dcpNozzle.Id()] = dcpNozzle
			xdcrf.logger.Debugf("Constructed source nozzle %v with vbList = %v \n", dcpNozzle.Id(), vbList)
		}

		total := 0
		for _, sourceNozzle := range sourceNozzles {
			total = total + len(sourceNozzle.(*parts.DcpNozzle).GetVBList())
		}

		if total != numOfVbs {
			panic(fmt.Sprintf("numOfVbs = %v, total =%v", numOfVbs, total))
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
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[uint16]string, error) {
	outNozzles := make(map[string]common.Nozzle)
	vbNozzleMap := make(map[uint16]string)

	targetBucketName := spec.TargetBucketName
	targetClusterRef, err := xdcrf.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err != nil {
		xdcrf.logger.Errorf("Error getting remote cluster with uuid=%v, err=%v\n", spec.TargetClusterUUID, err)
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
	xdcrf.logger.Debugf("Target topology retrived. kvVBMap = %v\n", kvVBMap)

	var vbCouchApiBaseMap map[uint16]string

	nozzleType, err := xdcrf.getNozzleType(targetClusterRef, spec)
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
		numOfNozzles := int(math.Min(float64(numOfVbs), float64(maxTargetNozzlePerNode)))

		numOfVbPerNozzle := int(math.Ceil(float64(numOfVbs) / float64(numOfNozzles)))
		xdcrf.logger.Debugf("maxTargetNozzlePerNode=%d\n", maxTargetNozzlePerNode)
		xdcrf.logger.Debugf("Constructing %d nozzles, each is responsible for %d vbuckets\n", numOfNozzles, numOfVbPerNozzle)
		var index int = 0
		for i := 0; i < numOfNozzles; i++ {
			// construct vb list for the out nozzle, which is needed by capi nozzle
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among out nozzles
			vbList := make([]uint16, 0)
			for i := 0; i < numOfVbPerNozzle; i++ {
				if index < numOfVbs {
					vbList = append(vbList, relevantVBs[index])
					index++
				} else {
					// no more vbs to process
					break
				}
			}

			// construct outgoing nozzle
			var outNozzle common.Nozzle
			if isCapiNozzle {
				outNozzle, err = xdcrf.constructCAPINozzle(spec.Id, targetClusterRef.UserName, targetClusterRef.Password, targetClusterRef.Certificate, vbList, vbCouchApiBaseMap, i, logger_ctx)
				if err != nil {
					return nil, nil, err
				}
			} else {
				connSize := numOfNozzles * 2
				outNozzle = xdcrf.constructXMEMNozzle(spec.Id, kvaddr, targetBucketName, bucketPwd, i, connSize, logger_ctx)
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
	logger_ctx *log.LoggerContext) (*parts.Router, error) {
	routerId := "Router" + PART_NAME_DELIMITER + id
	router, err := parts.NewRouter(routerId, spec.Settings.FilterExpression, downStreamParts, vbNozzleMap, logger_ctx)
	xdcrf.logger.Infof("Constructed router")
	return router, err
}

func (xdcrf *XDCRFactory) getNozzleType(targetClusterRef *metadata.RemoteClusterReference, spec *metadata.ReplicationSpecification) (base.XDCROutgoingNozzleType, error) {
	switch spec.Settings.RepType {
	case metadata.ReplicationTypeXmem:
		xmemCompatible, err := xdcrf.cluster_info_svc.IsClusterCompatible(targetClusterRef, []int{2, 5})
		if err != nil {
			xdcrf.logger.Errorf("Failed to get the version information, err=%v\n", err)
			return -1, err
		}
		if xmemCompatible {
			return base.Xmem, nil
		} else {
			xdcrf.logger.Infof("Using capi nozzle for %v since the node is not xmem compatible\n", targetClusterRef.HostName)
			return base.Capi, nil
		}
	case metadata.ReplicationTypeCapi:
		return base.Capi, nil
	default:
		// should never get here
		return base.Xmem, errors.New(fmt.Sprintf("Invalid replication type %v", spec.Settings.RepType))
	}
}

func (xdcrf *XDCRFactory) constructXMEMNozzle(topic string, kvaddr string,
	bucketName string,
	bucketPwd string,
	nozzle_index int,
	connPoolSize int,
	logger_ctx *log.LoggerContext) common.Nozzle {
	// partIds of the xmem nozzles look like "xmem_$topic_$kvaddr_1"
	xmemNozzle_Id := xdcrf.partId(XMEM_NOZZLE_NAME_PREFIX, topic, kvaddr, nozzle_index)
	nozzle := parts.NewXmemNozzle(xmemNozzle_Id, topic, connPoolSize, kvaddr, bucketName, bucketPwd, logger_ctx)
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
	nozzle := parts.NewCapiNozzle(capiNozzle_Id, capiConnectionStr, username, password, certificate, subVBCouchApiBaseMap, logger_ctx)
	return nozzle, nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp, targetClusterRef *metadata.RemoteClusterReference) (map[string]interface{}, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for XmemNozzle %s", part.Id())
		return xdcrf.constructSettingsForXmemNozzle(pipeline, part, targetClusterRef, settings)
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

func (xdcrf *XDCRFactory) constructSettingsForXmemNozzle(pipeline common.Pipeline, part common.Part, targetClusterRef *metadata.RemoteClusterReference, settings map[string]interface{}) (map[string]interface{}, error) {
	xmemSettings := make(map[string]interface{})
	spec := pipeline.Specification()
	repSettings := spec.Settings
	xmemConnStr := part.(*parts.XmemNozzle).ConnStr()

	xmemSettings[parts.SETTING_BATCHCOUNT] = repSettings.BatchCount
	xmemSettings[parts.SETTING_BATCHSIZE] = repSettings.BatchSize
	xmemSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	xmemSettings[parts.SETTING_BATCH_EXPIRATION_TIME] = time.Duration(float64(repSettings.MaxExpectedReplicationLag)*0.7) * time.Millisecond
	xmemSettings[parts.SETTING_OPTI_REP_THRESHOLD] = repSettings.OptimisticReplicationThreshold
	xmemSettings[parts.SETTING_STATS_INTERVAL] = repSettings.StatsInterval

	demandEncryption := targetClusterRef.DemandEncryption
	certificate := targetClusterRef.Certificate
	if demandEncryption {
		sslOverMem, err := xdcrf.cluster_info_svc.IsClusterCompatible(targetClusterRef, []int{3, 0})
		if err != nil {
			return nil, err
		}
		xdcrf.logger.Infof("sslOverMem=%v\n", sslOverMem)
		var ssl_map map[string]uint16
		if sslOverMem {
			ssl_map, err = utils.GetMemcachedSSLPort(targetClusterRef.HostName, targetClusterRef.UserName, targetClusterRef.Password, xdcrf.logger)
			if err != nil {
				xdcrf.logger.Infof("Failed to get memcached ssl port, err=%v\n", err)
				return nil, err
			}
			mem_ssl_port, ok := ssl_map[xmemConnStr]
			if !ok {
				return nil, fmt.Errorf("Can't get remote memcached ssl port for %v", xmemConnStr)
			}
			xdcrf.logger.Infof("mem_ssl_port=%v\n", mem_ssl_port)

			xmemSettings[parts.XMEM_SETTING_REMOTE_MEM_SSL_PORT] = mem_ssl_port
			xmemSettings[parts.XMEM_SETTING_CERTIFICATE] = certificate
			xmemSettings[parts.XMEM_SETTING_DEMAND_ENCRYPTION] = demandEncryption

		} else {
			local_proxy_port, err := xdcrf.xdcr_topology_svc.MyProxyPort()

			if err != nil {
				return nil, err
			}

			var remote_proxy_port uint16 = 0
			targetBucket, err := xdcrf.cluster_info_svc.GetBucket(targetClusterRef, spec.TargetBucketName)
			if err != nil {
				xdcrf.logger.Errorf("Error getting bucket. err=%v\n", err)
				return nil, err
			}
			defer targetBucket.Close()

			for _, node := range targetBucket.Nodes() {
				hostname := utils.GetHostName(node.Hostname)
				memcachedPort := uint16(node.Ports[base.DirectPortKey])
				hostAddr := utils.GetHostAddr(hostname, memcachedPort)
				if hostAddr == xmemConnStr {
					remote_proxy_port = uint16(node.Ports[base.SSLProxyPortKey])
					break
				}
			}
			if remote_proxy_port == 0 {
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

	capiSettings[parts.SETTING_BATCHCOUNT] = repSettings.BatchCount
	capiSettings[parts.SETTING_BATCHSIZE] = repSettings.BatchSize
	capiSettings[parts.SETTING_RESP_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(pipeline.Topic())
	capiSettings[parts.SETTING_OPTI_REP_THRESHOLD] = repSettings.OptimisticReplicationThreshold
	capiSettings[parts.SETTING_STATS_INTERVAL] = repSettings.StatsInterval

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
	return dcpNozzleSettings, nil
}

func (xdcrf *XDCRFactory) registerServices(pipeline common.Pipeline, logger_ctx *log.LoggerContext, kv_vb_map map[string][]uint16) error {
	through_seqno_tracker_svc := service_impl.NewThroughSeqnoTrackerSvc(logger_ctx)
	through_seqno_tracker_svc.Attach(pipeline)

	ctx := pipeline.RuntimeContext()

	//register pipeline supervisor
	supervisor := pipeline_svc.NewPipelineSupervisor(base.PipelineSupervisorIdPrefix+pipeline.Topic(), logger_ctx, xdcrf.pipeline_failure_handler, xdcrf.pipeline_master_supervisor)
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
	err = ctx.RegisterService(base.STATISTICS_MGR_SVC, pipeline_svc.NewStatisticsManager(through_seqno_tracker_svc, logger_ctx, kv_vb_map, bucket_name))
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
	}
	return settings, nil
}

func (xdcrf *XDCRFactory) constructSettingsForSupervisor(pipeline common.Pipeline, settings map[string]interface{}) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	repSettings := pipeline.Specification().Settings
	s[pipeline_svc.PIPELINE_LOG_LEVEL] = repSettings.LogLevel
	return s, nil
}

func (xdcrf *XDCRFactory) constructSettingsForStatsManager(pipeline common.Pipeline, settings map[string]interface{}, ts map[uint16]*base.VBTimestamp) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	repSettings := pipeline.Specification().Settings

	s[pipeline_svc.PUBLISH_INTERVAL] = time.Duration(repSettings.StatsInterval) * time.Millisecond

	//set the start vb sequence no map to statistics manager
	if pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC) == nil {
		return nil, errors.New("No checkpoint manager is registered with the pipeline")
	}

	s[pipeline_svc.VB_START_TS] = ts

	return s, nil
}
