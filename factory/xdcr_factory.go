package factory

import (
	"errors"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	pctx "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_ctx"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata_svc"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/pipeline_svc"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	xdcr_pipeline_svc "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/pipeline_svc"
	"github.com/couchbase/indexing/secondary/protobuf"
	sp "github.com/ysui6888/indexing/secondary/projector"
	"math"
	"strconv"
	"time"
)

const (
	PART_NAME_DELIMITER     = "_"
	KVFEED_NAME_PREFIX      = "kvfeed"
	XMEM_NOZZLE_NAME_PREFIX = "xmem"
)

// Factory for XDCR pipelines
type XDCRFactory struct {
	metadata_svc             metadata_svc.MetadataSvc
	cluster_info_svc         metadata_svc.ClusterInfoSvc
	xdcr_topology_svc        metadata_svc.XDCRCompTopologySvc
	default_logger_ctx       *log.LoggerContext
	pipeline_failure_handler base.PipelineFailureHandler
	logger                   *log.CommonLogger
}

//var xdcrf.logger *log.CommonLogger = log.NewLogger("XDCRFactory", log.LogLevelInfo)

// set call back functions is done only once
func NewXDCRFactory(metadata_svc metadata_svc.MetadataSvc,
	cluster_info_svc metadata_svc.ClusterInfoSvc,
	xdcr_topology_svc metadata_svc.XDCRCompTopologySvc,
	pipeline_default_logger_ctx *log.LoggerContext,
	factory_logger_ctx *log.LoggerContext,
	pipeline_failure_handler base.PipelineFailureHandler) *XDCRFactory {
	return &XDCRFactory{metadata_svc: metadata_svc,
		cluster_info_svc:         cluster_info_svc,
		xdcr_topology_svc:        xdcr_topology_svc,
		default_logger_ctx:       pipeline_default_logger_ctx,
		pipeline_failure_handler: pipeline_failure_handler,
		logger: log.NewLogger("XDCRFactory", factory_logger_ctx)}
}

func (xdcrf *XDCRFactory) NewPipeline(topic string) (common.Pipeline, error) {
	logger_ctx := log.CopyCtx(xdcrf.default_logger_ctx)

	spec, err := xdcrf.metadata_svc.ReplicationSpec(topic)
	xdcrf.logger.Debugf("replication specification = %v\n", spec)
	if err != nil {
		xdcrf.logger.Errorf("err=%v\n", err)
		return nil, err
	}

	// popuplate pipeline using config
	sourceNozzles, err := xdcrf.constructSourceNozzles(spec, topic, logger_ctx)
	if err != nil {
		return nil, err
	}

	outNozzles, vbNozzleMap, err := xdcrf.constructOutgoingNozzles(spec, logger_ctx)
	if err != nil {
		return nil, err
	}

	// TODO construct queue parts. This will affect vbMap in router. may need an additional outNozzle -> downStreamPart/queue map in constructRouter
	downStreamParts := make(map[string]common.Part)
	for partId, outNozzle := range outNozzles {
		downStreamParts[partId] = outNozzle
	}

	router, err := xdcrf.constructRouter(spec, downStreamParts, vbNozzleMap, logger_ctx)
	if err != nil {
		return nil, err
	}

	// connect parts
	for _, sourceNozzle := range sourceNozzles {
		sourceNozzle.SetConnector(router)
	}

	// construct pipeline
	pipeline := pp.NewPipelineWithSettingConstructor(topic, sourceNozzles, outNozzles, xdcrf.ConstructSettingsForPart, logger_ctx)
	if pipelineContext, err := pctx.NewWithSettingConstructor(pipeline, xdcrf.ConstructSettingsForService, logger_ctx); err != nil {
		return nil, err
	} else {

		//register services
		pipeline.SetRuntimeContext(pipelineContext)
		xdcrf.registerServices(pipeline, logger_ctx)
	}

	xdcrf.logger.Infof("XDCR pipeline constructed")
	return pipeline, nil
}

// construct source nozzles for the requested/current kv node
// question: should kvaddr be passed to factory, or can factory figure it out by itself through some GetCurrentNode API?
func (xdcrf *XDCRFactory) constructSourceNozzles(spec *metadata.ReplicationSpecification,
	topic string,
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, error) {
	sourceNozzles := make(map[string]common.Nozzle)

	kvHosts, err := xdcrf.xdcr_topology_svc.MyKVNodes()
	if err != nil {
		xdcrf.logger.Errorf("err=%v\n", err)
		return nil, err
	}
	xdcrf.logger.Debugf("kvHosts=%v\n", kvHosts)

	bucketName := spec.SourceBucketName

	sourceClusterUUID := spec.SourceClusterUUID

	maxNozzlesPerNode := spec.Settings.SourceNozzlePerNode

	serverVBMap, err := xdcrf.cluster_info_svc.GetServerVBucketsMap(sourceClusterUUID, bucketName)
	if err != nil {
		xdcrf.logger.Errorf("err=%v\n", err)
		return nil, err
	}

	for _, kvHost := range kvHosts {
		kvaddr := utils.GetHostAddr(kvHost, base.KVPortNumber)
		vbnos := serverVBMap[kvaddr]

		numOfVbs := len(vbnos)

		// the number of kvfeed nodes to construct is the smaller of vbucket list size and source connection size
		numOfKVFeeds := int(math.Min(float64(numOfVbs), float64(maxNozzlesPerNode)))

		numOfVbPerKVFeed := int(math.Ceil(float64(numOfVbs) / float64(numOfKVFeeds)))

		var index int
		for i := 0; i < numOfKVFeeds; i++ {
			// construct vbList for the kvfeed
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among kvfeeds

			//bucket has to be created for each KVFeed as it uses its underline
			//connection. Each Upr connection needs a separate socket
			//TODO: look into if different KVFeeds for the same kv node can share a
			//upr connection
			bucket, err := xdcrf.cluster_info_svc.GetBucket(sourceClusterUUID, bucketName)
			if err != nil {
				xdcrf.logger.Errorf("Error getting bucket. i=%d, err=%v\n", i, err)
				return nil, err
			}
			vbList := make([]uint16, 0, 15)
			for i := 0; i < numOfVbPerKVFeed; i++ {
				if index < numOfVbs {
					vbList = append(vbList, vbnos[index])
					index++
				} else {
					// no more vbs to process
					break
				}
			}

			// construct kvfeeds
			// partIds of the kvfeed nodes look like "kvfeed_$kvaddr_1"
			kvfeed, err := sp.NewKVFeed(kvaddr, topic, KVFEED_NAME_PREFIX + PART_NAME_DELIMITER + kvaddr + PART_NAME_DELIMITER + strconv.Itoa(i), bucket, vbList)
			if err != nil {
				xdcrf.logger.Errorf("Error on NewKVFeed. i=%d, err=%v\n", i, err)
				return nil, err
			}
			sourceNozzles[kvfeed.Id()] = kvfeed
			xdcrf.logger.Debugf("Constructed source nozzle %v with vbList = %v \n", kvfeed.Id(), vbList)
		}
	}
	xdcrf.logger.Infof("Constructed %v source nozzles\n", len(sourceNozzles))

	return sourceNozzles, nil
}

func (xdcrf *XDCRFactory) constructOutgoingNozzles(spec *metadata.ReplicationSpecification,
	logger_ctx *log.LoggerContext) (map[string]common.Nozzle, map[uint16]string, error) {
	outNozzles := make(map[string]common.Nozzle)
	vbNozzleMap := make(map[uint16]string)

	//	kvVBMap, bucketPwd, err := (*xdcr_factory.get_target_topology_callback)(config.TargetCluster, config.TargetBucketn)
	targetClusterUUID := spec.TargetClusterUUID
	targetBucketName := spec.TargetBucketName

	kvVBMap, err := xdcrf.cluster_info_svc.GetServerVBucketsMap(targetClusterUUID, targetBucketName)
	if err != nil {
		xdcrf.logger.Errorf("Error getting server vbuckets map, err=%v\n", err)
		return nil, nil, err
	}

	targetBucket, err := xdcrf.cluster_info_svc.GetBucket(targetClusterUUID, targetBucketName)
	if err != nil {
		xdcrf.logger.Errorf("Error getting bucket, err=%v\n", err)
		return nil, nil, err
	}

	bucketPwd := targetBucket.Password
	maxTargetNozzlePerNode := spec.Settings.TargetNozzlePerNode
	xdcrf.logger.Debugf("Target topology retrived. kvVBMap = %v\n", kvVBMap)

	for kvaddr, kvVBList := range kvVBMap {
		numOfVbs := len(kvVBList)
		// the number of xmem nozzles to construct is the smaller of vbucket list size and target connection size
		numOfNozzles := int(math.Min(float64(numOfVbs), float64(maxTargetNozzlePerNode)))

		numOfVbPerNozzle := int(math.Ceil(float64(numOfVbs) / float64(numOfNozzles)))
		xdcrf.logger.Debugf("maxTargetNozzlePerNode=%d\n", maxTargetNozzlePerNode)
		xdcrf.logger.Debugf("Constructing %d nozzles, each is responsible for %d vbuckets\n", numOfNozzles, numOfVbPerNozzle)
		var index int = 0
		for i := 0; i < numOfNozzles; i++ {

			// construct xmem nozzle
			// partIds of the xmem nozzles look like "xmem_$kvaddr_1"
			outNozzle, err := xdcrf.constructNozzleForTargetNode(kvaddr, targetBucketName, bucketPwd, i, logger_ctx)

			if err != nil {
				xdcrf.logger.Errorf("err=%v\n", err)
				return nil, nil, err
			}

			outNozzles[outNozzle.Id()] = outNozzle

			// construct vbMap for the out nozzle, which is needed by the router
			// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among out nozzles
			for i := 0; i < numOfVbPerNozzle; i++ {
				if index < numOfVbs {
					vbNozzleMap[kvVBList[index]] = outNozzle.Id()
					index++
				} else {
					// no more vbs to process
					break
				}
			}

			xdcrf.logger.Debugf("Constructed out nozzle %v\n", outNozzle.Id())
		}
	}

	xdcrf.logger.Infof("Constructed %v outgoing nozzles\n", len(outNozzles))
	xdcrf.logger.Debugf("vbNozzleMap = %v\n", vbNozzleMap)
	return outNozzles, vbNozzleMap, nil
}

func (xdcrf *XDCRFactory) constructRouter(spec *metadata.ReplicationSpecification,
	downStreamParts map[string]common.Part,
	vbNozzleMap map[uint16]string,
	logger_ctx *log.LoggerContext) (*parts.Router, error) {
	router, err := parts.NewRouter(spec.Settings.FilterExpression, downStreamParts, vbNozzleMap, logger_ctx)
	xdcrf.logger.Infof("Constructed router")
	return router, err
}

func (xdcrf *XDCRFactory) constructNozzleForTargetNode(kvaddr string,
	bucketName string,
	bucketPwd string,
	nozzle_index int,
	logger_ctx *log.LoggerContext) (common.Nozzle, error) {
	var nozzle common.Nozzle
	nozzleType, err := xdcrf.getNozzleType(kvaddr)

	if err != nil {
		xdcrf.logger.Errorf("err=%v\n", err)
		return nil, err
	}

	switch nozzleType {
	case base.Xmem:
		nozzle = xdcrf.constructXMEMNozzle(kvaddr, bucketName, bucketPwd, nozzle_index, logger_ctx)
	case base.Capi:
		nozzle = xdcrf.constructCAPINozzle(kvaddr, bucketName, bucketPwd, nozzle_index, logger_ctx)
	}

	return nozzle, err

}

func (xdcrf *XDCRFactory) getNozzleType(kvaddr string) (base.XDCROutgoingNozzleType, error) {
	beforeXMEM, err := xdcrf.cluster_info_svc.IsNodeCompatible(kvaddr, "2.5")
	if err != nil {
		xdcrf.logger.Errorf("err=%v\n", err)
		return -1, err
	}

	if beforeXMEM {
		return base.Xmem, nil
	} else {
		return base.Capi, nil
	}
}

func (xdcrf *XDCRFactory) constructXMEMNozzle(kvaddr string,
	bucketName string,
	bucketPwd string,
	nozzle_index int,
	logger_ctx *log.LoggerContext) common.Nozzle {
	xmemNozzle_Id := XMEM_NOZZLE_NAME_PREFIX + PART_NAME_DELIMITER + kvaddr + PART_NAME_DELIMITER + strconv.Itoa(nozzle_index)
	nozzle := parts.NewXmemNozzle(xmemNozzle_Id, kvaddr, bucketName, bucketPwd, logger_ctx)
	return nozzle
}

func (xdcrf *XDCRFactory) constructCAPINozzle(kvaddr string,
	bucketName string,
	bucketPwd string,
	nozzle_index int,
	logger_ctx *log.LoggerContext) common.Nozzle {
	//TODO: implement it
	return nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings map[string]interface{}) (map[string]interface{}, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		xdcrf.logger.Debugf("Construct settings for XmemNozzle %s", part.Id())
		return xdcrf.constructSettingsForXmemNozzle(pipeline.Topic(), settings)
	} else if _, ok := part.(*sp.KVFeed); ok {
		xdcrf.logger.Debugf("Construct settings for KVFeed %s", part.Id())
		return xdcrf.constructSettingsForKVFeed(pipeline, part.(*sp.KVFeed), settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) constructSettingsForXmemNozzle(topic string, settings map[string]interface{}) (map[string]interface{}, error) {
	xmemSettings := make(map[string]interface{})
	// TODO this may break
	repSettings, err := metadata.SettingsFromMap(settings)
	if err != nil {
		return nil, err
	}
	xmemSettings[parts.XMEM_SETTING_BATCHCOUNT] = repSettings.BatchCount
	xmemSettings[parts.XMEM_SETTING_BATCHSIZE] = repSettings.BatchSize
	xmemSettings[parts.XMEM_SETTING_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(topic)
	xmemSettings[parts.XMEM_SETTING_BATCH_EXPIRATION_TIME] = time.Duration(float64(repSettings.MaxExpectedReplicationLag)*0.7) * time.Millisecond

	return xmemSettings, nil

}

func (xdcrf *XDCRFactory) getTargetTimeoutEstimate(topic string) time.Duration {
	//TODO: implement
	//need to get the tcp ping time for the estimate
	return 100 * time.Millisecond
}

func (xdcrf *XDCRFactory) constructSettingsForKVFeed(pipeline common.Pipeline, part *sp.KVFeed, settings map[string]interface{}) (map[string]interface{}, error) {
	xdcrf.logger.Debugf("Construct settings for KVFeed ....")
	kvFeedSettings := make(map[string]interface{})

	if pipeline.RuntimeContext().Service("CheckpointManager") == nil {
		return nil, errors.New("No checkpoint manager is registered with the pipeline")
	}

	svc := pipeline.RuntimeContext().Service("CheckpointManager").(*pipeline_svc.CheckpointManager)
	topic := pipeline.Topic()
	startSeqNums := svc.StartSequenceNum(topic)
	xdcrf.logger.Debugf("start sequence number is %v\n", startSeqNums)
	spec, err := xdcrf.metadata_svc.ReplicationSpec(topic)
	if err == nil {
		sourceBucketName := spec.SourceBucketName

		vbList := part.GetVBList()
		tsVb := protobuf.NewTsVbuuid(sourceBucketName, len(vbList))
		for _, vb := range vbList {
			tsVb.Append(vb, 0, 0, startSeqNums[int(vb)], 0)
		}
		kvFeedSettings["key"] = tsVb
	}
	return kvFeedSettings, err
}

func (xdcrf *XDCRFactory) registerServices(pipeline common.Pipeline, logger_ctx *log.LoggerContext) {
	ctx := pipeline.RuntimeContext()

	//TODO:
	//register pipeline supervisor
	supervisor := xdcr_pipeline_svc.NewPipelineSupervisor(logger_ctx, xdcrf.pipeline_failure_handler)
	ctx.RegisterService(base.PIPELINE_SUPERVISOR_SVC, supervisor)
	//register pipeline checkpoint manager
	ctx.RegisterService(base.CHECKPOINT_MGR_SVC, &xdcr_pipeline_svc.CheckpointManager{})
	//register pipeline statistics manager
}

func (xdcrf *XDCRFactory) ConstructSettingsForService(pipeline common.Pipeline, service common.PipelineService, settings map[string]interface{}) (map[string]interface{}, error) {
	if _, ok := service.(*pipeline_svc.PipelineSupervisor); ok {
		xdcrf.logger.Debug("Construct settings for PipelineSupervisor")
		return xdcrf.constructSettingsForSupervisor(pipeline.Topic(), settings)
	}
	return settings, nil
}

func (xdcrf *XDCRFactory) constructSettingsForSupervisor(topic string, settings map[string]interface{}) (map[string]interface{}, error) {
	s := make(map[string]interface{})
	repSettings, err := metadata.SettingsFromMap(settings)
	if err != nil {
		return nil, err
	}
	s[pipeline_svc.PIPELINE_LOG_LEVEL] = repSettings.LogLevel
	return s, nil
}
