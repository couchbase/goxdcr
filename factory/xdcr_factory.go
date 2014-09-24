package factory

import (
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/couchbase/indexing/secondary/protobuf"
	"math"
	"strconv"
	//connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	"errors"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	pctx "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_ctx"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata_svc"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/pipeline_svc"
	xdcr_pipeline_svc "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/pipeline_svc"
	sp "github.com/ysui6888/indexing/secondary/projector"
	"time"
)

const (
	PART_NAME_DELIMITER     = "_"
	KVFEED_NAME_PREFIX      = "kvfeed"
	XMEM_NOZZLE_NAME_PREFIX = "xmem"
)

// Factory for XDCR pipelines
type XDCRFactory struct {
	metadata_svc      metadata_svc.MetadataSvc
	cluster_info_svc  metadata_svc.ClusterInfoSvc
	xdcr_topology_svc metadata_svc.XDCRCompTopologySvc
}

var logger_factory *log.CommonLogger = log.NewLogger("XDCRFactory", log.LogLevelDebug)

// set call back functions is done only once
func NewXDCRFactory(metadata_svc metadata_svc.MetadataSvc, cluster_info_svc metadata_svc.ClusterInfoSvc, xdcr_topology_svc metadata_svc.XDCRCompTopologySvc) *XDCRFactory {
	return &XDCRFactory{metadata_svc: metadata_svc,
		cluster_info_svc:  cluster_info_svc,
		xdcr_topology_svc: xdcr_topology_svc}
}

func (xdcrf *XDCRFactory) NewPipeline(topic string) (common.Pipeline, error) {
	spec, err := xdcrf.metadata_svc.ReplicationSpec(topic)
	logger_factory.Debugf("replication specification = %v\n", spec)
	if err != nil {
		return nil, err
	}

	// popuplate pipeline using config
	sourceNozzles, err := xdcrf.constructSourceNozzles(spec, topic)
	if err != nil {
		return nil, err
	}

	outNozzles, vbNozzleMap, err := xdcrf.constructOutgoingNozzles(spec)
	if err != nil {
		return nil, err
	}

	// TODO construct queue parts. This will affect vbMap in router. may need an additional outNozzle -> downStreamPart/queue map in constructRouter
	downStreamParts := make(map[string]common.Part)
	for partId, outNozzle := range outNozzles {
		downStreamParts[partId] = outNozzle
	}

	router, err := xdcrf.constructRouter(spec, downStreamParts, vbNozzleMap)
	if err != nil {
		return nil, err
	}

	// connect parts
	for _, sourceNozzle := range sourceNozzles {
		sourceNozzle.SetConnector(router)
	}

	// construct pipeline
	pipeline := pp.NewPipelineWithSettingConstructor(topic, sourceNozzles, outNozzles, xdcrf.ConstructSettingsForPart)
	if pipelineContext, err := pctx.New(pipeline); err != nil {
		return nil, err
	} else {

		//register services
		pipeline.SetRuntimeContext(pipelineContext)
		xdcrf.registerServices(pipeline)
	}

	logger_factory.Infof("XDCR pipeline constructed")
	return pipeline, nil
}

// construct source nozzles for the requested/current kv node
// question: should kvaddr be passed to factory, or can factory figure it out by itself through some GetCurrentNode API?
func (xdcrf *XDCRFactory) constructSourceNozzles(spec *metadata.ReplicationSpecification, topic string) (map[string]common.Nozzle, error) {
	sourceNozzles := make(map[string]common.Nozzle)

	//	kvaddr, bucket, vbnos, err := (*xdcr_factory.get_source_kv_topology_callback)(config.SourceCluster, config.SourceBucketn)
	kvaddrs, err := xdcrf.xdcr_topology_svc.MyKVNodes()
	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return nil, err
	}
	logger_factory.Debugf("kvaddrs=%v\n", kvaddrs)

	bucketName, err := spec.SourceBucketName()
	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return nil, err
	}

	sourceClusterUUID, err := spec.SourceClusterUUID()

	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return nil, err
	}
	maxNozzlesPerNode := spec.Settings().SourceNozzlesPerNode()

	serverVBMap, err := xdcrf.cluster_info_svc.GetServerVBucketsMap(sourceClusterUUID, bucketName)
	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return nil, err
	}
	for _, kvaddr := range kvaddrs {
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
				logger_factory.Errorf("Error getting bucket. i=%d, err=%v\n", i, err)
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
			// partIds of the kvfeed nodes look like "kvfeed_1"
			kvfeed, err := sp.NewKVFeed(kvaddr, topic, KVFEED_NAME_PREFIX+PART_NAME_DELIMITER+strconv.Itoa(i), bucket, vbList, i)
			if err != nil {
				logger_factory.Errorf("Error on NewKVFeed. i=%d, err=%v\n", i, err)
				return nil, err
			}
			sourceNozzles[kvfeed.Id()] = kvfeed
			logger_factory.Debugf("Constructed source nozzle %v with vbList = %v \n", kvfeed.Id(), vbList)
		}
	}
	logger_factory.Infof("Constructed %v source nozzles\n", len(sourceNozzles))

	return sourceNozzles, nil
}

func (xdcrf *XDCRFactory) constructOutgoingNozzles(spec *metadata.ReplicationSpecification) (map[string]common.Nozzle, map[uint16]string, error) {
	outNozzles := make(map[string]common.Nozzle)
	vbNozzleMap := make(map[uint16]string)

	//	kvVBMap, bucketPwd, err := (*xdcr_factory.get_target_topology_callback)(config.TargetCluster, config.TargetBucketn)
	targetClusterUUID, err := spec.TargetClusterUUID()
	if err != nil {
		logger_factory.Errorf("Error getting target cluster uuid, err=%v\n", err)
		return nil, nil, err
	}
	targetBucketName, err := spec.TargetBucketName()
	if err != nil {
		logger_factory.Errorf("Error getting bucket name, err=%v\n", err)
		return nil, nil, err
	}

	kvVBMap, err := xdcrf.cluster_info_svc.GetServerVBucketsMap(targetClusterUUID, targetBucketName)
	if err != nil {
		logger_factory.Errorf("Error getting server vbuckets map, err=%v\n", err)
		return nil, nil, err
	}

	targetBucket, err := xdcrf.cluster_info_svc.GetBucket(targetClusterUUID, targetBucketName)
	if err != nil {
		logger_factory.Errorf("Error getting bucket, err=%v\n", err)
		return nil, nil, err
	}

	bucketPwd := targetBucket.Password
	maxTargetNozzlePerNode := spec.Settings().TargetNozzlesPerNode()
	logger_factory.Debugf("Target topology retrived. kvVBMap = %v\n", kvVBMap)

	for kvaddr, kvVBList := range kvVBMap {
		numOfVbs := len(kvVBList)
		// the nuhmber of xmem nozzles to construct is the smaller of vbucket list size and target connection size
		numOfNozzles := int(math.Min(float64(numOfVbs), float64(maxTargetNozzlePerNode)))

		numOfVbPerNozzle := int(math.Ceil(float64(numOfVbs) / float64(numOfNozzles)))
		logger_factory.Debugf("maxTargetNozzlePerNode=%d\n", maxTargetNozzlePerNode)
		logger_factory.Debugf("Constructing %d nozzles, each is responsible for %d vbuckets\n", numOfNozzles, numOfVbPerNozzle)
		var index int = 0
		for i := 0; i < numOfNozzles; i++ {

			// construct xmem nozzle
			// partIds of the xmem nozzles look like "xmem_$kvaddr_1"
			outNozzle, err := xdcrf.constructNozzleForTargetNode(kvaddr, targetBucketName, bucketPwd, i)

			if err != nil {
				logger_factory.Errorf("err=%v\n", err)
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

			logger_factory.Debugf("Constructed out nozzle %v\n", outNozzle.Id())
		}
	}

	logger_factory.Infof("Constructed %v outgoing nozzles\n", len(outNozzles))
	logger_factory.Debugf("vbNozzleMap = %v\n", vbNozzleMap)
	return outNozzles, vbNozzleMap, nil
}

func (xdcrf *XDCRFactory) constructRouter(spec *metadata.ReplicationSpecification, downStreamParts map[string]common.Part, vbNozzleMap map[uint16]string) (*parts.Router, error) {
	router, err := parts.NewRouter(downStreamParts, vbNozzleMap)
	logger_factory.Infof("Constructed router")
	return router, err
}

func (xdcrf *XDCRFactory) constructNozzleForTargetNode(kvaddr string, bucketName string, bucketPwd string, nozzle_index int) (common.Nozzle, error) {
	var nozzle common.Nozzle
	nozzleType, err := xdcrf.getNozzleType(kvaddr)

	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return nil, err
	}

	switch nozzleType {
	case base.XMEM:
		nozzle = xdcrf.constructXMEMNozzle(kvaddr, bucketName, bucketPwd, nozzle_index)
	case base.CAPI:
		nozzle = xdcrf.constructCAPINozzle(kvaddr, bucketName, bucketPwd, nozzle_index)
	}

	return nozzle, err

}

func (xdcrf *XDCRFactory) getNozzleType(kvaddr string) (base.XDCROutgoingNozzleType, error) {
	beforeXMEM, err := xdcrf.cluster_info_svc.IsNodeCompatible(kvaddr, "2.5")
	if err != nil {
		logger_factory.Errorf("err=%v\n", err)
		return -1, err
	}

	if beforeXMEM {
		return base.XMEM, nil
	} else {
		return base.CAPI, nil
	}
}

func (xdcrf *XDCRFactory) constructXMEMNozzle(kvaddr string, bucketName string, bucketPwd string, nozzle_index int) common.Nozzle {
	xmemNozzle_Id := XMEM_NOZZLE_NAME_PREFIX + PART_NAME_DELIMITER + kvaddr + PART_NAME_DELIMITER + strconv.Itoa(nozzle_index)
	nozzle := parts.NewXmemNozzle(xmemNozzle_Id, kvaddr, bucketName, bucketPwd)
	return nozzle
}

func (xdcrf *XDCRFactory) constructCAPINozzle(kvaddr string, bucketName string, bucketPwd string, nozzle_index int) common.Nozzle {
	//TODO: implement it
	return nil
}

func (xdcrf *XDCRFactory) ConstructSettingsForPart(pipeline common.Pipeline, part common.Part, settings map[string]interface{}) (map[string]interface{}, error) {

	if _, ok := part.(*parts.XmemNozzle); ok {
		logger_factory.Debugf("Construct settings for XmemNozzle %s", part.Id())
		return xdcrf.constructSettingsForXmemNozzle(pipeline.Topic(), settings)
	} else if _, ok := part.(*sp.KVFeed); ok {
		logger_factory.Debugf("Construct settings for KVFeed %s", part.Id())
		return xdcrf.constructSettingsForKVFeed(pipeline, part.(*sp.KVFeed), settings)
	} else {
		return settings, nil
	}
}

func (xdcrf *XDCRFactory) constructSettingsForXmemNozzle(topic string, settings map[string]interface{}) (map[string]interface{}, error) {
	xmemSettings := make(map[string]interface{})
	repSettings := metadata.SettingsFromMap(settings)
	xmemSettings[parts.XMEM_SETTING_BATCHCOUNT] = repSettings.BatchCount()
	xmemSettings[parts.XMEM_SETTING_BATCHSIZE] = repSettings.BatchSize()
	xmemSettings[parts.XMEM_SETTING_TIMEOUT] = xdcrf.getTargetTimeoutEstimate(topic)

	return xmemSettings, nil

}

func (xdcrf *XDCRFactory) getTargetTimeoutEstimate(topic string) time.Duration {
	//TODO: implement
	//need to get the tcp ping time for the estimate
	return 100 * time.Millisecond
}

func (xdcrf *XDCRFactory) constructSettingsForKVFeed(pipeline common.Pipeline, part *sp.KVFeed, settings map[string]interface{}) (map[string]interface{}, error) {
	logger_factory.Debugf("Construct settings for KVFeed ....")
	kvFeedSettings := make(map[string]interface{})

	if pipeline.RuntimeContext().Service("CheckpointManager") == nil {
		return nil, errors.New("No checkpoint manager is registered with the pipeline")
	}

	svc := pipeline.RuntimeContext().Service("CheckpointManager").(*pipeline_svc.CheckpointManager)
	topic := pipeline.Topic()
	startSeqNums := svc.StartSequenceNum(topic)
	logger_factory.Debugf("start sequence number is %v\n", startSeqNums)
	spec, err := xdcrf.metadata_svc.ReplicationSpec(topic)
	if err == nil {
		sourceBucketName, err := spec.SourceBucketName()
		if err != nil {
			return kvFeedSettings, nil
		}

		vbList := part.GetVBList()
		tsVb := protobuf.NewTsVbuuid(sourceBucketName, len(vbList))
		for _, vb := range vbList {
			tsVb.Append(vb, 0, 0, startSeqNums[int(vb)], 0)
		}
		kvFeedSettings["key"] = tsVb
	}
	return kvFeedSettings, err
}

func (xdcrf *XDCRFactory) registerServices(pipeline common.Pipeline) {
	ctx := pipeline.RuntimeContext()

	//TODO:
	//register pipeline supervisor
	ctx.RegisterService("PipelineSupervisor", &xdcr_pipeline_svc.PipelineSupervisor{})
	//register pipeline checkpoint manager
	ctx.RegisterService("CheckpointManager", &xdcr_pipeline_svc.CheckpointManager{})
	//register pipeline statistics manager
}
