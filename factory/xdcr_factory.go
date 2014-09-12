package factory

import (
	"strconv"
	"sync"
	"math"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	//connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	pctx "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_ctx"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	"github.com/couchbaselabs/go-couchbase"
	sp "github.com/ysui6888/indexing/secondary/projector"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
)

const (
	POOL_NAME = "default"
	PART_NAME_DELIMITER = "_"
	KVFEED_NAME_PREFIX = "kvfeed"
	XMEM_NOZZLE_NAME_PREFIX = "xmem"
)

// call back function for getting xdcr config
type Get_XDCR_Config_Callback_Func func(topic string) (*base.XDCRConfig, error)
// call back function for getting source kv node topology, i.e.,
// 1. kvaddr,  which will be used by kvfeed
// 2. Bucket, which will be used by kvfeed
// 3. a list of vbuckets owned by the source kv, which will be used for load balancing 
type Get_Source_KV_Topology_Callback_Func func(sourceCluster, sourceBucketn string) (string, *couchbase.Bucket, []uint16, error)
// call back function for getting target topology and bucket password if it is set, i.e., a map of kvaddr -> vbucket list
type Get_Target_Topology_Callback_Func func(targetCluster, targetBucketn string) (map[string][]uint16, string, error)

// Factory for XDCR pipelines
type xdcrFactory struct {
	get_xdcr_config_callback      *Get_XDCR_Config_Callback_Func
	get_source_kv_topology_callback      *Get_Source_KV_Topology_Callback_Func
	get_target_topology_callback      *Get_Target_Topology_Callback_Func
	once sync.Once
}

var xdcr_factory xdcrFactory

var logger_factory *log.CommonLogger = log.NewLogger("XDCRFactory", log.LogLevelInfo)

// set call back functions is done only once
func SetCallBackFuncs(get_xdcr_config_callback *Get_XDCR_Config_Callback_Func,
	get_source_kv_topology_callback *Get_Source_KV_Topology_Callback_Func,
	get_target_topology_callback *Get_Target_Topology_Callback_Func) {
	xdcr_factory.once.Do(func() {
		xdcr_factory.get_xdcr_config_callback = get_xdcr_config_callback
		xdcr_factory.get_source_kv_topology_callback = get_source_kv_topology_callback
		xdcr_factory.get_target_topology_callback = get_target_topology_callback
	})
}

func NewPipeline (topic string) (common.Pipeline, error) { 	
 	config, err := (*xdcr_factory.get_xdcr_config_callback)(topic)
 	logger_factory.Debugf("XDCR config = %v\n", config)
 	if err != nil {
		return nil, err
 	}
 	
 	// popuplate pipeline using config
 	sourceNozzles, err := xdcr_factory.constructSourceNozzles(config, topic); 
 	if err != nil {
		return nil, err
 	}
 	
 	outNozzles, vbNozzleMap, err := xdcr_factory.constructOutgoingNozzles(config); 
 	if err != nil {
		return nil, err
 	}
 	
 	// TODO construct queue parts. This will affect vbMap in router. may need an additional outNozzle -> downStreamPart/queue map in constructRouter
 	
 	downStreamParts := make(map[string]common.Part)
	for partId, outNozzle := range outNozzles {
		downStreamParts[partId] = outNozzle
	}	
 	
 	router, err := xdcr_factory.constructRouter(config, downStreamParts, vbNozzleMap); 
 	if err != nil {
		return nil, err
 	}
 	
 	// connect parts
 	for _, sourceNozzle := range sourceNozzles {
 		sourceNozzle.SetConnector(router)
 	}
 	
 	// construct pipeline
	pipeline := pp.NewGenericPipeline(topic, sourceNozzles, outNozzles)
	if pipelineContext, err := pctx.New(pipeline); err != nil {
		return nil, err
	} else {
 		pipeline.SetRuntimeContext(pipelineContext)
 	}
 	
 	logger_factory.Infof("XDCR pipeline constructed")
	return pipeline, nil
}

// construct source nozzles for the requested/current kv node
// question: should kvaddr be passed to factory, or can factory figure it out by itself through some GetCurrentNode API?
func (xdcrf *xdcrFactory) constructSourceNozzles(config *base.XDCRConfig, topic string) (map[string]common.Nozzle, error) {
	sourceNozzles := make(map[string]common.Nozzle)
	
	kvaddr, bucket, vbnos, err := (*xdcr_factory.get_source_kv_topology_callback)(config.SourceCluster, config.SourceBucketn)
	logger_factory.Debugf("Source topology retrieved. kvaddr = %v; vbnos = %v\n", kvaddr, vbnos)
	if err != nil {
		return nil, err
	}
	
	numOfVbs := len(vbnos)
	
	// the number of kvfeed nodes to construct is the smaller of vbucket list size and source connection size 
	numOfKVFeeds := int(math.Min(float64(numOfVbs), float64(config.NumSourceConn)))
	
	numOfVbPerKVFeed := int(math.Ceil(float64(numOfVbs)/float64(numOfKVFeeds))) 
		
	var index int
	for i := 0; i < numOfKVFeeds; i++ {
		// construct vbList for the kvfeed
		// before statistics info is available, the default load balancing stragegy is to evenly distribute vbuckets among kvfeeds 
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
		kvfeed , err := sp.NewKVFeed(kvaddr, topic, KVFEED_NAME_PREFIX + PART_NAME_DELIMITER + strconv.Itoa(i), bucket, vbList)
		if err != nil {
			return nil, err
		}
		sourceNozzles[kvfeed.Id()] = kvfeed
		logger_factory.Debugf("Constructed source nozzle %v with vbList = %v \n", kvfeed.Id(), vbList)
	}
	
	logger_factory.Infof("Constructed %v source nozzles\n", len(sourceNozzles))
	
	return sourceNozzles, nil
}

// TODO figure out whether to construct CAPI or XMEM type
func (xdcrf *xdcrFactory) constructOutgoingNozzles(config *base.XDCRConfig) (map[string]common.Nozzle, map[uint16]string, error) {
	outNozzles := make(map[string]common.Nozzle)
	vbNozzleMap := make(map[uint16]string)
	
	kvVBMap, bucketPwd, err := (*xdcr_factory.get_target_topology_callback)(config.TargetCluster, config.TargetBucketn)
	logger_factory.Debugf("Target topology retrived. kvVBMap = %v\n", kvVBMap)
	if err != nil {
		return nil, nil, err
	}
	
	for kvaddr, kvVBList := range kvVBMap {
		numOfVbs := len(kvVBList)
		// the nuhmber of xmem nozzles to construct is the smaller of vbucket list size and target connection size 
		numOfNozzles := int(math.Min(float64(numOfVbs), float64(config.NumTargetConn)))
		
		numOfVbPerNozzle := int(math.Ceil(float64(numOfVbs)/float64(numOfNozzles))) 
		
		var index int = 0
		for i := 0; i < numOfNozzles; i++ {
		
			// construct xmem nozzle
	    	// partIds of the xmem nozzles look like "xmem_$kvaddr_1"
			outNozzle := parts.NewXmemNozzle(XMEM_NOZZLE_NAME_PREFIX + PART_NAME_DELIMITER + kvaddr + PART_NAME_DELIMITER + strconv.Itoa(i), kvaddr, config.TargetBucketn, bucketPwd)
			outNozzles[outNozzle.Id()] = outNozzle
			
			// TODO pass kvaddr and other info to xmem once setters are exposed
			
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

func (xdcrf *xdcrFactory) constructRouter(config *base.XDCRConfig, downStreamParts map[string]common.Part, vbNozzleMap map[uint16]string) (*parts.Router, error) {
	router, err := parts.NewRouter(downStreamParts, vbNozzleMap)
	logger_factory.Infof("Constructed router")
	return router, err
}

