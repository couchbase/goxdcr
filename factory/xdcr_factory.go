package factory

import (
	"strconv"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	//connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	pp "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline"
	pctx "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_ctx"
	parts "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	sc "github.com/ysui6888/indexing/secondary/common"
	sp "github.com/ysui6888/indexing/secondary/projector"
	//log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
)

const (
	POOL_NAME = "default"
	PART_NAME_DELIMITER = "_"
	XMEM_NOZZLE_NAME_PREFIX = "xmem"
)

// Factory for XDCR pipelines

type xdcrFactory struct {
}

// configuration of a particular replication, i.e., pipeline
type xdcrConfig struct {
	sourceCluster string  // source cluster address
	sourceBucketn string   // source bucket name
	numConnPerKV  int  // number of connections per source KV node
	targetCluster string  // target cluster address
	targetBucketn string   // target bucket name
	numOutgoingConn  int  // number of connections to target cluster
	
}

var xdcr_factory xdcrFactory

func NewPipeline (topic string) (common.Pipeline, error) { 	
 	config := xdcr_factory.getConfig(topic)
 	
 	// popuplate pipeline using config
 	sourceNozzles, err := xdcr_factory.constructSourceNozzles(config, topic); 
 	if err != nil {
		return nil, err
 	}
 	
 	outNozzles := xdcr_factory.constructOutgoingNozzles(config); 
 	
 	router, err := xdcr_factory.constructRouter(config, outNozzles); 
 	if err != nil {
		return nil, err
 	}
 	
 	// connect parts
 	for _, sourceNozzle := range sourceNozzles {
 		sourceNozzle.SetConnector(router)
 	}
 	
 	// TODO construct queue parts
 	
 	// construct pipeline
	pipeline := pp.NewGenericPipeline(topic, sourceNozzles, outNozzles)
	if pipelineContext, err := pctx.New(pipeline); err != nil {
		return nil, err
	} else {
 		pipeline.SetRuntimeContext(pipelineContext)
 	}
 	
	return pipeline, nil
}

func (xdcrf *xdcrFactory) getConfig(topic string) *xdcrConfig {
	// TODO get replication config for a particular topic from metadata store
	
	// for testing. it is ugly but did not find viable alternatives to pass these parameters in from test env without metadata store
	return &xdcrConfig {
	sourceCluster: "localhost:9000",
	sourceBucketn: "default",
	numConnPerKV: 2,
	targetCluster: "" , 
	targetBucketn: "",
	numOutgoingConn: 3,
	}
}

func (xdcrf *xdcrFactory) constructSourceNozzles(config *xdcrConfig, topic string) (map[string]common.Nozzle, error) {
	sourceNozzles := make(map[string]common.Nozzle)
	
	bucket, err := sc.ConnectBucket(config.sourceCluster, POOL_NAME, config.sourceBucketn)
	if err != nil {
		return nil, err
	}
	
	for _, kvaddr := range bucket.VBServerMap().ServerList {
		// for each kv node, construct maxConnPerKVNode KVFeed nozzles
		for i := 0; i < config.numConnPerKV; i++ {
			kvfeed , err := sp.NewKVFeed(kvaddr, topic, kvaddr + PART_NAME_DELIMITER + strconv.Itoa(i), bucket)
			if err != nil {
				return nil, err
			}
			sourceNozzles[kvfeed.Id()] = kvfeed
		}
	}
	
	return sourceNozzles, nil
}

func (xdcrf *xdcrFactory) constructOutgoingNozzles(config *xdcrConfig) (map[string]common.Nozzle) {
	outNozzles := make(map[string]common.Nozzle)
	
	for i := 0; i < config.numOutgoingConn; i++ {
			xmem := parts.NewXmemNozzle(XMEM_NOZZLE_NAME_PREFIX + PART_NAME_DELIMITER + strconv.Itoa(i))
			outNozzles[xmem.Id()] = xmem
		}
		
	return outNozzles
}

func (xdcrf *xdcrFactory) constructRouter(config *xdcrConfig, outNozzles map[string]common.Nozzle) (*parts.Router, error) {
	downStreamParts := make(map[string]common.Part)
	for partId, nozzle := range outNozzles {
		downStreamParts[partId] = nozzle
	}	
	if router, err := parts.NewRouter(downStreamParts, nil/*vbMap*/); err == nil {
		return router, nil
	} else {
		return nil, err
	}
}