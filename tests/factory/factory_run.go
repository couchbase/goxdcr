// unit test for xdcr pipeline factory.
package main

import (
	"flag"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	factory "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/factory"
	sp "github.com/ysui6888/indexing/secondary/projector"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/ysui6888/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
	"os"
	"errors"
)

var options struct {
	sourceBucket string // source bucket
	targetBucket string //target bucket
	connectStr    string //connect string
	numConnPerKV  int  // number of connections per source KV node
	numOutgoingConn  int  // number of connections to target cluster
	username      string //username
	password      string //password
	maxVbno       int    // maximum number of vbuckets
}

const (
	TEST_TOPIC = "test"
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)


func argParse() {

	flag.StringVar(&options.sourceBucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.targetBucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.IntVar(&options.numConnPerKV, "numConnPerKV", NUM_SOURCE_CONN,
		"number of connections per kv node")
	flag.IntVar(&options.numOutgoingConn, "numOutgoingConn", NUM_TARGET_CONN,
		"number of outgoing connections to target")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	options.connectStr = args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing ...")
	argParse()
	fmt.Printf("connectStr=%s\n", options.connectStr)
	fmt.Println("Done with parsing the arguments")
	err := invokeFactory()
	if err == nil {
		fmt.Println("Test passed.")
	} else {
		fmt.Println(err)
	}
}

func invokeFactory() error {
	var get_xdcr_config_callback factory.Get_XDCR_Config_Callback_Func = getXDCRConfig
	var get_source_kv_topology_callback factory.Get_Source_KV_Topology_Callback_Func = getSourceTopology
	var get_target_topology_callback factory.Get_Target_Topology_Callback_Func = getTargetTopology
	factory.SetCallBackFuncs(&get_xdcr_config_callback, &get_source_kv_topology_callback, &get_target_topology_callback);
	pl, err := factory.NewPipeline(TEST_TOPIC)
	if err != nil {
		return err
	}
	
	sources := pl.Sources()
	targets := pl.Targets()
	
	if len(sources) != NUM_SOURCE_CONN {
		return errors.New(fmt.Sprintf("incorrect source nozzles. expected %v; actual %v", NUM_SOURCE_CONN, len(sources)));
	}
	if len(targets) != NUM_TARGET_CONN {
		return errors.New(fmt.Sprintf("incorrect target nozzles. expected %v; actual %v", NUM_TARGET_CONN, len(targets)));
	}
	for sourceId, source := range sources {
		_, ok := source.(*sp.KVFeed)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for source nozzle %v.", sourceId));
		}
		
		// validate connector in source nozzles 
		connector := source.Connector()
		if connector == nil {
			return errors.New(fmt.Sprintf("no connector defined in source nozzle %v.", sourceId));
		}
		_, ok = connector.(*parts.Router)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect connector type in source nozzle %v.", sourceId));
		}
		downStreamParts := source.Connector().DownStreams()
		if len(downStreamParts) != NUM_TARGET_CONN {
			return errors.New(fmt.Sprintf("incorrect number of downstream parts for source nozzle %v. expected %v; actual %v", sourceId, NUM_TARGET_CONN, len(downStreamParts)));
		}
		for partId := range downStreamParts {
			if _, ok := targets[partId]; !ok {
				return errors.New(fmt.Sprintf("invalid downstream part %v for source nozzle %v.", partId, sourceId));
			}
		}
	}
	//validate that target nozzles do not have connectors
	for targetId, target := range targets {
		_, ok := target.(*parts.XmemNozzle)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for target nozzle %v.", targetId));
		}
		if target.Connector() != nil {
			return errors.New(fmt.Sprintf("target nozzle %v has connector, which is invalid.", targetId));
		}
	}
	
	return nil
}

// implementation of call back funcs required by xdcrFactory
func getXDCRConfig(topic string) (*base.XDCRConfig, error) {
	return &base.XDCRConfig {
	SourceCluster: options.connectStr,
	SourceBucketn: "default",
	NumSourceConn: NUM_SOURCE_CONN,
	TargetCluster: options.connectStr, 
	TargetBucketn: "default",
	NumTargetConn: NUM_TARGET_CONN,
	}, nil
}

func getSourceTopology(sourceCluster, sourceBucketn string) (string, *couchbase.Bucket, []uint16, error) {
	fmt.Printf("Getting source topo. sourceCluster=%v; sourceBucket=%v\n", sourceCluster, sourceBucketn)
	bucket, err := common.ConnectBucket(sourceCluster, "default", sourceBucketn)
	if err != nil {
		return "", nil, nil, err
	}
	
	// in test env, there should be only one kv in bucket server list
	kvaddr := bucket.VBServerMap().ServerList[0]
	
	m, err := bucket.GetVBmap([]string{kvaddr})
	if err != nil {
		return "", nil, nil, err
	}
	
	vbList := m[kvaddr]
	
	fmt.Printf("Returning target topo. kvaddr=%v; vbList=%v\n", kvaddr, vbList)
	
	return kvaddr, bucket, vbList, nil
}

func getTargetTopology(targetCluster, targetBucketn string) (map[string][]uint16, error) {
	bucket, err := common.ConnectBucket(targetCluster, "default", targetBucketn)
	if err != nil {
		return nil, err
	}
	
	return bucket.GetVBmap(bucket.VBServerMap().ServerList)
}

