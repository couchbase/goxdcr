// unit test for xdcr pipeline factory.
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/factory"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	sp "github.com/ysui6888/indexing/secondary/projector"
	"os"
	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/mock_services"
)

var options struct {
	sourceKVHost      string //source kv host name
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	connectStr      string //connect string
	numConnPerKV    int    // number of connections per source KV node
	numOutgoingConn int    // number of connections to target cluster
	username        string //username
	password        string //password
	maxVbno         int    // maximum number of vbuckets
}

const (
	TEST_TOPIC      = "test"
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

func argParse() {
	flag.StringVar(&options.connectStr, "connectStr", "127.0.0.1:9000",
		"connection string to source cluster")
	flag.StringVar(&options.sourceKVHost, "source_kv_host", "127.0.0.1",
		"source KV host name")
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
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
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
	msvc := c.NewMockMetadataSvc()
	mcisvc := &c.MockClusterInfoSvc{}
	mxtsvc := &c.MockXDCRTopologySvc{}

	c.SetTestOptions(options.sourceBucket, options.targetBucket, options.connectStr, options.connectStr, options.sourceKVHost, options.username, options.password, options.numConnPerKV, options.numOutgoingConn)
	fac := factory.NewXDCRFactory(msvc, mcisvc, mxtsvc, log.DefaultLoggerContext, log.DefaultLoggerContext, nil)

	pl, err := fac.NewPipeline(TEST_TOPIC)
	if err != nil {
		return err
	}

	sources := pl.Sources()
	targets := pl.Targets()

	if len(sources) != NUM_SOURCE_CONN {
		return errors.New(fmt.Sprintf("incorrect source nozzles. expected %v; actual %v", NUM_SOURCE_CONN, len(sources)))
	}
	if len(targets) != NUM_TARGET_CONN {
		return errors.New(fmt.Sprintf("incorrect target nozzles. expected %v; actual %v", NUM_TARGET_CONN, len(targets)))
	}
	for sourceId, source := range sources {
		_, ok := source.(*sp.KVFeed)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for source nozzle %v.", sourceId))
		}

		// validate connector in source nozzles
		connector := source.Connector()
		if connector == nil {
			return errors.New(fmt.Sprintf("no connector defined in source nozzle %v.", sourceId))
		}
		_, ok = connector.(*parts.Router)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect connector type in source nozzle %v.", sourceId))
		}
		downStreamParts := source.Connector().DownStreams()
		if len(downStreamParts) != NUM_TARGET_CONN {
			return errors.New(fmt.Sprintf("incorrect number of downstream parts for source nozzle %v. expected %v; actual %v", sourceId, NUM_TARGET_CONN, len(downStreamParts)))
		}
		for partId := range downStreamParts {
			if _, ok := targets[partId]; !ok {
				return errors.New(fmt.Sprintf("invalid downstream part %v for source nozzle %v.", partId, sourceId))
			}
		}
	}
	//validate that target nozzles do not have connectors
	for targetId, target := range targets {
		_, ok := target.(*parts.XmemNozzle)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for target nozzle %v.", targetId))
		}
		if target.Connector() != nil {
			return errors.New(fmt.Sprintf("target nozzle %v has connector, which is invalid.", targetId))
		}
	}

	return nil
}

