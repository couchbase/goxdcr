// unit test for xdcr pipeline factory.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/parts"
	s "github.com/couchbase/goxdcr/service_impl"
    "github.com/couchbase/goxdcr/metadata"
    base "github.com/couchbase/goxdcr/base"
	ms "github.com/couchbase/goxdcr/mock_services"
	utils "github.com/couchbase/goxdcr/utils"
)

var options struct {
	sourceKVHost      string //source kv host name
	sourceKVAdminPort      uint64 //source kv admin port
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	connectStr      string //connect string
	username        string //username
	password        string //password
}

const (
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

func argParse() {
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.sourceBucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.targetBucket, "target_bucket", "target",
		"bucket to replicate to")
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
	top_svc, err := s.NewXDCRTopologySvc(options.username, options.password, uint16(options.sourceKVAdminPort), base.AdminportNumber, true, nil)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}
	
	options.sourceKVHost, err = top_svc.MyHost()
	if err != nil {
		fmt.Printf("Error getting current host. err=%v\n", err)
		os.Exit(1)
	}
	
	options.connectStr = utils.GetHostAddr(options.sourceKVHost, uint16(options.sourceKVAdminPort))
	
	ms.SetTestOptions(utils.GetHostAddr(options.sourceKVHost, uint16(options.sourceKVAdminPort)), options.username, options.password)
	
	msvc, err := s.DefaultMetadataSvc()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return err
	}
	
	replSpecSvc := s.NewReplicationSpecService(msvc, nil)

	fac := factory.NewXDCRFactory(replSpecSvc, &ms.MockClusterInfoSvc{}, top_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, nil)

	replSpec := metadata.NewReplicationSpecification(options.connectStr, options.sourceBucket, options.connectStr, options.targetBucket, "")
	replSpec.Settings.SourceNozzlePerNode = NUM_SOURCE_CONN
	replSpec.Settings.TargetNozzlePerNode = NUM_TARGET_CONN
	err = replSpecSvc.AddReplicationSpec(replSpec)
	if err != nil {
		return err
	}
	defer replSpecSvc.DelReplicationSpec(replSpec.Id)
	
	pl, err := fac.NewPipeline(replSpec.Id)
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
		_, ok := source.(*parts.DcpNozzle)
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

