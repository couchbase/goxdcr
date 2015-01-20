// unit test for xdcr pipeline factory.
package main

import (
	"errors"
	"flag"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/factory"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/tests/common"
	"os"
)

var options struct {
	sourceKVHost      string //source kv host name
	sourceKVAdminPort uint64 //source kv admin port
	sourceBucket      string // source bucket
	targetBucket      string //target bucket

	// parameters of remote cluster
	remoteUuid             string // remote cluster uuid
	remoteName             string // remote cluster name
	remoteHostName         string // remote cluster host name
	remoteUserName         string //remote cluster userName
	remotePassword         string //remote cluster password
	remoteDemandEncryption bool   // whether encryption is needed
	remoteCertificateFile  string // file containing certificate for encryption
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

	flag.StringVar(&options.remoteUuid, "remoteUuid", "1234567",
		"remote cluster uuid")
	flag.StringVar(&options.remoteName, "remoteName", "remote",
		"remote cluster name")
	flag.StringVar(&options.remoteHostName, "remoteHostName", "127.0.0.1:9000",
		"remote cluster host name")
	flag.StringVar(&options.remoteUserName, "remoteUserName", "Administrator", "remote cluster userName")
	flag.StringVar(&options.remotePassword, "remotePassword", "welcome", "remote cluster password")
	flag.BoolVar(&options.remoteDemandEncryption, "remoteDemandEncryption", false, "whether encryption is needed")
	flag.StringVar(&options.remoteCertificateFile, "remoteCertificateFile", "", "file containing certificate for encryption")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing ...")
	argParse()
	fmt.Println("Done with parsing the arguments")
	err := invokeFactory()
	if err == nil {
		fmt.Println("Test passed.")
	} else {
		fmt.Println(err)
	}
}

func invokeFactory() error {
	top_svc, err := s.NewXDCRTopologySvc(uint16(options.sourceKVAdminPort), base.AdminportNumber, true, nil)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}

	options.sourceKVHost, err = top_svc.MyHost()
	if err != nil {
		fmt.Printf("Error getting current host. err=%v\n", err)
		os.Exit(1)
	}

	msvc, err := s.NewMetaKVMetadataSvc(nil)
	if err != nil {
		fmt.Printf("Error creating metadata service. err=%v\n", err)
		os.Exit(1)
	}
	
	audit_svc, err := s.NewAuditSvc(top_svc, nil)
	if err != nil {
		fmt.Printf("Error starting audit service. err=%v\n", err)
		os.Exit(1)
	}

	repl_spec_svc := s.NewReplicationSpecService(msvc, nil)
	remote_cluster_svc := s.NewRemoteClusterService(msvc, nil)
	cluster_info_svc := s.NewClusterInfoSvc(nil)
	checkpoints_svc := s.NewCheckpointsService(msvc, nil)
	capi_svc := s.NewCAPIService(nil)

	replication_manager.StartReplicationManager(options.sourceKVHost, base.AdminportNumber,
		repl_spec_svc,
		remote_cluster_svc,
		cluster_info_svc, top_svc, s.NewReplicationSettingsSvc(msvc, nil), checkpoints_svc, capi_svc, audit_svc)

	fac := factory.NewXDCRFactory(repl_spec_svc, remote_cluster_svc, cluster_info_svc, top_svc, checkpoints_svc, capi_svc, log.DefaultLoggerContext, log.DefaultLoggerContext, nil, nil)

	// create remote cluster reference needed by replication
	err = common.CreateTestRemoteCluster(remote_cluster_svc, options.remoteUuid, options.remoteName, options.remoteHostName, options.remoteUserName, options.remotePassword,
		options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	defer common.DeleteTestRemoteCluster(remote_cluster_svc, options.remoteName)

	remoteClusterRef, err := remote_cluster_svc.RemoteClusterByRefName(options.remoteName)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	replSpec := metadata.NewReplicationSpecification(options.sourceBucket, remoteClusterRef.Uuid, options.targetBucket)
	replSpec.Settings.SourceNozzlePerNode = NUM_SOURCE_CONN
	replSpec.Settings.TargetNozzlePerNode = NUM_TARGET_CONN
	err = repl_spec_svc.AddReplicationSpec(replSpec)
	if err != nil {
		return err
	}
	defer repl_spec_svc.DelReplicationSpec(replSpec.Id)

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
