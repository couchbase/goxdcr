// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package main

//go:generate go run ../generate.go GlobalStatsTable

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	base "github.com/couchbase/goxdcr/base"
	log "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/peerToPeer"
	rm "github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/streamApiWatcher"
	utilities "github.com/couchbase/goxdcr/utils"
)

var done = make(chan bool)

var options struct {
	sourceKVAdminPort uint64 //source kv admin port
	xdcrRestPort      uint64 // port number of XDCR rest server

	sslProxyUpstreamPort uint64 // gometa request port
	isEnterprise         bool   // whether couchbase is of enterprise edition
	ipv4                 string // required/optional/off
	ipv6                 string // required/optional/off
	isConvert            bool   // whether xdcr is running in conversion/upgrade mode
	caFileLocation       string
	clientCertFile       string
	clientKeyFile        string

	// logging related parameters
	logFileDir          string
	maxLogFileSize      uint64
	maxNumberOfLogFiles uint64
}

var max_retry_wait_for_metadata_service = 30
var retry_interval_wait_for_metadata_service = time.Second

func argParse() {
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.Uint64Var(&options.xdcrRestPort, "xdcrRestPort", uint64(base.AdminportNumber),
		"port number of XDCR rest server")
	flag.Uint64Var(&options.sslProxyUpstreamPort, "localProxyPort", 0,
		"port number for ssl proxy upstream port")
	flag.BoolVar(&options.isEnterprise, "isEnterprise", true,
		"whether couchbase is of enterprise edition")
	flag.StringVar(&options.ipv4, "ipv4", "required",
		"whether ipv4 is required/optional/off")
	flag.StringVar(&options.ipv6, "ipv6", "optional",
		"whether ipv6 is required/optional/off")

	flag.BoolVar(&options.isConvert, "isConvert", false,
		"whether xdcr is running in convertion/upgrade mode")

	flag.StringVar(&options.logFileDir, "logFileDir", "",
		"directory for couchbase server logs")
	flag.Uint64Var(&options.maxLogFileSize, "maxLogFileSize", 40*1024*1024,
		"maximum log file size")
	flag.Uint64Var(&options.maxNumberOfLogFiles, "maxNumberOfLogFiles", 5,
		"maximum number of log files")

	flag.StringVar(&options.caFileLocation, "caFile", "",
		"location of the cluster CA file")
	flag.StringVar(&options.clientCertFile, "clientCertFile", "",
		"Internal communication certificate file")
	flag.StringVar(&options.clientKeyFile, "clientKeyFile", "",
		"Internal communication key file")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func validateIpFamilyOptions(ipv4, ipv6 string) error {
	// This is for backward compatibility
	if ipv6 == "true" {
		ipv4 = base.IpFamilyOptionalOption
		ipv6 = base.IpFamilyRequiredOption
	} else if ipv6 == "false" {
		ipv4 = base.IpFamilyRequiredOption
		ipv6 = base.IpFamilyOptionalOption
	}
	// New IpFamily options
	if ipv4 != base.IpFamilyRequiredOption && ipv6 != base.IpFamilyRequiredOption {
		return fmt.Errorf("Invalid options ipv4=%v,ipv6=%v. At least one should be %v", ipv4, ipv6, base.IpFamilyRequiredOption)
	}
	if ipv4 == base.IpFamilyRequiredOption && ipv6 == base.IpFamilyRequiredOption {
		return fmt.Errorf("Invalid options ipv4=%v,ipv6=%v. Only one can be %v", ipv4, ipv6, base.IpFamilyRequiredOption)
	}
	if ipv4 != base.IpFamilyRequiredOption && ipv4 != base.IpFamilyOptionalOption && ipv4 != base.IpFamilyOffOption {
		return fmt.Errorf("Invalid option ipv4=%v", ipv4)
	}
	if ipv6 != base.IpFamilyRequiredOption && ipv6 != base.IpFamilyOptionalOption && ipv6 != base.IpFamilyOffOption {
		return fmt.Errorf("Invalid option ipv6=%v", ipv6)
	}
	return nil
}

func main() {
	HideConsole(true)
	defer HideConsole(false)

	runtime.GOMAXPROCS(rm.GoMaxProcs_env())

	argParse()

	if err := validateIpFamilyOptions(options.ipv4, options.ipv6); err != nil {
		fmt.Printf("Invalid options ipv4=%v,ipv6=%\n", options.ipv4, options.ipv6)
		os.Exit(1)
	}

	// initializes logger
	if options.logFileDir != "" {
		log.Init(options.logFileDir, options.maxLogFileSize, options.maxNumberOfLogFiles)
	}

	// Initializes official utility object to be used throughout
	utils := utilities.NewUtilities()

	// This needs to be started immediately since some of the constructors will start to query the security setting
	securitySvc := service_impl.NewSecurityService(options.caFileLocation, nil)
	securitySvc.Start()

	top_svc, err := service_impl.NewXDCRTopologySvc(uint16(options.sourceKVAdminPort), uint16(options.xdcrRestPort), options.isEnterprise, options.ipv4, options.ipv6, securitySvc, nil, utils)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}

	host := top_svc.GetLocalHostName()

	metakv_svc, err := metadata_svc.NewMetaKVMetadataSvc(nil, utils, false /*readOnly*/)
	if err != nil {
		fmt.Printf("Error starting metadata service. err=%v\n", err)
		os.Exit(1)
	}

	err = waitForMetadataService(metakv_svc)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	internalSettings_svc := metadata_svc.NewInternalSettingsSvc(metakv_svc, nil)

	rm.InitConstants(top_svc, internalSettings_svc)

	audit_svc, err := service_impl.NewAuditSvc(top_svc, nil, utils)
	if err != nil {
		fmt.Printf("Error starting audit service. err=%v\n", err)
		os.Exit(1)
	}

	processSetting_svc := metadata_svc.NewGlobalSettingsSvc(metakv_svc, nil)

	if options.isConvert {
		// disable uilogging during upgrade by specifying a nil uilog service
		remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(nil, metakv_svc, top_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting remote cluster service. err=%v\n", err)
			os.Exit(1)
		}
		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(nil, remote_cluster_svc, metakv_svc, top_svc, nil, nil, utils, nil)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}

		ckpt_svc, err := metadata_svc.NewCheckpointsService(metakv_svc, nil, utils, replication_spec_svc)
		if err != nil {
			fmt.Printf("Error starting checkpoints service. err=%v\n", err)
			os.Exit(1)
		}

		migration_svc := service_impl.NewMigrationSvc(top_svc, remote_cluster_svc,
			replication_spec_svc,
			metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil, top_svc),
			ckpt_svc, nil, utils)
		err = migration_svc.Migrate()
		if err == nil {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	} else {
		uilog_svc := service_impl.NewUILogSvc(top_svc, nil, utils)
		eventlog_svc := service_impl.NewEventLog(top_svc, utils, nil)
		remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(uilog_svc, metakv_svc, top_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting remote cluster service. err=%v\n", err)
			os.Exit(1)
		}
		resolver_svc := service_impl.NewResolverSvc(top_svc)

		replicationSettingSvc := metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil, top_svc)

		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(uilog_svc, remote_cluster_svc, metakv_svc, top_svc, resolver_svc, nil, utils, replicationSettingSvc)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}

		checkpointsService, err := metadata_svc.NewCheckpointsService(metakv_svc, nil, utils, replication_spec_svc)
		if err != nil {
			fmt.Printf("Error starting checkpoints service. err=%v\n", err)
			os.Exit(1)
		}

		bucketTopologyService, err := service_impl.NewBucketTopologyService(top_svc, remote_cluster_svc, utils,
			base.TopologyChangeCheckInterval, log.DefaultLoggerContext, replication_spec_svc, securitySvc, streamApiWatcher.GetStreamApiWatcher)
		if err != nil {
			fmt.Printf("Error starting bucket topology service. err=%v\n", err)
			os.Exit(1)
		}

		manifestsService := metadata_svc.NewManifestsService(metakv_svc, nil)
		collectionsManifestService, err := metadata_svc.NewCollectionsManifestService(remote_cluster_svc,
			replication_spec_svc, uilog_svc, log.DefaultLoggerContext, utils, checkpointsService,
			top_svc, bucketTopologyService, manifestsService)
		if err != nil {
			fmt.Printf("Error starting collections manifest service. err=%v\n", err)
			os.Exit(1)
		}

		backfillReplService, err := metadata_svc.NewBackfillReplicationService(uilog_svc,
			metakv_svc, log.DefaultLoggerContext, utils, replication_spec_svc, top_svc,
			bucketTopologyService)
		if err != nil {
			fmt.Printf("Error starting backfill replication service. err=%v\n", err)
			os.Exit(1)
		}

		p2pMgr, err := peerToPeer.NewPeerToPeerMgr(log.DefaultLoggerContext, top_svc, utils, bucketTopologyService,
			replication_spec_svc, base.P2POpaqueCleanupInterval, checkpointsService, collectionsManifestService,
			backfillReplService, securitySvc, rm.BackfillManager)
		if err != nil {
			fmt.Printf("Error starting P2P manager. err=%v\n", err)
			os.Exit(1)
		}

		// start replication manager in normal mode
		rm.StartReplicationManager(host,
			uint16(options.xdcrRestPort),
			uint16(options.sourceKVAdminPort),
			replication_spec_svc,
			remote_cluster_svc,
			top_svc,
			replicationSettingSvc,
			checkpointsService,
			service_impl.NewCAPIService(nil, utils),
			audit_svc,
			uilog_svc,
			eventlog_svc,
			processSetting_svc,
			internalSettings_svc,
			service_impl.NewThroughputThrottlerSvc(nil),
			resolver_svc,
			utils,
			collectionsManifestService,
			backfillReplService,
			bucketTopologyService,
			securitySvc,
			p2pMgr)

		// keep main alive in normal mode
		<-done
	}
}

// wait [for an upward of 30 seconds] for metadata service to become available
func waitForMetadataService(metakv_svc service_def.MetadataSvc) error {
	num_retry := 0
	for {
		_, err := metakv_svc.GetAllMetadataFromCatalog(metadata_svc.RemoteClustersCatalogKey)
		if err == nil {
			return nil
		}
		num_retry++
		if num_retry > max_retry_wait_for_metadata_service {
			return fmt.Errorf("Metadata service not available after %v retries. \n", num_retry-1)
		} else {
			fmt.Printf("Metadata service not available. Retrying after %v. \n", retry_interval_wait_for_metadata_service)
			time.Sleep(retry_interval_wait_for_metadata_service)
		}
	}
}
