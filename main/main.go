// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	base "github.com/couchbase/goxdcr/base"
	log "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata_svc"
	rm "github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/service_impl"
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

	cluster_info_svc := service_impl.NewClusterInfoSvc(nil, utils)
	top_svc, err := service_impl.NewXDCRTopologySvc(uint16(options.sourceKVAdminPort), uint16(options.xdcrRestPort), options.isEnterprise, options.ipv4, options.ipv6, cluster_info_svc, nil, utils)
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
	bucketSettings_svc := metadata_svc.NewBucketSettingsService(metakv_svc, top_svc, nil, utils)

	if options.isConvert {
		// disable uilogging during upgrade by specifying a nil uilog service
		remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(nil, metakv_svc, top_svc, cluster_info_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting remote cluster service. err=%v\n", err)
			os.Exit(1)
		}
		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(nil, remote_cluster_svc, metakv_svc, top_svc, cluster_info_svc, nil, nil, utils)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}

		migration_svc := service_impl.NewMigrationSvc(top_svc, remote_cluster_svc,
			replication_spec_svc,
			metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil, top_svc),
			metadata_svc.NewCheckpointsService(metakv_svc, nil, utils),
			nil, utils)
		err = migration_svc.Migrate()
		if err == nil {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	} else {
		uilog_svc := service_impl.NewUILogSvc(top_svc, nil, utils)
		remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(uilog_svc, metakv_svc, top_svc, cluster_info_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting remote cluster service. err=%v\n", err)
			os.Exit(1)
		}
		resolver_svc := service_impl.NewResolverSvc(top_svc)

		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(uilog_svc, remote_cluster_svc, metakv_svc, top_svc, cluster_info_svc, resolver_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}

		checkpointsService := metadata_svc.NewCheckpointsService(metakv_svc, nil, utils)
		manifestsService := metadata_svc.NewManifestsService(metakv_svc, nil)
		collectionsManifestService, err := metadata_svc.NewCollectionsManifestService(remote_cluster_svc,
			replication_spec_svc, uilog_svc, log.DefaultLoggerContext, utils, checkpointsService,
			top_svc, manifestsService)
		if err != nil {
			fmt.Printf("Error starting collections manifest service. err=%v\n", err)
			os.Exit(1)
		}

		backfillReplService, err := metadata_svc.NewBackfillReplicationService(uilog_svc,
			metakv_svc, log.DefaultLoggerContext, utils, replication_spec_svc, cluster_info_svc, top_svc)
		if err != nil {
			fmt.Printf("Error starting backfill replication service. err=%v\n", err)
			os.Exit(1)
		}

		// start replication manager in normal mode
		rm.StartReplicationManager(host,
			uint16(options.xdcrRestPort),
			uint16(options.sourceKVAdminPort),
			replication_spec_svc,
			remote_cluster_svc,
			cluster_info_svc,
			top_svc,
			metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil, top_svc),
			checkpointsService,
			service_impl.NewCAPIService(cluster_info_svc, nil, utils),
			audit_svc,
			uilog_svc,
			processSetting_svc,
			bucketSettings_svc,
			internalSettings_svc,
			service_impl.NewThroughputThrottlerSvc(nil),
			resolver_svc,
			utils,
			collectionsManifestService,
			backfillReplService)

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
