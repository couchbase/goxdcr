// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/goxdcr/backfill_manager"
	base "github.com/couchbase/goxdcr/base"
	log "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata_svc"
	rm "github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/service_impl"
	utilities "github.com/couchbase/goxdcr/utils"
	"os"
	"runtime"
	"time"
)

var done = make(chan bool)

var options struct {
	sourceKVAdminPort uint64 //source kv admin port
	xdcrRestPort      uint64 // port number of XDCR rest server

	sslProxyUpstreamPort uint64 // gometa request port
	isEnterprise         bool   // whether couchbase is of enterprise edition
	isIpv6               bool   // whether couchbase supports ipv6
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
	flag.BoolVar(&options.isIpv6, "ipv6", false,
		"whether couchbase supports ipv6")
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

func main() {
	HideConsole(true)
	defer HideConsole(false)

	runtime.GOMAXPROCS(rm.GoMaxProcs_env())

	argParse()

	// initializes logger
	if options.logFileDir != "" {
		log.Init(options.logFileDir, options.maxLogFileSize, options.maxNumberOfLogFiles)
	}

	// Initializes official utility object to be used throughout
	utils := utilities.NewUtilities()

	cluster_info_svc := service_impl.NewClusterInfoSvc(nil, utils)
	top_svc, err := service_impl.NewXDCRTopologySvc(uint16(options.sourceKVAdminPort), uint16(options.xdcrRestPort), options.isEnterprise, options.isIpv6, cluster_info_svc, nil, utils)
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
		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(nil, remote_cluster_svc, metakv_svc, top_svc, cluster_info_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}

		migration_svc := service_impl.NewMigrationSvc(top_svc, remote_cluster_svc,
			replication_spec_svc,
			metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil, top_svc),
			metadata_svc.NewCheckpointsService(metakv_svc, nil),
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
		replication_spec_svc, err := metadata_svc.NewReplicationSpecService(uilog_svc, remote_cluster_svc, metakv_svc, top_svc, cluster_info_svc, nil, utils)
		if err != nil {
			fmt.Printf("Error starting replication spec service. err=%v\n", err)
			os.Exit(1)
		}
		checkpointsService := metadata_svc.NewCheckpointsService(metakv_svc, nil)
		manifestsService := metadata_svc.NewManifestsService(metakv_svc, nil)
		collectionsManifestService, err := metadata_svc.NewCollectionsManifestService(remote_cluster_svc,
			replication_spec_svc, uilog_svc, log.DefaultLoggerContext, utils, checkpointsService,
			top_svc, manifestsService)
		if err != nil {
			fmt.Printf("Error starting collections manifest service. err=%v\n", err)
			os.Exit(1)
		}
		internalSettings_svc := metadata_svc.NewInternalSettingsSvc(metakv_svc, nil)

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
			utils,
			collectionsManifestService)

		backfillMgr := backfill_manager.NewBackfillManager(collectionsManifestService,
			rm.ExitProcess, replication_spec_svc)
		backfillMgr.Start()

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
