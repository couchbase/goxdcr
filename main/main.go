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
	base "github.com/couchbase/goxdcr/base"
	log "github.com/couchbase/goxdcr/log"
	rm "github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/service_impl"
	"os"
	"runtime"
)

var done = make(chan bool)

var options struct {
	sourceKVAdminPort uint64 //source kv admin port
	xdcrRestPort      uint64 // port number of XDCR rest server

	sslProxyUpstreamPort uint64 // gometa request port
	isEnterprise         bool   // whether couchbase is of enterprise edition
	isConvert            bool   // whether xdcr is running in conversion/upgrade mode

	// logging related parameters
	logFileDir          string
	maxLogFileSize      uint64
	maxNumberOfLogFiles uint64
}

func argParse() {
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.Uint64Var(&options.xdcrRestPort, "xdcrRestPort", uint64(base.AdminportNumber),
		"port number of XDCR rest server")
	flag.Uint64Var(&options.sslProxyUpstreamPort, "localProxyPort", 0,
		"port number for ssl proxy upstream port")
	flag.BoolVar(&options.isEnterprise, "isEnterprise", true,
		"whether couchbase is of enterprise edition")
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
	runtime.GOMAXPROCS(rm.GoMaxProcs())

	argParse()

	// initializes logger
	if options.logFileDir != "" {
		log.Init(options.logFileDir, options.maxLogFileSize, options.maxNumberOfLogFiles)
	}

	cluster_info_svc := s.NewClusterInfoSvc(nil)

	top_svc, err := s.NewXDCRTopologySvc(uint16(options.sourceKVAdminPort), uint16(options.xdcrRestPort), uint16(options.sslProxyUpstreamPort), options.isEnterprise, cluster_info_svc, nil)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}

	host := base.LocalHostName

	//metadata_svc, err := s.NewMetadataSvc(utils.GetHostAddr(host, uint16(options.gometaRequestPort)), nil)
	metadata_svc, err := s.NewMetaKVMetadataSvc(nil)
	if err != nil {
		fmt.Printf("Error starting metadata service. err=%v\n", err)
		os.Exit(1)
	}

	audit_svc, err := s.NewAuditSvc(top_svc, nil)
	if err != nil {
		fmt.Printf("Error starting audit service. err=%v\n", err)
		os.Exit(1)
	}

	if options.isConvert {
		// disable uilogging during upgrade by specifying a nil uilog service
		remote_cluster_svc := s.NewRemoteClusterService(nil, metadata_svc, top_svc, cluster_info_svc, nil)
		migration_svc := s.NewMigrationSvc(top_svc, remote_cluster_svc,
			s.NewReplicationSpecService(nil, remote_cluster_svc, metadata_svc, top_svc, nil),
			s.NewReplicationSettingsSvc(metadata_svc, nil),
			s.NewCheckpointsService(metadata_svc, nil),
			nil)
		err = migration_svc.Migrate()
		if err == nil {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	} else {
		uilog_svc := s.NewUILogSvc(top_svc, nil)
		remote_cluster_svc := s.NewRemoteClusterService(uilog_svc, metadata_svc, top_svc, cluster_info_svc, nil)
		// start replication manager in normal mode
		rm.StartReplicationManager(host,
			uint16(options.xdcrRestPort),
			s.NewReplicationSpecService(uilog_svc, remote_cluster_svc, metadata_svc, top_svc, nil),
			remote_cluster_svc,
			cluster_info_svc,
			top_svc,
			s.NewReplicationSettingsSvc(metadata_svc, nil),
			s.NewCheckpointsService(metadata_svc, nil),
			s.NewCAPIService(cluster_info_svc, nil),
			audit_svc)

		// keep main alive in normal mode
		<-done
	}
}
