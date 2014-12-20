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
	"os"
	rm "github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/service_impl"
	utils "github.com/couchbase/goxdcr/utils"	
	base "github.com/couchbase/goxdcr/base"

	"github.com/couchbase/goxdcr/metakv/metakvsanity"
)

var done = make(chan bool)

var options struct {
	sourceKVAdminPort      uint64 //source kv admin port
	xdcrRestPort      uint64 // port number of XDCR rest server
	gometaRequestPort        uint64// gometa request port
	isEnterprise    bool  // whether couchbase is of enterprise edition
	isConvert    bool  // whether xdcr is running in conversion/upgrade mode
	// TODO remove after auth changes
	username        string //username on source cluster
	password        string //password on source cluster	
}

func argParse() {
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.Uint64Var(&options.xdcrRestPort, "xdcrRestPort", uint64(base.AdminportNumber),
		"port number of XDCR rest server")
	flag.Uint64Var(&options.gometaRequestPort, "gometaRequestPort", uint64(base.GometaRequestPortNumber),
		"port number for gometa requests")
	flag.BoolVar(&options.isEnterprise, "isEnterprise", true,
		"whether couchbase is of enterprise edition")
	flag.BoolVar(&options.isConvert, "isConvert", false,
		"whether xdcr is running in convertion/upgrade mode")
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()

	metakvsanity.MaybeRun()

	top_svc, err := s.NewXDCRTopologySvc(options.username, options.password, uint16(options.sourceKVAdminPort), uint16(options.xdcrRestPort), options.isEnterprise, nil)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}
	
	host, err := top_svc.MyHost()
	if err != nil {
		fmt.Printf("Error getting current host. err=%v\n", err)
		os.Exit(1)
	}

	metadata_svc, err := s.NewMetadataSvc(utils.GetHostAddr(host, uint16(options.gometaRequestPort)), nil)
	if err != nil {
		fmt.Printf("Error starting metadata service. err=%v\n", err)
		os.Exit(1)
	}
	
	if options.isConvert {
		fmt.Println("Starting replication manager in conversion/upgrade mode.")
		// start replication manager in conversion/upgrade mode
		rm.StartReplicationManagerForConversion(
							   s.NewReplicationSpecService(metadata_svc, nil),
							   s.NewRemoteClusterService(metadata_svc, nil))
	} else {
		// start replication manager in normal mode
		rm.StartReplicationManager(host,
							   uint16(options.xdcrRestPort),
							   s.NewReplicationSpecService(metadata_svc, nil),
							   s.NewRemoteClusterService(metadata_svc, nil),	
							   s.NewClusterInfoSvc(nil), 
							   top_svc, 
							   s.NewReplicationSettingsSvc(metadata_svc, nil))
							   
		// keep main alive in normal mode
		<-done
	}						 
}
