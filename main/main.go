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

	ap "github.com/Xiaomei-Zhang/goxdcr/adminport"
	rm "github.com/Xiaomei-Zhang/goxdcr/replication_manager"
	c "github.com/Xiaomei-Zhang/goxdcr/mock_services"
	s "github.com/Xiaomei-Zhang/goxdcr/services"
)

var done = make(chan bool)

var options struct {
	sourceClusterAddr      string //source cluster addr
	sourceKVHost      string //source kv host name
	username        string //username on source cluster
	password        string //password on source cluster	
	// temp params for mock services. should be removed later
	targetClusterAddr      string //target cluster addr
	sourceBucket          string // source bucket
	targetBucket          string // target bucket
}

func argParse() {
	flag.StringVar(&options.sourceClusterAddr, "sourceClusterAddr", "127.0.0.1:9000",
		"connection string to source cluster")
	flag.StringVar(&options.sourceKVHost, "sourceKVHost", "127.0.0.1",
		"source KV host name")
	flag.StringVar(&options.targetClusterAddr, "targetClusterAddr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")
		
	flag.StringVar(&options.sourceBucket, "sourceBucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.targetBucket, "targetBucket", "target",
		"bucket to replicate to")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	
	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Failed to start gometa service. err: ", err)
		os.Exit(1)
	}
	defer s.KillGometaService(cmd)
	
	c.SetTestOptions(options.sourceBucket, options.targetBucket, options.sourceClusterAddr, options.targetClusterAddr, options.sourceKVHost, options.username, options.password)
		
	xdcrTopologyService := new(c.MockXDCRTopologySvc)
	hostAddr, err := xdcrTopologyService.MyHost()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting host address \n")
		os.Exit(1)
	}
	
	metadata_svc, err := s.DefaultMetadataSvc()
	if err != nil {
		fmt.Println("Error starting metadata service. ", err.Error())
		os.Exit(1)
	}
	
	rm.Initialize(metadata_svc, new(c.MockClusterInfoSvc), xdcrTopologyService, new(c.MockReplicationSettingsSvc))
	go ap.MainAdminPort(hostAddr)
	<-done
}
