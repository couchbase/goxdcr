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
	ms "github.com/couchbase/goxdcr/mock_services"
	s "github.com/couchbase/goxdcr/service_impl"
	utils "github.com/couchbase/goxdcr/utils"	
)

var done = make(chan bool)

var options struct {
	sourceKVHost      string //source kv host name
	sourceKVPort      int //source kv admin port
	gometaPort        int // gometa request port
	username        string //username on source cluster
	password        string //password on source cluster	
}

func argParse() {
	flag.StringVar(&options.sourceKVHost, "sourceKVHost", "127.0.0.1",
		"source KV host name")
	flag.IntVar(&options.sourceKVPort, "sourceKVPort", 9000,
		"admin port number for source kv")
	flag.IntVar(&options.gometaPort, "gometaPort", 5003,
		"port number for gometa requests")
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
	
	// TODO remove after real services are implemented
	ms.SetTestOptions(utils.GetHostAddr(options.sourceKVHost, options.sourceKVPort), options.sourceKVHost, options.username, options.password)
	
	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Failed to start gometa service. err: ", err)
		os.Exit(1)
	}
	defer s.KillGometaService(cmd)
	
	metadata_svc, err := s.NewMetadataSvc(utils.GetHostAddr(options.sourceKVHost, options.gometaPort), nil)
	if err != nil {
		fmt.Println("Error starting metadata service. ", err.Error())
		os.Exit(1)
	}

	rm.StartReplicationManager(options.sourceKVHost, options.sourceKVPort,
							   s.NewReplicationSpecService(metadata_svc, nil),
							   s.NewRemoteClusterService(metadata_svc, nil),	
							   new(ms.MockClusterInfoSvc), 
							   new(ms.MockXDCRTopologySvc), 
							   new(ms.MockReplicationSettingsSvc))
								  
	<-done
}
