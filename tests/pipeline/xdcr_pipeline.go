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
	"github.com/couchbase/goxdcr/metadata"
	c "github.com/couchbase/goxdcr/mock_services"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/services"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	//	"net/http"
	"os"
	"time"
)

//import _ "net/http/pprof"

const (
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

var options struct {
	source_bucket           string // source bucket
	target_bucket           string //target bucket
	source_cluster_addr     string //source connect string
	target_cluster_addr     string //target connect string
	source_kv_host string //source kv host name
	source_kv_port      int //source kv admin port
	gometa_port        int // gometa request port
	source_cluster_username string //source cluster username
	source_cluster_password string //source cluster password
	target_cluster_username string //target cluster username
	target_cluster_password string //target cluster password
	target_bucket_password  string //target bucket password
}

func argParse() {
	flag.StringVar(&options.source_kv_host, "source_kv_host", "127.0.0.1",
		"source KV host name")
	flag.IntVar(&options.source_kv_port, "source_kv_port", 9000,
		"admin port number for source kv")
	flag.IntVar(&options.gometa_port, "gometa_port", 5003,
		"port number for gometa requests")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.source_cluster_username, "source_cluster_username", "Administrator",
		"user name to use for logging into source cluster")
	flag.StringVar(&options.source_cluster_password, "source_cluster_password", "welcome",
		"password to use for logging into source cluster")
	flag.StringVar(&options.target_cluster_username, "target_cluster_username", "Administrator",
		"user name to use for logging into target cluster")
	flag.StringVar(&options.target_cluster_password, "target_cluster_password", "welcome",
		"password to use for logging into target cluster")
	flag.StringVar(&options.target_bucket_password, "target_bucket_password", "",
		"password to use for accessing target bucket")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	//	go func() {
	//		log.Println("Try to start pprof...")
	//		err := http.ListenAndServe("localhost:7000", nil)
	//		if err != nil {
	//			panic(err)
	//		} else {
	//			log.Println("Http server for pprof is started")
	//		}
	//	}()

	//	c.SetLogLevel(c.LogLevelTrace)
	fmt.Println("Start Testing ...")
	argParse()
	
	options.source_cluster_addr = utils.GetHostAddr(options.source_kv_host, options.source_kv_port)

	// set up gometa service
	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Test failed. Failed to start goMeta service. err: ", err)
		return
	}

	defer s.KillGometaService(cmd)

	err = setup()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}

	test()
	verify()
}

func setup() error {
	//	flushTargetBkt()
	c.SetTestOptions(options.source_cluster_addr, options.source_kv_host, options.source_cluster_username, options.source_cluster_password)
	metadata_svc, err := s.DefaultMetadataSvc()
	if err != nil {
		return err
	}
	replication_manager.StartReplicationManager(options.source_kv_host, options.source_kv_port,
								  metadata_svc, new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))
	fmt.Println("Finish setup")
	return nil
}

func test() {
	fmt.Println("Start testing")
	settings := make(map[string]interface{})
	settings[metadata.PipelineLogLevel] = "Error"
	settings[metadata.SourceNozzlePerNode] = NUM_SOURCE_CONN
	settings[metadata.TargetNozzlePerNode] = NUM_TARGET_CONN
	settings[metadata.BatchCount] = 500

	topic, err := replication_manager.CreateReplication(options.source_cluster_addr, options.source_bucket, options.target_cluster_addr, options.target_bucket, "", settings, true)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	//	time.Sleep(1 * time.Second)
	//
	//	replication_manager.PauseReplication(topic)
	//
	//	err = replication_manager.SetPipelineLogLevel(topic, "Error")
	//	if err != nil {
	//		fail(fmt.Sprintf("%v", err))
	//	}
	//	fmt.Printf("Replication %s is paused\n", topic)
	//	time.Sleep(100 * time.Millisecond)
	//	err = replication_manager.ResumeReplication(topic)
	//	if err != nil {
	//		fail(fmt.Sprintf("%v", err))
	//	}
	//	fmt.Printf("Replication %s is resumed\n", topic)
	//	time.Sleep(2 * time.Second)
	//	summary(topic)
	//
	time.Sleep(1 * time.Minute)

	//delete the replication before we go
	err = replication_manager.DeleteReplication(topic, true)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	fmt.Printf("Replication %s is deleted\n", topic)

}

func summary(topic string) {
	pipeline := pipeline_manager.Pipeline(topic)
	targetNozzles := pipeline.Targets()
	for _, targetNozzle := range targetNozzles {
		fmt.Println(targetNozzle.(*parts.XmemNozzle).StatusSummary())
	}
}
func fail(msg string) {
	panic(fmt.Sprintf("TEST FAILED - %s", msg))
}

func verify() {
	sourceDocCount := getDocCounts(options.source_cluster_addr, options.source_bucket, "")
	targetDocCount := getDocCounts(options.target_cluster_addr, options.target_bucket, options.target_bucket_password)

	if sourceDocCount == targetDocCount {
		fmt.Println("TEST SUCCESS")
	} else {
		fmt.Printf("TEST FAILED\n")
		fmt.Printf("Source doc count=%d; target doc count=%d\n", sourceDocCount, targetDocCount)
	}
}

func getDocCounts(clusterAddress string, bucketName string, password string) int {
	output := &utils.CouchBucket{}
	baseURL, err := couchbase.ParseURL("http://" + clusterAddress)

	if err == nil {
		err = utils.QueryRestAPI(baseURL,
			"/pools/default/buckets/"+bucketName,
			bucketName,
			password,
			"GET",
			output, nil)
	}
	if err != nil {
		panic(err)
	}
	log.Printf("name=%s itemCount=%d\n, out=%v\n", output.Name, output.Stat.ItemCount, output)
	return output.Stat.ItemCount

}

func flushTargetBkt() {
	//flush the target bucket
	baseURL, err := couchbase.ParseURL("http://" + options.target_bucket + ":" + options.target_bucket_password + "@" + options.target_cluster_addr)

	if err == nil {
		err = utils.QueryRestAPI(baseURL,
			"/pools/default/buckets/"+options.target_bucket+"/controller/doFlush",
			options.target_cluster_username,
			options.target_cluster_password,
			"POST",
			nil, nil)
	}

	if err != nil {
		log.Printf("Setup error=%v\n", err)
	} else {
		log.Println("Setup is done")
	}

}
