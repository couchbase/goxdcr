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
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/service_impl"
	testcommon "github.com/couchbase/goxdcr/tests/common"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"os"
	"time"
)

import _ "net/http/pprof"
import _ "expvar"

const (
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

var options struct {
	source_bucket       string // source bucket
	target_bucket       string //target bucket
	source_cluster_addr string //source connect string
	target_cluster_addr string //target connect string
	source_kv_host      string //source kv host name
	source_kv_port      uint64 //source kv admin port

	target_bucket_password string //target bucket password

	// parameters of remote cluster
	remoteUuid             string // remote cluster uuid
	remoteName             string // remote cluster name
	remoteHostName         string // remote cluster host name
	remoteUserName         string //remote cluster userName
	remotePassword         string //remote cluster password
	remoteDemandEncryption uint64 // whether encryption is needed
	remoteCertificateFile  string // file containing certificate for encryption
}

var logger *log.CommonLogger = log.NewLogger("xdcr_pipeline", log.DefaultLoggerContext)

func argParse() {
	flag.Uint64Var(&options.source_kv_port, "source_kv_port", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.remoteUuid, "remoteUuid", "1234567",
		"remote cluster uuid")
	flag.StringVar(&options.remoteName, "remoteName", "remote",
		"remote cluster name")
	flag.StringVar(&options.remoteHostName, "remoteHostName", "127.0.0.1:9000",
		"remote cluster host name")
	flag.StringVar(&options.remoteUserName, "remoteUserName", "Administrator", "remote cluster userName")
	flag.StringVar(&options.remotePassword, "remotePassword", "welcome", "remote cluster password")
	flag.Uint64Var(&options.remoteDemandEncryption, "remoteDemandEncryption", 0, "whether encryption is needed")
	flag.StringVar(&options.remoteCertificateFile, "remoteCertificateFile", "", "file containing certificate for encryption")
	flag.StringVar(&options.target_bucket_password, "target_bucket_password", "",
		"password to use for accessing target bucket")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	go func() {
		logger.Info("Try to start pprof...")
		err := http.ListenAndServe("localhost:7000", nil)
		if err != nil {
			panic(err)
		} else {
			logger.Info("Http server for pprof is started")
		}
	}()

	//	c.SetLogLevel(c.LogLevelTrace)
	logger.Info("Start Testing ...")
	argParse()

	err := setup()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}

	test()
	verify()
}

func setup() error {
	logger.Info("setup....")
	cluster_info_svc := service_impl.NewClusterInfoSvc(nil)
	top_svc, err := service_impl.NewXDCRTopologySvc(uint16(options.source_kv_port), base.AdminportNumber, 11997, true, cluster_info_svc, nil)
	if err != nil {
		logger.Errorf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}

	options.source_kv_host, err = top_svc.MyHost()
	if err != nil {
		logger.Errorf("Error getting current host. err=%v\n", err)
		os.Exit(1)
	}

	options.source_cluster_addr = utils.GetHostAddr(options.source_kv_host, uint16(options.source_kv_port))

	metakv_svc, err := metadata_svc.NewMetaKVMetadataSvc(nil)
	if err != nil {
		fmt.Printf("Error creating metadata service. err=%v\n", err)
		os.Exit(1)
	}

	audit_svc, err := service_impl.NewAuditSvc(top_svc, nil)
	if err != nil {
		fmt.Printf("Error starting audit service. err=%v\n", err)
		os.Exit(1)
	}

	uilog_svc := service_impl.NewUILogSvc(top_svc, nil)
	remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(uilog_svc, metakv_svc, top_svc, cluster_info_svc, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	repl_spec_svc, err := metadata_svc.NewReplicationSpecService(uilog_svc, remote_cluster_svc, metakv_svc, top_svc, cluster_info_svc, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	processSetting_svc := metadata_svc.NewGlobalSettingsSvc(metakv_svc, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	replication_manager.StartReplicationManager(options.source_kv_host, base.AdminportNumber,
		repl_spec_svc, remote_cluster_svc,
		cluster_info_svc, top_svc, metadata_svc.NewReplicationSettingsSvc(metakv_svc, nil),
		metadata_svc.NewCheckpointsService(metakv_svc, nil), service_impl.NewCAPIService(cluster_info_svc, nil),
		audit_svc, uilog_svc, processSetting_svc)

	logger.Info("Finish setup")
	return nil
}

func test() {
	logger.Info("Start testing")
	settings := make(map[string]interface{})
	settings[metadata.PipelineLogLevel] = "Info"
	settings[metadata.CheckpointInterval] = 20
	settings[metadata.PipelineStatsInterval] = 10000
	settings[metadata.SourceNozzlePerNode] = NUM_SOURCE_CONN
	settings[metadata.BatchCount] = 500
	settings[metadata.OptimisticReplicationThreshold] = 60
	settings[metadata.Active] = true

	// create remote cluster reference needed by replication
	err := testcommon.CreateTestRemoteCluster(replication_manager.RemoteClusterService(), options.remoteUuid, options.remoteName, options.remoteHostName, options.remoteUserName, options.remotePassword,
		options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		fmt.Println(err.Error())
		fail(fmt.Sprintf("%v", err))
	}
	logger.Info("CreateTestRemoteCluster succeeded")

	defer testcommon.DeleteTestRemoteCluster(replication_manager.RemoteClusterService(), options.remoteName)

	topic, errorsMap, err := replication_manager.CreateReplication(false, options.source_bucket, options.remoteName, options.target_bucket, settings, &base.RealUserId{})
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	} else if len(errorsMap) != 0 {
		fail(fmt.Sprintf("%v", errorsMap))
	}
	//delete the replication before we go
	defer func() {
		err = replication_manager.DeleteReplication(topic, &base.RealUserId{})
		if err != nil {
			fail(fmt.Sprintf("%v", err))
		}
		fmt.Printf("Replication %s is deleted\n", topic)
	}()
	logger.Info("CreateReplication succeeded")

	time.Sleep(30 * time.Second)

	pipeline := pipeline_manager.ReplicationStatus(topic).Pipeline()

	if pipeline == nil {
		fail(fmt.Sprintf("Failed to start pipeline %v", topic))
	}

	err = verifyStartingTimestamps(pipeline, true)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}

	time.Sleep(30 * time.Second)
	logger.Info("........Verifying checkpointing ....")
	ckpt_docs, err := replication_manager.CheckpointService().CheckpointsDocs(topic)
	if err != nil {
		fail(err.Error())
	}

	if len(ckpt_docs) == 0 {
		logger.Info("Didn't find any checkpoint doc")
		fail(fmt.Sprintf("No checkpointing happended as it is supposed to"))
	}
	settings[metadata.Active] = false
	replication_manager.UpdateReplicationSettings(topic, settings, &base.RealUserId{})

	logger.Infof("Replication %s is paused\n", topic)
	time.Sleep(100 * time.Millisecond)

	settings[metadata.Active] = true
	errMap, err := replication_manager.UpdateReplicationSettings(topic, settings, &base.RealUserId{})
	if err != nil || len(errMap) > 0 {
		fail(fmt.Sprintf("err= %v, errMap=%v", err, errMap))
	}
	time.Sleep(30 * time.Second)
	pipeline = pipeline_manager.ReplicationStatus(topic).Pipeline()
	if pipeline == nil {
		logger.Info("Failed to resume replication")
		fail(fmt.Sprintf("Failed to resume replication %s", topic))
	}
	logger.Infof("Replication %s is resumed\n", topic)
	err = verifyStartingTimestamps(pipeline, false)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}

	time.Sleep(2 * time.Minute)

}

func verifyStartingTimestamps(pipeline common.Pipeline, noPreviousCkpts bool) error {
	settings := pipeline.Settings()
	vbts_map, ok := settings["VBTimestamps"].(map[uint16]*base.VBTimestamp)

	if !ok {
		return errors.New(fmt.Sprintf("VBTimestamps is not set in pipeline %v's settings", pipeline.InstanceId()))
	}
	for vbno, vbts := range vbts_map {
		if noPreviousCkpts {
			if vbts.Vbuuid == 0 || vbts.Seqno != 0 {
				return errors.New(fmt.Sprintf("VBTimestamps for vb=%v is not valid, Failover_uuid should not be null, seqno should 0, Vbuuid=%v, seqno=%v", vbno, vbts.Vbuuid, vbts.Seqno))
			}
		} else {
			if vbts.Vbuuid == 0 || vbts.Seqno == 0 {
				return errors.New(fmt.Sprintf("VBTimestamps for vb=%v is not valid, Failover_uuid should not be null, seqno should not be 0, Vbuuid=%v, seqno=%v", vbno, vbts.Vbuuid, vbts.Seqno))
			}

		}
	}
	logger.Infof("Start seqno is verified")
	return nil
}

func summary(topic string) {
	pipeline := pipeline_manager.ReplicationStatus(topic).Pipeline()
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

	err, _ := utils.QueryRestApiWithAuth("http://"+clusterAddress,
		"/pools/default/buckets/"+bucketName,
		false,
		bucketName,
		password,
		nil,
		"GET", "", nil,
		0, output, nil, false, logger)
	if err != nil {
		panic(err)
	}
	logger.Infof("name=%s itemCount=%d\n, out=%v\n", output.Name, output.Stat.ItemCount, output)
	return output.Stat.ItemCount

}

func flushTargetBkt() {
	//flush the target bucket
	baseURL := "http://" + options.target_bucket + ":" + options.target_bucket_password + "@" + options.target_cluster_addr

	err, _ := utils.QueryRestApiWithAuth(baseURL,
		"/pools/default/buckets/"+options.target_bucket+"/controller/doFlush",
		false,
		options.remoteUserName,
		options.remotePassword,
		nil,
		"POST", "", nil,
		0, nil, nil, false, logger)

	if err != nil {
		logger.Infof("Setup error=%v\n", err)
	} else {
		logger.Infof("Setup is done")
	}

}
