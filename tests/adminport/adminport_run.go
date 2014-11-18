// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// Test for KVFeed, source nozzle in XDCR
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	c "github.com/couchbase/goxdcr/mock_services"
	pm "github.com/couchbase/goxdcr/pipeline_manager"
	rm "github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/services"
	ms "github.com/couchbase/goxdcr/mock_services"
	utils "github.com/couchbase/goxdcr/utils"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	TestTopic        = "test"
	NumSourceConn    = 2
	NumTargetConn    = 3
	FilterExpression = "testExpr"
	BatchCount       = 20
	BatchSize        = 30
)

var options struct {
	sourceBucket string // source bucket
	targetBucket string //target bucket
	sourceKVHost string //source kv host name
	sourceKVPort      int //source kv admin port
	gometaPort        int // gometa request port
	filterName   string //filter name
	username     string //username
	password     string //password
}

func argParse() {
	flag.StringVar(&options.sourceKVHost, "sourceKVHost", "127.0.0.1",
		"source KV host name")
	flag.IntVar(&options.sourceKVPort, "sourceKVPort", 9000,
		"admin port number for source kv")
	flag.IntVar(&options.gometaPort, "gometaPort", 5003,
		"port number for gometa requests")
	flag.StringVar(&options.sourceBucket, "sourceBucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.targetBucket, "targetBucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.filterName, "filterName", "myActive",
		"name of filter to use for replication")
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing adminport...")
	argParse()
	fmt.Printf("sourceKVHost=%s\n", options.sourceKVHost)
	fmt.Println("Done with parsing the arguments")
	startAdminport()
}

func startAdminport() {
	c.SetTestOptions(utils.GetHostAddr(options.sourceKVHost, options.sourceKVPort), options.sourceKVHost, options.username, options.password)

	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}

	defer s.KillGometaService(cmd)

	metadata_svc, err := s.DefaultMetadataSvc()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}
	
	rm.StartReplicationManager(options.sourceKVHost, options.sourceKVPort,
								  metadata_svc, new(ms.MockClusterInfoSvc), new(ms.MockXDCRTopologySvc), new(ms.MockReplicationSettingsSvc))
	
	//wait for server to finish starting
	time.Sleep(time.Second * 3)

	replicationId, err := testCreateReplication()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	escapedReplId := url.QueryEscape(replicationId)
	fmt.Println("replicationId: ", replicationId, " escaped replication id: "+escapedReplId)

	if err := testViewReplicationSettings(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testChangeReplicationSettings(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testPauseReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testResumeReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testGetStatistics(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testDeleteReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("All tests passed.")

}

func getUrlPrefix() string {
	return "http://" + utils.GetHostAddr(options.sourceKVHost, base.AdminportNumber) + base.AdminportUrlPrefix
}

func testCreateReplication() (string, error) {
	url := getUrlPrefix() + rm.CreateReplicationPath

	params := make(map[string]interface{})
	params[rm.FromBucket] = options.sourceBucket
	params[rm.ToClusterUuid] = utils.GetHostAddr(options.sourceKVHost, options.sourceKVPort)
	params[rm.ToBucket] = options.targetBucket
	params[rm.FilterName] = options.filterName
	params[rm.FilterExpression] = FilterExpression
	params[rm.BatchCount] = BatchCount

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)

	request, err := http.NewRequest(rm.MethodPost, url, paramsBuf)
	if err != nil {
		return "", err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	err = validateResponse("CreateReplication", response, err)
	if err != nil {
		return "", err
	}

	replicationId, err := rm.DecodeCreateReplicationResponse(response)
	
	time.Sleep(30 * time.Second)

	// verify that the replication is created and started and is being
	// managed by pipeline manager
	return replicationId, validatePipeline("CreateReplication", replicationId, true)
}

func testPauseReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + rm.PauseReplicationPrefix + base.UrlDelimiter + escapedReplId

	request, err := http.NewRequest(rm.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	err = validateResponse("PauseReplication", response, err)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("PauseReplication", replicationId, false)
}

func testResumeReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + rm.ResumeReplicationPrefix + base.UrlDelimiter + escapedReplId

	request, err := http.NewRequest(rm.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	err = validateResponse("ResumeReplication", response, err)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("ResumeReplication", replicationId, true)
}

func testDeleteReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + rm.DeleteReplicationPrefix + base.UrlDelimiter + escapedReplId

	request, err := http.NewRequest(rm.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	err = validateResponse("DeleteReplication", response, err)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("DeleteReplication", replicationId, false)
}

func testViewReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + rm.SettingsReplicationsPath + base.UrlDelimiter + replicationId

	request, err := http.NewRequest(rm.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	return validateResponse("ViewReplicationSettings", response, err)
}

func testChangeReplicationSettings(replicationId, escapedReplicationId string) error {
	url := getUrlPrefix() + rm.SettingsReplicationsPath + base.UrlDelimiter + escapedReplicationId

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSize

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)

	request, err := http.NewRequest(rm.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)
	err = validateResponse("ChangeReplicationSettings", response, err)
	if err != nil {
		return err
	}
	
	spec, err := rm.MetadataService().ReplicationSpec(replicationId)
	if err != nil {
		return err
	}
	resultingBatchSize := spec.Settings.BatchSize
	if resultingBatchSize != BatchSize {
		return errors.New(fmt.Sprintf("TestChangeReplicationSettings failed. Resulting batch size, %v, does not match the specified value, %v\n", resultingBatchSize, BatchSize))
	}
	
	return nil
}

func testGetStatistics() error {
	url := getUrlPrefix() + rm.StatisticsPath

	request, err := http.NewRequest(rm.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	return validateResponse("GetStatistics", response, err)
}

func validateResponse(testName string, response *http.Response, err error) error {
	if err != nil || response.StatusCode != 200 {
		errMsg := fmt.Sprintf("Test %v failed. err=%v", testName, err)
		if response != nil {
			errMsg += fmt.Sprintf("; response status=%v", response.Status)
		}
		errMsg += "\n"
		return errors.New(errMsg)
	}
	return nil
}

func validatePipeline(testName string, replicationId string, pipelineRunning bool) error {
	if (pm.Pipeline(replicationId) == nil) == pipelineRunning {
		var errMsg string
		if pipelineRunning {
			errMsg = ", should be running but was not"
		} else {
			errMsg = ", should not be running but was"
		}
		return errors.New(fmt.Sprintf("Test %v failed. Pipeline, %v%v\n", testName, replicationId, errMsg))
	} else {
		fmt.Println("Test ", testName, " passed.")
		return nil
	}
}
