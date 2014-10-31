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
	"flag"
	"fmt"
	"os"
	"time"
	"net/http"
	"bytes"
	"errors"
	"net/url"
	ap "github.com/Xiaomei-Zhang/goxdcr/adminport"
	base "github.com/Xiaomei-Zhang/goxdcr/base"
	rm "github.com/Xiaomei-Zhang/goxdcr/replication_manager"
	c "github.com/Xiaomei-Zhang/goxdcr/mock_services"
	s "github.com/Xiaomei-Zhang/goxdcr/services"
	utils "github.com/Xiaomei-Zhang/goxdcr/utils"
	pm "github.com/Xiaomei-Zhang/goxdcr/pipeline_manager"
)

const (
	TestTopic      = "test"
	NumSourceConn = 2
	NumTargetConn = 3
	FilterExpression = "testExpr"
	BatchCount = 20
	BatchSize = 30
	Active = false
)

var options struct {
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	connectStr      string //connect string
	sourceKVHost      string //source kv host name
	filterName      string //filter name
	username        string //username
	password        string //password
}

func argParse() {
	flag.StringVar(&options.connectStr, "connectStr", "127.0.0.1:9000",
		"connection string to source cluster")
	flag.StringVar(&options.sourceKVHost, "source_kv_host", "127.0.0.1",
		"source KV host name")
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
	c.SetTestOptions(options.sourceBucket, options.targetBucket, options.connectStr, options.connectStr, options.sourceKVHost, options.username, options.password)
	
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
	
	rm.Initialize(metadata_svc, new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))

	go ap.MainAdminPort(options.sourceKVHost)
	//wait for server to finish starting
	time.Sleep(time.Second * 3)

	replicationId, err := testCreateReplication()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	
	escapedReplId := url.QueryEscape(replicationId)
	fmt.Println("replicationId: ", replicationId, " escaped replication id: " + escapedReplId)

	if err := testViewReplicationSettings(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testChangeReplicationSettings(escapedReplId); err != nil {
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
	url := getUrlPrefix() + ap.CreateReplicationPath
	
	params := make(map[string]interface{})
	params[ap.FromBucket] = options.sourceBucket
	params[ap.ToClusterUuid] = options.connectStr
	params[ap.ToBucket] = options.targetBucket
	params[ap.FilterName] = options.filterName
	params[ap.FilterExpression] = FilterExpression
	params[ap.BatchCount] = BatchCount
		
	paramsBytes, _ := ap.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return "", err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("CreateReplication", response, err)
	if err != nil {
		return "", err
	}
	
	replicationId, err := ap.DecodeCreateReplicationResponse(response)
	
	// verify that the replication is created and started and is being
	// managed by pipeline manager
	return replicationId, validatePipeline("CreateReplication", replicationId, true)
}

func testPauseReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.PauseReplicationPrefix + base.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("PauseReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("PauseReplication", replicationId, false)
}

func testResumeReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.ResumeReplicationPrefix + base.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("ResumeReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("ResumeReplication", replicationId, true)
}

func testDeleteReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.DeleteReplicationPrefix + base.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("DeleteReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("DeleteReplication", replicationId, false)
}

func testViewReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + ap.SettingsReplicationsPath + base.UrlDelimiter + replicationId
	
	request, err := http.NewRequest(ap.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	return validateResponse("ViewReplicationSettings", response, err)
}

func testChangeReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + ap.SettingsReplicationsPath + base.UrlDelimiter + replicationId
	
	params := make(map[string]interface{})
	params[ap.Active] = Active
	params[ap.BatchSize] = BatchSize
		
	paramsBytes, _ := ap.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	return validateResponse("ChangeReplicationSettings", response, err)
}

func testGetStatistics() error {
	url := getUrlPrefix() + ap.StatisticsPath
	
	request, err := http.NewRequest(ap.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, ap.DefaultContentType)
	
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
	} else{
		fmt.Println("Test ", testName, " passed.")
		return nil
	}
}





