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
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"encoding/json"
	base "github.com/couchbase/goxdcr/base"
	pm "github.com/couchbase/goxdcr/pipeline_manager"
	rm "github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/tests/common"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	NumSourceConn    = 2
	NumTargetConn    = 3
	BatchCount       = 600
	BatchSizeInternal       = 512
	BatchSizeDefault       = 1024
	BatchSizePerRepl        = 3036
)

var options struct {
	sourceBucket string // source bucket
	targetBucket string //target bucket
	sourceKVHost string //source kv host name
	sourceKVAdminPort      uint64 //source kv admin port
	filterName   string //filter name
	username     string //username
	password     string //password
	
	// parameters of remote cluster
	remoteUuid string // remote cluster uuid
	remoteName string // remote cluster name
	remoteHostName string // remote cluster host name
	remoteUserName     string //remote cluster userName
	remotePassword     string //remote cluster password
	remoteDemandEncryption  bool  // whether encryption is needed
	remoteCertificateFile  string // file containing certificate for encryption
}

func argParse() {
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.sourceBucket, "sourceBucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.targetBucket, "targetBucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.filterName, "filterName", "myActive",
		"name of filter to use for replication")
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")

	flag.StringVar(&options.remoteUuid, "remoteUuid", "1234567",
		"remote cluster uuid")
	flag.StringVar(&options.remoteName, "remoteName", "remote",
		"remote cluster name")
	flag.StringVar(&options.remoteHostName, "remoteHostName", "127.0.0.1:9000",
		"remote cluster host name")
	flag.StringVar(&options.remoteUserName, "remoteUserName", "Administrator", "remote cluster userName")
	flag.StringVar(&options.remotePassword, "remotePassword", "welcome", "remote cluster password")
	flag.BoolVar(&options.remoteDemandEncryption, "remoteDemandEncryption", false, "whether encryption is needed")
	flag.StringVar(&options.remoteCertificateFile, "remoteCertificateFile", "", "file containing certificate for encryption")


	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing adminport...")
	argParse()
	fmt.Println("Done with parsing the arguments")
	startAdminport()
}

func startAdminport() {
	top_svc, err := s.NewXDCRTopologySvc(options.username, options.password, uint16(options.sourceKVAdminPort), base.AdminportNumber, true, nil)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}
	
	options.sourceKVHost, err = top_svc.MyHost()
	if err != nil {
		fmt.Printf("Error getting current host. err=%v\n", err)
		os.Exit(1)
	}
	
	metadata_svc, err := s.DefaultMetadataSvc()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}
	
	remote_cluster_svc := s.NewRemoteClusterService(metadata_svc, nil)
	
	rm.StartReplicationManager(options.sourceKVHost,
							   base.AdminportNumber,
							   s.NewReplicationSpecService(metadata_svc, nil),
							   remote_cluster_svc,	
							   s.NewClusterInfoSvc(nil), 
							   top_svc, 
							   s.NewReplicationSettingsSvc(metadata_svc, nil))
	
	//wait for server to finish starting
	time.Sleep(time.Second * 3)
	
	// create remote cluster reference needed by replication
	err = common.CreateTestRemoteCluster(remote_cluster_svc, options.remoteUuid, options.remoteName, options.remoteHostName, options.remoteUserName, options.remotePassword, 
                             options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	
	defer common.DeleteTestRemoteCluster(remote_cluster_svc, options.remoteName)
	
	if err := testChangeInternalSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testViewInternalSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testChangeDefaultReplicationSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testViewDefaultReplicationSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
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

	if err := testGetStatistics(options.sourceBucket); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testDeleteReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("All tests passed.")

}

func testChangeInternalSettings() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.InternalSettingsPath

	params := make(map[string]interface{})
	params[rm.ConvertRestKeyToRestInternalKey(rm.BatchSize)] = BatchSizeInternal

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeInternalSettings", base.MethodPost, url, paramsBytes)
	if err != nil {
		return err
	}
	
	defaultSettings, err := rm.ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return err
	}
	return common.ValidateFieldValue("internal BatchSize", BatchSizeInternal, defaultSettings.BatchSize)
}

func testViewInternalSettings() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.InternalSettingsPath
	response, err := common.SendRequestAndValidateResponse("testViewInternalSettings", base.MethodGet, url, nil)
	if err != nil {
		return err
	}
	
	settingsMap, err := decodeSettingsMapFromResponse(response)
	if err != nil {
		return err
	}
	newBatchSize := int(settingsMap[rm.ConvertRestKeyToRestInternalKey(rm.BatchSize)].(float64))
	return common.ValidateFieldValue("internal BatchSize", BatchSizeInternal, newBatchSize)
}

func testChangeDefaultReplicationSettings() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizeDefault

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeDefaultReplicationSettings", base.MethodPost, url, paramsBytes)
	if err != nil {
		return err
	}
	
	defaultSettings, err := rm.ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return err
	}
	return common.ValidateFieldValue("default BatchSize", BatchSizeDefault, defaultSettings.BatchSize)
}

func testViewDefaultReplicationSettings() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath
	response, err := common.SendRequestAndValidateResponse("testViewDefaultReplicationSettings", base.MethodGet, url, nil)
	if err != nil {
		return err
	}
	
	settingsMap, err := decodeSettingsMapFromResponse(response)
	fmt.Printf("m=%v\n", settingsMap)
	if err != nil {
		return err
	}
	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	return common.ValidateFieldValue("default BatchSize", BatchSizeDefault, newBatchSize)
}
	
func testCreateReplication() (string, error) {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.CreateReplicationPath

	params := make(map[string]interface{})
	params[rm.FromBucket] = options.sourceBucket
	params[rm.ToCluster] = options.remoteName
	params[rm.ToBucket] = options.targetBucket
	params[rm.FilterName] = options.filterName
	params[rm.BatchCount] = BatchCount

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)

	response, err := common.SendRequestAndValidateResponse("testCreateReplication", base.MethodPost, url, paramsBytes)
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
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath + base.UrlDelimiter + escapedReplId

	settings := make(map[string]interface{})
	settings[rm.Paused] = true
	paramsBytes, _ := rm.EncodeMapIntoByteArray(settings)

	_, err := common.SendRequestAndValidateResponse("testPauseReplication", base.MethodPost, url, paramsBytes)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("PauseReplication", replicationId, false)
}

func testResumeReplication(replicationId, escapedReplId string) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath + base.UrlDelimiter + escapedReplId

	settings := make(map[string]interface{})
	settings[rm.Paused] = false
	paramsBytes, _ := rm.EncodeMapIntoByteArray(settings)
	
	_, err := common.SendRequestAndValidateResponse("testResumeReplication", base.MethodPost, url, paramsBytes)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("ResumeReplication", replicationId, true)
}

func testDeleteReplication(replicationId, escapedReplId string) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.DeleteReplicationPrefix + base.UrlDelimiter + escapedReplId

	_, err := common.SendRequestAndValidateResponse("testDeleteReplication", base.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	
	time.Sleep(10 * time.Second)

	return validatePipeline("DeleteReplication", replicationId, false)
}

func testViewReplicationSettings(replicationId string) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath + base.UrlDelimiter + replicationId

	response, err := common.SendRequestAndValidateResponse("testViewReplicationSettings", base.MethodGet, url, nil)
	if err != nil {
		return err
	}
	
	settingsMap, err := decodeSettingsMapFromResponse(response)
	if err != nil {
		return err
	}
	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	return common.ValidateFieldValue("BatchSize in replication", BatchSizeDefault, newBatchSize)
}

func testChangeReplicationSettings(replicationId, escapedReplicationId string) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.SettingsReplicationsPath + base.UrlDelimiter + escapedReplicationId

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizePerRepl

	paramsBytes, _ := rm.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeReplicationSettings", base.MethodPost, url, paramsBytes)
	if err != nil {
		return err
	}
	
	// verify that batchsize of the particular replication has been changed
	spec, err := rm.ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return err
	}
	err = common.ValidateFieldValue("BatchSize in replication", BatchSizePerRepl, spec.Settings.BatchSize)
	if err != nil {
		return err
	}
	
	// verify that default batchsize has not been changed
	defaultSettings, err := rm.ReplicationSettingsService().GetDefaultReplicationSettings()

	if err != nil {
		return err
	}
	return common.ValidateFieldValue("default BatchSize", BatchSizeDefault, defaultSettings.BatchSize)
}

func testGetStatistics(bucket string) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.StatisticsPrefix +base.UrlDelimiter + bucket
	_, err := common.SendRequestAndValidateResponse("testGetStatistics", base.MethodGet, url, nil)
	return err
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

func decodeSettingsMapFromResponse(response *http.Response) (map[string]interface{}, error) {
	defer response.Body.Close()

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var settingsMap map[string]interface{}
	err = json.Unmarshal(bodyBytes, &settingsMap)
	if err != nil {
		return nil, err
	} else {
		return settingsMap, nil
	}
}
