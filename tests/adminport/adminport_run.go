// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// Test for KVFeed, source nozzle in XDCR
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	base "github.com/couchbase/goxdcr/v8/base"
	rm "github.com/couchbase/goxdcr/v8/replication_manager"
	"github.com/couchbase/goxdcr/v8/tests/common"
	"github.com/couchbase/goxdcr/v8/utils"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	NumSourceConn     = 2
	NumTargetConn     = 3
	BatchCount        = 600
	BatchSizeInternal = 512
	BatchSizeDefault  = 1024
	BatchSizePerRepl  = 3036
	GoMaxProcs = 5
)

var options struct {
	sourceBucket      string // source bucket
	targetBucket      string //target bucket
	sourceKVHost      string //source kv host name
	sourceKVAdminPort uint64 //source kv admin port
	// used by tests for internal rest apis (for which cbauth.SetRequestAuth() does not work)
	username string //username
	password string //password

	// parameters of remote cluster
	remoteUuid             string // remote cluster uuid
	remoteName             string // remote cluster name
	remoteHostName         string // remote cluster host name
	remoteUserName         string //remote cluster userName
	remotePassword         string //remote cluster password
	remoteDemandEncryption uint64   // whether encryption is needed
	remoteCertificateFile  string // file containing certificate for encryption
}

func argParse() {
	flag.StringVar(&options.sourceKVHost, "sourceKVHost", base.LocalHostName, "source kv host")
	flag.Uint64Var(&options.sourceKVAdminPort, "sourceKVAdminPort", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.sourceBucket, "sourceBucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.targetBucket, "targetBucket", "target",
		"bucket to replicate to")
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
	flag.Uint64Var(&options.remoteDemandEncryption, "remoteDemandEncryption", 0, "whether encryption is needed")
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
	//startAdminport()
	if err := testInternalSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func startAdminport() {

	// create remote cluster reference needed by replication
	err := common.CreateTestRemoteClusterThroughRest(options.sourceKVHost, options.sourceKVAdminPort, options.remoteUuid, options.remoteName, options.remoteHostName, options.remoteUserName, options.remotePassword,
		options.remoteDemandEncryption, options.remoteCertificateFile, options.username, options.password)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer common.DeleteTestRemoteClusterThroughRest(options.sourceKVHost, options.sourceKVAdminPort, options.remoteName, options.username, options.password)

	if err := testInternalSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testDefaultReplicationSettingsWithJustValidate(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testDefaultReplicationSettings(); err != nil {
		fmt.Println(err.Error())
		return
	}

	replicationId, escapedReplId, err := testCreateReplication()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testReplicationSettingsWithJustValidate(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testReplicationSettings(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testGetAllReplications(replicationId); err != nil {
		fmt.Println(err.Error())
		return
	}

	time.Sleep(time.Second * 10)

	if err := testGetAllReplicationInfos(replicationId); err != nil {
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

	// re-create replication
	replicationId, escapedReplId, err = testCreateReplication()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// delete the replication through DeleteAllReplications API
	if err := testDeleteAllReplications(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("All tests passed.")

}

func testInternalSettings() error {
	fmt.Println("Start 	testInternalSettings")
	// change internal settings
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.InternalSettingsPath

	params := make(map[string]interface{})
	params[rm.ConvertRestKeyToRestInternalKey(rm.BatchSize)] = 1000
	params[rm.ConvertRestKeyToRestInternalKey(rm.GoMaxProcs)] = 11


	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeInternalSettings", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	// view internal settings and verify changes
	response, err := common.SendRequestAndValidateResponse("testInternalSettings", base.MethodGet, url, nil, options.username, options.password)
	if err != nil {
		return err
	}

	settingsMap, err := decodeSettingsMapFromResponse(response)
	if err != nil {
		return err
	}
	newBatchSize := int(settingsMap[rm.ConvertRestKeyToRestInternalKey(rm.BatchSize)].(float64))
	newMaxProcs := int(settingsMap[rm.ConvertRestKeyToRestInternalKey(rm.GoMaxProcs)].(float64))
	fmt.Println("MaxProcs Size Done", newMaxProcs)
	common.ValidateFieldValue("MaxProcs", GoMaxProcs, newMaxProcs)
	return common.ValidateFieldValue("internal BatchSize", BatchSizeInternal, newBatchSize) 
}

func testDefaultReplicationSettingsWithJustValidate() error {
	fmt.Println("Start testDefaultReplicationSettingsWithJustValidate")

	// change default settings with just_validate flag specified in request url
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath + base.UrlDelimiter + base.JustValidatePostfix

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizeDefault

	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeDefaultReplicationSettingsWithJustValidate", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	// view default settings and verify that changes are not applied
	settingsMap, err := getDefaultSettings("testDefaultReplicationSettingsWithJustValidate")
	if err != nil {
		return err
	}

	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	err = common.ValidateFieldValue("default BatchSize", BatchSizeDefault, newBatchSize)
	if err == nil {
		return errors.New("BatchSize should not have been changed")
	}
	return nil
}

func testDefaultReplicationSettings() error {
	fmt.Println("Start testDefaultReplicationSettings")

	// change default settings
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizeDefault

	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestAndValidateResponse("testChangeDefaultReplicationSettings", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	// view default settings and verify changes
	settingsMap, err := getDefaultSettings("testDefaultReplicationSettings")
	if err != nil {
		return err
	}

	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	return common.ValidateFieldValue("default BatchSize", BatchSizeDefault, newBatchSize)
}

func testCreateReplication() (string, string, error) {
	fmt.Println("Start testCreateReplication")

	restUrl := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.CreateReplicationPath

	params := make(map[string]interface{})
	params[rm.ReplicationType] = rm.ReplicationTypeValue
	params[base.FromBucket] = options.sourceBucket
	params[base.ToCluster] = options.remoteName
	params[base.ToBucket] = options.targetBucket
	params[rm.BatchCount] = BatchCount

	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	response, err := common.SendRequestAndValidateResponse("testCreateReplication", base.MethodPost, restUrl, paramsBytes, options.username, options.password)
	if err != nil {
		return "", "", err
	}

	replicationId, err := rm.DecodeCreateReplicationResponse(response)
	escapedReplId := url.QueryEscape(replicationId)

	fmt.Printf("id=%v, eid=%v\n", replicationId, escapedReplId)

	fmt.Println("Waiting for replication to finish starting")
	time.Sleep(30 * time.Second)

	// verify that the replication is created and started
	err = validatePipeline("CreateReplication", replicationId, escapedReplId, true, true)
	if err != nil {
		return "", "", err
	}

	// verify replication settings
	settingsMap, err := getReplicationSettings("testCreateReplication", escapedReplId)
	if err != nil {
		return "", "", err
	}
	replBatchCount := int(settingsMap[rm.BatchCount].(float64))
	replBatchSize := int(settingsMap[rm.BatchSize].(float64))

	// BatchCount should take the value that is explicitly passed in from CreateReplication
	err = common.ValidateFieldValue("BatchCount in replication", BatchCount, replBatchCount)
	if err != nil {
		return "", "", err
	}
	// BatchSize should take the value from default settings
	err = common.ValidateFieldValue("BatchSize in replication", BatchSizeDefault, replBatchSize)
	if err != nil {
		return "", "", err
	}

	return replicationId, escapedReplId, nil
}

func testGetAllReplications(replicationId string) error {
	fmt.Println("Start testGetAllReplications")

	// has to use internal rest port for this since it is not a public api
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, uint64(base.AdminportNumber)) + rm.AllReplicationsPath

	response, err := common.SendRequestAndValidateResponse("testGetAllReplications", base.MethodGet, url, nil, options.username, options.password)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var replMapArr []map[string]interface{}
	err = json.Unmarshal(bodyBytes, &replMapArr)
	if err != nil {
		return err
	}

	if len(replMapArr) != 1 {
		return errors.New(fmt.Sprintf("Number of replications returned is %v instead of 1\n", len(replMapArr)))
	}

	replMap := replMapArr[0]

	if replMap[base.ReplicationDocId] != replicationId {
		return errors.New(fmt.Sprintf("Did not find replication with id %v\n", replicationId))
	}

	err = common.ValidateFieldValue("Type", base.ReplicationDocTypeXmem, replMap[base.ReplicationDocType])
	if err != nil {
		return err
	}
	return common.ValidateFieldValue("PauseRequested", false, replMap[base.ReplicationDocPauseRequested])
}

func testGetAllReplicationInfos(replicationId string) error {
	fmt.Println("Start testGetAllReplicationInfos")

	// has to use internal rest port for this since it is not a public api
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, uint64(base.AdminportNumber)) + rm.AllReplicationInfosPath

	response, err := common.SendRequestAndValidateResponse("testGetAllReplicationInfos", base.MethodGet, url, nil, options.username, options.password)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var replInfoArr []base.ReplicationInfo
	err = json.Unmarshal(bodyBytes, &replInfoArr)
	if err != nil {
		return err
	}

	if len(replInfoArr) != 1 {
		return errors.New(fmt.Sprintf("Number of replications returned is %v instead of 1\n", len(replInfoArr)))
	}

	replInfo := replInfoArr[0]

	if replInfo.Id != replicationId {
		return errors.New(fmt.Sprintf("Did not find replication info with id %v\n", replicationId))
	}
	return nil
}

func testPauseReplication(replicationId, escapedReplId string) error {
	fmt.Println("Start testPauseReplication")

	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath

	settings := make(map[string]interface{})
	settings[rm.PauseRequested] = true
	paramsBytes, _ := utils.EncodeMapIntoByteArray(settings)

	_, err := common.SendRequestWithEscapedIdAndValidateResponse("testPauseReplication", base.MethodPost, url, escapedReplId, paramsBytes, options.username, options.password)

	if err != nil {
		return err
	}

	fmt.Println("Waiting for replication to finish pausing")
	time.Sleep(10 * time.Second)

	return validatePipeline("PauseReplication", replicationId, escapedReplId, true, false)
}

func testResumeReplication(replicationId, escapedReplId string) error {
	fmt.Println("Start testResumeReplication")

	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath

	settings := make(map[string]interface{})
	settings[rm.PauseRequested] = false
	paramsBytes, _ := utils.EncodeMapIntoByteArray(settings)

	_, err := common.SendRequestWithEscapedIdAndValidateResponse("testResumeReplication", base.MethodPost, url, escapedReplId, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	fmt.Println("Waiting for replication to finish resuming")
	time.Sleep(10 * time.Second)

	return validatePipeline("ResumeReplication", replicationId, escapedReplId, true, true)
}

func testDeleteReplication(replicationId, escapedReplId string) error {
	fmt.Println("Start testDeleteReplication")
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.DeleteReplicationPrefix

	_, err := common.SendRequestWithEscapedIdAndValidateResponse("testDeleteReplication", base.MethodDelete, url, escapedReplId, nil, options.username, options.password)
	if err != nil {
		return err
	}

	fmt.Println("Waiting for replication to finish deleting")
	time.Sleep(10 * time.Second)

	return validatePipeline("DeleteReplication", replicationId, escapedReplId, false, false)
}

func testDeleteAllReplications(replicationId, escapedReplId string) error {
	fmt.Println("Start testDeleteAllReplications")
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, uint64(base.AdminportNumber)) + rm.AllReplicationsPath + base.UrlDelimiter + options.sourceBucket

	_, err := common.SendRequestAndValidateResponse("testDeleteAllReplications", base.MethodDelete, url, nil, options.username, options.password)
	if err != nil {
		return err
	}

	fmt.Println("Waiting for replication to finish deleting")
	time.Sleep(10 * time.Second)

	return validatePipeline("DeleteAllReplications", replicationId, escapedReplId, false, false)
}

func testReplicationSettingsWithJustValidate(escapedReplId string) error {
	fmt.Println("Start testReplicationSettingsWithJustValidate")
	testName := "testReplicationSettingsWithJustValidate"

	// change replication settings
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizePerRepl
	// specify just_validate in request body
	params[base.JustValidate] = true

	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestWithEscapedIdAndValidateResponse(testName, base.MethodPost, url, escapedReplId, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	// verify that BatchSize in repl settings is not changed
	settingsMap, err := getReplicationSettings(testName, escapedReplId)
	if err != nil {
		return err
	}

	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	err = common.ValidateFieldValue("BatchSize in Replication", BatchSizePerRepl, newBatchSize)
	if err == nil {
		return errors.New("BatchSize should not have been changed")
	}

	return nil
}

func testReplicationSettings(escapedReplId string) error {
	fmt.Println("Start testReplicationSettings")
	testName := "testReplicationSettings"

	// change replication settings
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath

	params := make(map[string]interface{})
	params[rm.BatchSize] = BatchSizePerRepl

	paramsBytes, _ := utils.EncodeMapIntoByteArray(params)

	_, err := common.SendRequestWithEscapedIdAndValidateResponse(testName, base.MethodPost, url, escapedReplId, paramsBytes, options.username, options.password)

	if err != nil {
		return err
	}

	// verify that BatchSize in repl settings is changed
	settingsMap, err := getReplicationSettings(testName, escapedReplId)
	if err != nil {
		return err
	}

	newBatchSize := int(settingsMap[rm.BatchSize].(float64))
	err = common.ValidateFieldValue("BatchSize in Replication", BatchSizePerRepl, newBatchSize)
	if err != nil {
		return err
	}

	// view that BatchSize in default settings is not changed
	settingsMap, err = getDefaultSettings(testName)
	if err != nil {
		return err
	}

	defaultBatchSize := int(settingsMap[rm.BatchSize].(float64))
	return common.ValidateFieldValue("default BatchSize", BatchSizeDefault, defaultBatchSize)
}

func testGetStatistics(bucket string) error {
	fmt.Println("Start testGetStatistics")
	// NOTE this API uses the xdcr internal rest port. The same api does not exist on the couchbase adminport
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, uint64(base.AdminportNumber)) + rm.StatisticsPrefix + base.UrlDelimiter + bucket
	_, err := common.SendRequestAndValidateResponse("testGetStatistics", base.MethodGet, url, nil, options.username, options.password)
	return err
}

// may need to find a better way to tell whether the pipeline is actually running
func validatePipeline(testName, replicationId, escapedReplId string, pipelineExists bool, pipelineActive bool) error {
	settingsMap, _ := getReplicationSettings(testName, escapedReplId)

	if !pipelineExists {
		if settingsMap != nil {
			return errors.New(fmt.Sprintf("Test %v failed. Pipeline, %v, should not exist but does.\n", testName, replicationId))
		}
		return nil
	} else {
		if settingsMap == nil {
			return errors.New(fmt.Sprintf("Test %v failed. Pipeline, %v, should exist but does not.\n", testName, replicationId))
		}

		// now check if pipeline is active
		pauseRequsted := settingsMap[rm.PauseRequested].(bool)
		if pauseRequsted == pipelineActive {
			var errMsg string
			if pipelineActive {
				errMsg = ", should be active but was not"
			} else {
				errMsg = ", should not be active but was"
			}
			return errors.New(fmt.Sprintf("Test %v failed. Pipeline, %v%v.\n", testName, replicationId, errMsg))
		} else {
			return nil
		}
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

func getDefaultSettings(testName string) (map[string]interface{}, error) {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath
	response, err := common.SendRequestAndValidateResponse(testName, base.MethodGet, url, nil, options.username, options.password)
	if err != nil {
		return nil, err
	}

	return decodeSettingsMapFromResponse(response)
}

func getReplicationSettings(testName, escapedReplId string) (map[string]interface{}, error) {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVAdminPort) + rm.SettingsReplicationsPath
	response, err := common.SendRequestWithEscapedIdAndValidateResponse(testName, base.MethodGet, url, escapedReplId, nil, options.username, options.password)
	fmt.Printf("url=%v, res=%v, err=%v\n", url, response, err)

	if err != nil {
		return nil, err
	}

	return decodeSettingsMapFromResponse(response)
}
