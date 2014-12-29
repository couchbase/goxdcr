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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	rm "github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/tests/common"
	utils "github.com/couchbase/goxdcr/utils"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
)

var options struct {
	sourceKVHost string //source kv host name
	sourceKVPort uint64 //source kv admin port
	
	username string //username
	password string //password

	// parameters of remote cluster
	remoteUuid             string // remote cluster uuid
	remoteName             string // remote cluster name
	remoteHostName         string // remote cluster host name
	remoteUserName         string //remote cluster userName
	remotePassword         string //remote cluster password
	remoteDemandEncryption bool   // whether encryption is needed
	remoteCertificateFile  string // file containing certificate for encryption

	newRemoteName     string // new remote cluster name
	newRemoteHostName string // new remote cluster host name
}

func argParse() {
	flag.StringVar(&options.sourceKVHost, "sourceKVHost", base.LocalHostName,
		"source kv host")
	flag.Uint64Var(&options.sourceKVPort, "sourceKVPort", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.username, "username", "Administrator", "userName to cluster admin console")
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

	flag.StringVar(&options.newRemoteName, "newRemoteName", "newRemote",
		"remote cluster name")
	// This is for testChangeRemoteClusterWithIdChange(). This needs to be manually changed to point to a valid non-local cluster for the test to work
	flag.StringVar(&options.newRemoteHostName, "newRemoteHostName", "", //"ec2-204-236-128-120.us-west-1.compute.amazonaws.com:8091",
		"remote cluster host name")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing adminport...")
	argParse()
	startAdminport()
}

func startAdminport() {

	// verify that tests start from a clean slate
	if _, err := getRemoteClusterAndVerifyExistence("test set up", options.remoteName, false); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testCreateRemoteClusterWithJustValidate(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testCreateRemoteCluster(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testChangeRemoteClusterWithJustValidate(); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testChangeRemoteClusterWithoutIdChange(); err != nil {
		fmt.Println(err.Error())
		return
	}

	// Note, this test does not work by default.
	// To make it work, modify options.newRemoteName to point to a valid non-local cluster.
	/*if err := testChangeRemoteClusterWithIdChange(); err != nil {
		fmt.Println(err.Error())
		return
	}*/

	if err := testDeleteRemoteCluster(options.newRemoteName); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("All tests passed.")

}

func testAuth() error {
	url := fmt.Sprintf("http://%s:%s@%s/pools", options.remoteUserName, options.remotePassword, options.remoteHostName)
	fmt.Printf("url=%v\n", url)
	request, err := http.NewRequest(base.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	fmt.Printf("response=%v\n", response)

	// verify contents in response
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	fmt.Printf("body=%v\n", bodyBytes)

	var v map[string]interface{}
	err = json.Unmarshal(bodyBytes, &v)
	fmt.Printf("v=%v, v.type=%v, err=%v\n", v, reflect.TypeOf(v), err)

	uuid, ok := v["uuid"]
	fmt.Printf("uuid=%v, ok=%v\n", uuid, ok)
	return nil
}

func testSSLAuth() error {

	serverCert, err := ioutil.ReadFile(options.remoteCertificateFile)
	if err != nil {
		fmt.Printf("Could not load server certificate! err=%v\n", err)
		return err
	}

	sslPort, err := utils.GetXDCRSSLPort(options.remoteHostName, options.remoteUserName, options.remotePassword)
	if err != nil {
		return err
	}

	hostNode := strings.Split(options.remoteHostName, base.UrlPortNumberDelimiter)[0]
	newHostName := utils.GetHostAddr(hostNode, sslPort)
	url := fmt.Sprintf("https://%s:%s@%s%s", options.remoteUserName, options.remotePassword, newHostName, base.PoolsPath)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	fmt.Printf("url=%v\n", url)
	response, err := utils.SendHttpRequestThroughSSL(request, serverCert)

	if err != nil {
		return err
	}
	// verify contents in response
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	fmt.Printf("body=%v\n", bodyBytes)

	var v map[string]interface{}
	err = json.Unmarshal(bodyBytes, &v)
	fmt.Printf("v=%v, v.type=%v, err=%v\n", v, reflect.TypeOf(v), err)

	uuid, ok := v["uuid"]
	fmt.Printf("uuid=%v, ok=%v\n", uuid, ok)
	return nil
}

// GetRemoteCluster by calling RemoteClusters() API.
func getRemoteCluster(testName, remoteClusterName string) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath
	response, err := common.SendRequestAndValidateResponse(testName, base.MethodGet, url, nil, options.username, options.password)
	if err != nil {
		return nil, err
	}

	// verify contents in response
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var remoteClusters []metadata.RemoteClusterReference
	err = json.Unmarshal(bodyBytes, &remoteClusters)
	if err != nil {
		return nil, err
	}

	for _, remoteCluster := range remoteClusters {
		if remoteCluster.Name == remoteClusterName {
			ref = &remoteCluster
			break
		}
	}

	return ref, nil
}

func getRemoteClusterAndVerifyExistence(testName, remoteClusterName string, expectedToExist bool) (*metadata.RemoteClusterReference, error) {
	ref, err := getRemoteCluster(testName, remoteClusterName)
	if err != nil {
		return nil, err
	}

	if !expectedToExist && ref != nil {
		return nil, errors.New("Did not expect remote cluster to exist but it did.")
	}
	if expectedToExist && ref == nil {
		return nil, errors.New("Expected remote cluster to exist but it did not.")
	}

	return ref, nil
}

func testCreateRemoteClusterWithJustValidate() error {
	fmt.Println("Starting testCreateRemoteClusterWithJustValidate")
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath + base.JustValidatePostfix
	paramsBytes, err := createRequestBody(options.remoteName, options.remoteHostName, options.remoteUserName,
		options.remotePassword, options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		return err
	}
	_, err = common.SendRequestAndValidateResponse("testCreateRemoteClusterWithJustValidate", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	_, err = getRemoteClusterAndVerifyExistence("testCreateRemoteClusterWithJustValidate", options.remoteName, false)
	return err
}

func testCreateRemoteCluster() error {
	fmt.Println("Starting testCreateRemoteCluster")
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath
	paramsBytes, err := createRequestBody(options.remoteName, options.remoteHostName, options.remoteUserName,
		options.remotePassword, options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		return err
	}
	_, err = common.SendRequestAndValidateResponse("testCreateRemoteCluster", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	ref, err := getRemoteClusterAndVerifyExistence("testCreateRemoteCluster", options.remoteName, true)
	if err != nil {
		return err
	}

	return verifyRemoteClusterWithoutId(ref, options.remoteName, options.remoteHostName, options.remoteUserName, options.remotePassword, options.remoteDemandEncryption)
}

func testChangeRemoteClusterWithJustValidate() error {
	fmt.Println("Starting testChangeRemoteClusterWithJustValidate")

	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath + base.UrlDelimiter + options.remoteName + base.JustValidatePostfix
	paramsBytes, err := createRequestBody(options.newRemoteName, options.remoteHostName, options.remoteUserName,
		options.remotePassword, options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		return err
	}
	_, err = common.SendRequestAndValidateResponse("testChangeRemoteClusterWithJustValidate", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	_, err = getRemoteClusterAndVerifyExistence("testChangeRemoteClusterWithJustValidate", options.remoteName, true)
	if err != nil {
		return err
	}

	_, err = getRemoteClusterAndVerifyExistence("testChangeRemoteClusterWithJustValidate", options.newRemoteName, false)
	return err
}

// change name of remote cluster, which does not lead to id change of the corresponding reference
func testChangeRemoteClusterWithoutIdChange() error {
	fmt.Println("Starting testChangeRemoteClusterWithoutIdChange")
	oldRef, err := getRemoteCluster("testChangeRemoteClusterWithoutIdChange", options.remoteName)
	if err != nil {
		return err
	}

	oldRefId := oldRef.Id

	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath + base.UrlDelimiter + options.remoteName
	paramsBytes, err := createRequestBody(options.newRemoteName, options.remoteHostName, options.remoteUserName,
		options.remotePassword, options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		return err
	}
	_, err = common.SendRequestAndValidateResponse("testChangeRemoteClusterWithoutIdChange", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	ref, err := getRemoteClusterAndVerifyExistence("testChangeRemoteClusterWithoutIdChange", options.newRemoteName, true)
	if err != nil {
		return err
	}

	// verify that ref id stay the same
	err = verifyRemoteClusterId(ref, oldRefId, true /*sameIdExpected*/)
	if err != nil {
		return err
	}

	// verify other cluster fields
	return verifyRemoteClusterWithoutId(ref, options.newRemoteName, options.remoteHostName, options.remoteUserName, options.remotePassword, options.remoteDemandEncryption)
}

// change hostname of remote cluster, which should lead to id change of the corresponding reference
func testChangeRemoteClusterWithIdChange() error {
	fmt.Println("Starting testChangeRemoteClusterWithIdChange")
	oldRef, err := getRemoteCluster("testChangeRemoteClusterWithIdChange", options.newRemoteName)
	if err != nil {
		return err
	}

	oldRefId := oldRef.Id

	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath + base.UrlDelimiter + options.newRemoteName
	paramsBytes, err := createRequestBody(options.newRemoteName, options.newRemoteHostName, options.remoteUserName,
		options.remotePassword, options.remoteDemandEncryption, options.remoteCertificateFile)
	if err != nil {
		return err
	}
	_, err = common.SendRequestAndValidateResponse("testChangeRemoteClusterWithIdChange", base.MethodPost, url, paramsBytes, options.username, options.password)
	if err != nil {
		return err
	}

	ref, err := getRemoteClusterAndVerifyExistence("testChangeRemoteClusterWithIdChange", options.newRemoteName, true)
	if err != nil {
		return err
	}

	// verify that ref id changed
	err = verifyRemoteClusterId(ref, oldRefId, false /*sameIdExpected*/)
	if err != nil {
		return err
	}

	// verify other cluster fields
	return verifyRemoteClusterWithoutId(ref, options.newRemoteName, options.remoteHostName, options.remoteUserName, options.remotePassword, options.remoteDemandEncryption)
}

func testDeleteRemoteCluster(remoteName string) error {
	fmt.Println("Starting testDeleteRemoteCluster")
	url := common.GetAdminportUrlPrefix(options.sourceKVHost, options.sourceKVPort) + base.RemoteClustersPath + base.UrlDelimiter + remoteName

	_, err := common.SendRequestAndValidateResponse("testDeleteRemoteCluster", base.MethodDelete, url, nil, options.username, options.password)
	if err != nil {
		return err
	}

	_, err = getRemoteClusterAndVerifyExistence("testDeleteRemoteCluster", remoteName, false)
	return err
}

func verifyRemoteClusterWithoutId(remoteCluster *metadata.RemoteClusterReference, name, hostname, username, password string, demandEncryption bool) error {
	if err := common.ValidateFieldValue(base.RemoteClusterUuid, name, remoteCluster.Name); err == nil {
		return err
	}

	if err := common.ValidateFieldValue(base.RemoteClusterHostName, hostname, remoteCluster.HostName); err != nil {
		return err
	}

	if err := common.ValidateFieldValue(base.RemoteClusterUserName, username, remoteCluster.UserName); err != nil {
		return err
	}

	if err := common.ValidateFieldValue(base.RemoteClusterPassword, password, remoteCluster.Password); err != nil {
		return err
	}

	if err := common.ValidateFieldValue(base.RemoteClusterDemandEncryption, demandEncryption, remoteCluster.DemandEncryption); err != nil {
		return err
	}

	return nil
}

func verifyRemoteClusterId(remoteCluster *metadata.RemoteClusterReference, id string, sameIdExpected bool) error {
	if err := common.ValidateFieldValue(base.RemoteClusterUuid, id, remoteCluster.Id); sameIdExpected != (err == nil) {
		return errors.New("id validation failed")
	}
	return nil
}

func createRequestBody(name, hostname, username, password string, demandEncryption bool, certificateFile string) ([]byte, error) {

	params := make(map[string]interface{})
	params[base.RemoteClusterName] = name
	params[base.RemoteClusterHostName] = hostname
	params[base.RemoteClusterUserName] = username
	params[base.RemoteClusterPassword] = password
	params[base.RemoteClusterDemandEncryption] = demandEncryption

	// read certificate from file
	if certificateFile != "" {
		serverCert, err := ioutil.ReadFile(certificateFile)
		if err != nil {
			fmt.Printf("Could not load server certificate! err=%v\n", err)
			return nil, err
		}
		params[base.RemoteClusterCertificate] = serverCert
	}
	return rm.EncodeMapIntoByteArray(params)
}
