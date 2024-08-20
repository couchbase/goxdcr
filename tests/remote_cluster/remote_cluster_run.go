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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/tests/common"
	"github.com/couchbase/goxdcr/v8/utils"
	"io/ioutil"
	"os"
)

var logger *log.CommonLogger = log.NewLogger("remote_cluster_run", log.DefaultLoggerContext)

var options struct {
	sourceKVHost string //source kv host name
	sourceKVPort uint64 //source kv admin port
	username     string //username
	password     string //password

	// parameters of remote cluster
	remoteUuid             string // remote cluster uuid
	remoteName             string // remote cluster name
	remoteHostName         string // remote cluster host name
	remoteUserName         string //remote cluster userName
	remotePassword         string //remote cluster password
	remoteDemandEncryption uint64 // whether encryption is needed
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
	flag.Uint64Var(&options.remoteDemandEncryption, "remoteDemandEncryption", 0, "whether encryption is needed")
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
	fmt.Println("Start Testing remote_cluster_run...")
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

func verifyRemoteClusterWithoutId(remoteCluster *metadata.RemoteClusterReference, name, hostname, username, password string, demandEncryption uint64) error {
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

	if err := common.ValidateFieldValue(base.RemoteClusterDemandEncryption, demandEncryption != 0, remoteCluster.DemandEncryption); err != nil {
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

func createRequestBody(name, hostname, username, password string, demandEncryption uint64, certificateFile string) ([]byte, error) {

	params := make(map[string]interface{})
	params[base.RemoteClusterName] = name
	params[base.RemoteClusterHostName] = hostname
	params[base.RemoteClusterUserName] = username
	params[base.RemoteClusterPassword] = password
	params[base.RemoteClusterDemandEncryption] = int(demandEncryption)

	// read certificate from file
	if certificateFile != "" {
		serverCert, err := ioutil.ReadFile(certificateFile)
		if err != nil {
			fmt.Printf("Could not load server certificate! err=%v\n", err)
			return nil, err
		}
		params[base.RemoteClusterCertificate] = serverCert
	}
	return utils.EncodeMapIntoByteArray(params)
}
