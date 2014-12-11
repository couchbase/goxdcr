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
	"io/ioutil"
	"encoding/json"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	rm "github.com/couchbase/goxdcr/replication_manager"
	s "github.com/couchbase/goxdcr/service_impl"
	utils "github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/goxdcr/tests/common"
	"net/http"
	"os"
	"time"
	"reflect"
	"strings"
)

var options struct {
	sourceKVHost string //source kv host name
	sourceKVPort      uint64 //source kv admin port

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
	flag.Uint64Var(&options.sourceKVPort, "sourceKVPort", 9000,
		"admin port number for source kv")
	flag.StringVar(&options.username, "username", "Administrator", "userName to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")
	
	flag.StringVar(&options.remoteUuid, "remoteUuid", "1234567",
		"remote cluster uuid")
	flag.StringVar(&options.remoteName, "remoteName", "remote",
		"remote cluster name")
	flag.StringVar(&options.remoteHostName, "remoteHostName", /*"127.0.0.1:9000",//*/"ec2-204-236-128-120.us-west-1.compute.amazonaws.com:8091",
		"remote cluster host name")
	flag.StringVar(&options.remoteUserName, "remoteUserName", "Administrator", "remote cluster userName")
	flag.StringVar(&options.remotePassword, "remotePassword", "welcome", "remote cluster password")
	flag.BoolVar(&options.remoteDemandEncryption, "remoteDemandEncryption", true, "whether encryption is needed")
	flag.StringVar(&options.remoteCertificateFile, "remoteCertificateFile", "/Users/yu/pem/remoteCert.pem", "file containing certificate for encryption")

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
	top_svc, err := s.NewXDCRTopologySvc(options.username, options.password, uint16(options.sourceKVPort), base.AdminportNumber, true, nil)
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
	
	rm.StartReplicationManager(options.sourceKVHost,
							   base.AdminportNumber, 
							   s.NewReplicationSpecService(metadata_svc, nil),
							   s.NewRemoteClusterService(metadata_svc, nil),	
							   s.NewClusterInfoSvc(nil),  
							   top_svc, 
							   s.NewReplicationSettingsSvc(metadata_svc, nil))
	
	//wait for server to finish starting
	time.Sleep(time.Second * 3)
	
	/*if err := testAuth(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testSSLAuth(); err != nil {
		fmt.Println(err.Error())
		return
	}*/
	
	// Uncomment if need to clean up residual test data
	/*if err := testDeleteRemoteCluster(); err != nil {
		fmt.Println(err.Error())
		return
	}*/
		
	if err := testRemoteClusters(false/*remoteClusterExpected*/); err != nil {
		fmt.Println(err.Error())
		return
	}

	if err := testCreateRemoteCluster(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testRemoteClusters(true/*remoteClusterExpected*/); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testDeleteRemoteCluster(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testRemoteClusters(false/*remoteClusterExpected*/); err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("All tests passed.")

}

func testAuth() error{
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
	
	sslPort, err := utils. GetXDCRSSLPort(options.remoteHostName, options.remoteUserName, options.remotePassword)
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


func testRemoteClusters(remoteClusterExpected bool) error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.RemoteClustersPath

	request, err := http.NewRequest(base.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	err = common.ValidateResponse("RemoteClusters", response, err)
	if err != nil {
		return err
	}
	
	// verify contents in response
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	var remoteClusters []metadata.RemoteClusterReference
	err = json.Unmarshal(bodyBytes, &remoteClusters)
	if err != nil {
		return err
	}
	
	remoteClusterExists := false
	for _, remoteCluster := range remoteClusters {
		if remoteCluster.Name == options.remoteName {
			remoteClusterExists = true
			// verify that fields of remote cluster are as expected
			err = verifyRemoteCluster(&remoteCluster)
			if err != nil {
				return err
			}
			break
		}
	}
	
	if remoteClusterExists && !remoteClusterExpected {
		return errors.New("Did not expect remote cluster to exist but it did.")
	} 
	if !remoteClusterExists && remoteClusterExpected {
		return errors.New("Expected remote cluster to exist but it did not.")
	} 
	return nil
}
	
func testCreateRemoteCluster() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.RemoteClustersPath

	params := make(map[string]interface{})
	params[rm.RemoteClusterUuid] = options.remoteUuid
	params[rm.RemoteClusterName] = options.remoteName
	params[rm.RemoteClusterHostName] = options.remoteHostName
	params[rm.RemoteClusterUserName] = options.remoteUserName
	params[rm.RemoteClusterPassword] = options.remotePassword
	params[rm.RemoteClusterDemandEncryption] = options.remoteDemandEncryption
	
	// read certificate from file
	if options.remoteCertificateFile != "" {
		serverCert, err := ioutil.ReadFile(options.remoteCertificateFile)
		if err != nil {
    		fmt.Printf("Could not load server certificate! err=%v\n", err)
    		return err
		}
		params[rm.RemoteClusterCertificate] = serverCert
	}

	paramsBytes, err := rm.EncodeMapIntoByteArray(params)
	if err != nil {
		return err
	}
	paramsBuf := bytes.NewBuffer(paramsBytes)

	request, err := http.NewRequest(base.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	response, err := http.DefaultClient.Do(request)

	err = common.ValidateResponse("CreateRemoteCluster", response, err)
	if err != nil {
		return err
	}
	return nil
}

func testDeleteRemoteCluster() error {
	url := common.GetAdminportUrlPrefix(options.sourceKVHost) + rm.RemoteClustersPath + base.UrlDelimiter + options.remoteName

	request, err := http.NewRequest(base.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(rm.ContentType, rm.DefaultContentType)

	fmt.Println("request", request)

	response, err := http.DefaultClient.Do(request)

	return common.ValidateResponse("DeleteRemoteCluster", response, err)
}

func verifyRemoteCluster(remoteCluster *metadata.RemoteClusterReference) error {
	if err := common.ValidateFieldValue(rm.RemoteClusterUuid, options.remoteUuid, remoteCluster.Uuid); err == nil {
		return errors.New("uuid is supposed to be updated to real value")
	}
	if err := common.ValidateFieldValue(rm.RemoteClusterHostName, options.remoteHostName, remoteCluster.HostName); err != nil {
		return err
	}
	if err := common.ValidateFieldValue(rm.RemoteClusterUserName, options.remoteUserName, remoteCluster.UserName); err != nil {
		return err
	}
	if err := common.ValidateFieldValue(rm.RemoteClusterPassword, options.remotePassword, remoteCluster.Password); err != nil {
		return err
	}
	if err := common.ValidateFieldValue(rm.RemoteClusterDemandEncryption, options.remoteDemandEncryption, remoteCluster.DemandEncryption); err != nil {
		return err
	}
	return nil
}