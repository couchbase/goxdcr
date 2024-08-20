/*
Copyright 2014-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package common

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
	"io/ioutil"
	"net/http"
	"reflect"
)

func GetAdminportUrlPrefix(hostName string, adminportNumber uint64) string {
	return "http://" + base.GetHostAddr(hostName, uint16(adminportNumber)) + base.AdminportUrlPrefix
}

func ValidateResponse(testName string, response *http.Response, err error) error {
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

func ValidateFieldValue(fieldName string, expectedValue, actualValue interface{}) error {
	if expectedValue != actualValue {
		return errors.New(fmt.Sprintf("Incorrect value in field %v. Expected value=%v with type=%v, actual value=%v with type=%v\n",
			fieldName, expectedValue, reflect.TypeOf(expectedValue), actualValue, reflect.TypeOf(actualValue)))
	}
	return nil
}

func CreateTestRemoteCluster(remote_cluster_service service_def.RemoteClusterSvc, remoteUuid, remoteName, remoteHostName, remoteUserName, remotePassword string,
	remoteDemandEncryption uint64, remoteCertificateFile string) error {
	var serverCert []byte
	var err error
	// read certificate from file
	if remoteCertificateFile != "" {
		serverCert, err = ioutil.ReadFile(remoteCertificateFile)
		if err != nil {
			fmt.Printf("Could not load server certificate! err=%v\n", err)
			return err
		}
	}

	remoteClusterRef, err := metadata.NewRemoteClusterReference(remoteUuid, remoteName, remoteHostName, remoteUserName, remotePassword, "", remoteDemandEncryption != 0, metadata.EncryptionType_Full, serverCert)
	if err != nil {
		return err
	}

	err = remote_cluster_service.AddRemoteCluster(remoteClusterRef, false /*skipConnectivityValidation*/)
	fmt.Printf("Added remote cluster reference with name=%v, remoteDemandEncryption=%v, err=%v\n", remoteName, remoteDemandEncryption != 0, err)
	return err
}

func DeleteTestRemoteCluster(remote_cluster_service service_def.RemoteClusterSvc, remoteName string) error {
	_, err := remote_cluster_service.DelRemoteCluster(remoteName)
	fmt.Printf("Deleted remote cluster reference with name=%v, err=%v\n", remoteName, err)
	return err
}

func SendRequestAndValidateResponse(testName, httpMethod, url string, body []byte, username, password string) (*http.Response, error) {
	request, err := constructRequest(httpMethod, url, body)
	if err != nil {
		return nil, err
	}

	// set auth parameters
	request.SetBasicAuth(username, password)

	return sendRequest(testName, request)
}

func SendRequestWithEscapedIdAndValidateResponse(testName, httpMethod, url, escapedId string, body []byte, username, password string) (*http.Response, error) {
	request, err := constructRequest(httpMethod, url, body)
	if err != nil {
		return nil, err
	}

	// golang does not provide APIs to construct Request or URL with escaped path. Has to do this as a workaround
	request.URL.Path += base.UrlDelimiter + escapedId

	// set auth parameters
	request.SetBasicAuth(username, password)

	return sendRequest(testName, request)
}

func constructRequest(httpMethod, url string, body []byte) (*http.Request, error) {
	request, err := http.NewRequest(httpMethod, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	request.Close = true
	request.Header.Set(base.ContentType, base.DefaultContentType)
	return request, nil
}

func sendRequest(testName string, request *http.Request) (*http.Response, error) {
	response, err := http.DefaultClient.Do(request)
	err = ValidateResponse(testName, response, err)
	return response, err
}

func CreateTestRemoteClusterThroughRest(sourceKVHost string, adminport uint64, remoteUuid, remoteName, remoteHostName, remoteUserName, remotePassword string,
	remoteDemandEncryption uint64, remoteCertificateFile, username, password string) error {
	url := GetAdminportUrlPrefix(sourceKVHost, adminport) + base.RemoteClustersPath

	params := make(map[string]interface{})
	params[base.RemoteClusterUuid] = remoteUuid
	params[base.RemoteClusterName] = remoteName
	params[base.RemoteClusterHostName] = remoteHostName
	params[base.RemoteClusterUserName] = remoteUserName
	params[base.RemoteClusterPassword] = remotePassword
	params[base.RemoteClusterDemandEncryption] = int(remoteDemandEncryption)

	// read certificate from file
	if remoteCertificateFile != "" {
		serverCert, err := ioutil.ReadFile(remoteCertificateFile)
		if err != nil {
			fmt.Printf("Could not load server certificate! err=%v\n", err)
			return err
		}
		params[base.RemoteClusterCertificate] = serverCert
	}

	paramsBytes, err := utils.EncodeMapIntoByteArray(params)
	if err != nil {
		return err
	}

	_, err = SendRequestAndValidateResponse("CreateTestRemoteClusterThroughRest", base.MethodPost, url, paramsBytes, username, password)
	return err
}

func DeleteTestRemoteClusterThroughRest(sourceKVHost string, adminport uint64, remoteName string, username, password string) error {
	url := GetAdminportUrlPrefix(sourceKVHost, adminport) + base.RemoteClustersPath + base.UrlDelimiter + remoteName

	_, err := SendRequestAndValidateResponse("DeleteTestRemoteClusterThroughRest", base.MethodDelete, url, nil, username, password)
	return err
}
