package common

import (
	"net/http"
	"fmt"
	"errors"
	"io/ioutil"
	"reflect"
	"bytes"
	rm "github.com/couchbase/goxdcr/replication_manager"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

func GetAdminportUrlPrefix(hostName string, adminportNumber uint64) string {
	return "http://" + utils.GetHostAddr(hostName, uint16(adminportNumber)) + base.AdminportUrlPrefix
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
                             remoteDemandEncryption bool, remoteCertificateFile string) error {
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
	
	remoteClusterRef := metadata.NewRemoteClusterReference(remoteUuid, remoteName, remoteHostName, remoteUserName, remotePassword, remoteDemandEncryption, serverCert)
	err = remote_cluster_service.AddRemoteCluster(remoteClusterRef)
	fmt.Printf("Added remote cluster reference with name=%v, err=%v\n", remoteName, err)
	return err
}

func DeleteTestRemoteCluster(remote_cluster_service service_def.RemoteClusterSvc, remoteName string) error {
	err := remote_cluster_service.DelRemoteCluster(remoteName)
	fmt.Printf("Deleted remote cluster reference with name=%v, err=%v\n", remoteName, err)
	return err
}

func SendRequestAndValidateResponse(testName, httpMethod, urlStr string, body []byte, username, password string) (*http.Response, error) {
	request, err := http.NewRequest(httpMethod, urlStr, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}	
	
	request.Header.Set(rm.ContentType, rm.DefaultContentType)
	request.SetBasicAuth(username, password)
	//fmt.Printf("request=%v, url=%v\n", request, request.URL)
	response, err := http.DefaultClient.Do(request)

	err = ValidateResponse(testName, response, err)
	//fmt.Printf("err=%v, response=%v\n", err, response)
	return response, err
}

func SendRequestWithEscapedIdAndValidateResponse(testName, httpMethod, urlStr, escapedId string, body []byte, username, password string) (*http.Response, error) {
	request, err := http.NewRequest(httpMethod, urlStr, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	
	// golang does not provide APIs to construct Request or URL with escaped path. Has to do this as a workaround
	request.URL.Path += base.UrlDelimiter + escapedId	
	
	request.Header.Set(rm.ContentType, rm.DefaultContentType)
	request.SetBasicAuth(username, password)
	//fmt.Printf("request=%v, url=%v\n", request, request.URL)
	response, err := http.DefaultClient.Do(request)

	err = ValidateResponse(testName, response, err)
	//fmt.Printf("err=%v, response=%v\n", err, response)
	return response, err
}

func CreateTestRemoteClusterThroughRest(sourceKVHost string, adminport uint64, username, password, remoteUuid, remoteName, remoteHostName, remoteUserName, remotePassword string, 
                             remoteDemandEncryption bool, remoteCertificateFile string) error {
	url := GetAdminportUrlPrefix(sourceKVHost, adminport) + base.RemoteClustersPath

	params := make(map[string]interface{})
	params[base.RemoteClusterUuid] = remoteUuid
	params[base.RemoteClusterName] = remoteName
	params[base.RemoteClusterHostName] = remoteHostName
	params[base.RemoteClusterUserName] = remoteUserName
	params[base.RemoteClusterPassword] = remotePassword
	params[base.RemoteClusterDemandEncryption] = remoteDemandEncryption
	
	// read certificate from file
	if remoteCertificateFile != "" {
		serverCert, err := ioutil.ReadFile(remoteCertificateFile)
		if err != nil {
    		fmt.Printf("Could not load server certificate! err=%v\n", err)
    		return err
		}
		params[base.RemoteClusterCertificate] = serverCert
	}

	paramsBytes, err := rm.EncodeMapIntoByteArray(params)
	if err != nil {
		return err
	}
	
	_, err = SendRequestAndValidateResponse("CreateTestRemoteClusterThroughRest", base.MethodPost, url, paramsBytes, username, password)
	return err
}

func DeleteTestRemoteClusterThroughRest(sourceKVHost string, adminport uint64,  username, password, remoteName string) error {
	url := GetAdminportUrlPrefix(sourceKVHost, adminport) + base.RemoteClustersPath + base.UrlDelimiter + remoteName
	
	_, err := SendRequestAndValidateResponse("DeleteTestRemoteClusterThroughRest", base.MethodDelete, url, nil, username, password)
	return err
}
