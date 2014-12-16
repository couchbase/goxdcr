// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the refific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package service_impl

import (
	"errors"
	"fmt"
	"encoding/json"
	"io/ioutil"
	"reflect"
	"net/http"
	"strings"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	rm "github.com/couchbase/goxdcr/replication_manager"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	RemoteClustersCatalogKey = "remoteClustersCatalog"
)

type RemoteClusterService struct {
	metadata_svc  service_def.MetadataSvc
	logger      *log.CommonLogger
}

func NewRemoteClusterService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *RemoteClusterService {
	return &RemoteClusterService{
					metadata_svc:  metadata_svc,  
					logger:    log.NewLogger("RemoteClusterService", logger_ctx),
		}
}

func (service *RemoteClusterService) RemoteClusterByRefId(refId string) (*metadata.RemoteClusterReference, error) {
	result, err := service.metadata_svc.Get(refId)
	if err != nil {
		return nil, err
	}
	var ref = &metadata.RemoteClusterReference{}
	err = json.Unmarshal(result, ref) 
	return ref, err
}

func (service *RemoteClusterService) RemoteClusterByUuid(uuid string) (*metadata.RemoteClusterReference, error) {
	return service.RemoteClusterByRefId(metadata.RemoteClusterRefId(uuid))
}

func (service *RemoteClusterService) RemoteClusterByRefName(refName string) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference   
	results, err := service.RemoteClusters()
	if err != nil {
		return nil, err
	}
	
	for _, result := range results {
		if result.Name == refName {
			ref = result
			break
		}
	}
	
	if ref == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find remote reference with name=%v\n", refName))
	} else {
		return ref, nil
	}
}

// this assumes that the ref to be added is not yet in gometa
func (service *RemoteClusterService) AddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Adding remote cluster with referenceId %v\n", ref.Id)
	
	err := service.ValidateRemoteCluster(ref)
	if err != nil {
		return err
	}
	
	return service.addRemoteCluster(ref)
}

// this assumes that the ref to be added is not yet in gometa
func (service *RemoteClusterService) SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Setting remote cluster with refName %v\n", refName)
	
	err := service.ValidateRemoteCluster(ref)
	if err != nil {
		return err
	}
	
	oldRef, err := service.RemoteClusterByRefName(refName)
	if err != nil {
		return err
	}
	
	if ref.Id == oldRef.Id {
		// if the id of the remote cluster reference has not been changed, simply update the existing reference
		key := ref.Id
		value, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		service.logger.Debugf("Remote cluster being changed: key=%v, value=%v\n", key, string(value))
		return service.metadata_svc.Set(key, value)
	} else {
		// if id of the remote cluster reference has been changed, delete the existing reference and create a new one
		err = service.metadata_svc.DelWithCatalog(RemoteClustersCatalogKey, oldRef.Id)
		if err != nil {
			return err
		}
		return service.addRemoteCluster(ref)	
	}
	
}

func (service *RemoteClusterService) DelRemoteCluster(refName string) error {
	service.logger.Infof("Deleting remote cluster with reference name=%v\n", refName)
	
	ref, err := service.RemoteClusterByRefName(refName)
	if err != nil {
		return err
	}
	
	key := ref.Id
	
	return service.metadata_svc.DelWithCatalog(RemoteClustersCatalogKey, key)
}

func (service *RemoteClusterService) RemoteClusters() (map[string]*metadata.RemoteClusterReference, error) {
	service.logger.Infof("Getting remote clusters")
	
	refs := make(map[string]*metadata.RemoteClusterReference, 0)

	keys, err := service.metadata_svc.GetKeysFromCatalog(RemoteClustersCatalogKey)	
	service.logger.Debugf("keys for remote clusters %v\n", keys)
	if err != nil {
		return nil, err
	}
	
	if keys != nil {
		for _, key := range keys {
			// ignore error. it is ok for some keys in catalog to be invalid
			ref, _ := service.RemoteClusterByRefId(key)
			if ref != nil {
				refs[key] = ref
			}
		}
	}

	return refs, nil
}

// validate remote cluster info and retrieve actual uuid 
func (service *RemoteClusterService) ValidateRemoteCluster(ref *metadata.RemoteClusterReference) error {

	isEnterprise, err := rm.XDCRCompTopologyService().IsMyClusterEnterprise()
	if err != nil {
		return err
	}		
	if ref.DemandEncryption && !isEnterprise {
		return errors.New("Encryption can only be used in enterprise edition.")
	}
		
	var response *http.Response
	if ref.DemandEncryption {
		response, err = connectToRemoteClusterThroughHttps(ref.HostName, ref.UserName, ref.Password, ref.Certificate)
	} else {
		response, err = connectToRemoteClusterThroughHttp(ref.HostName, ref.UserName, ref.Password)
	}
	if err != nil {
		return err
	}
	
	// verify contents in response
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	// xxx/pools returns a map object
	var poolsInfo map[string]interface{}
	err = json.Unmarshal(bodyBytes, &poolsInfo)
	if err != nil {
		return err
	}
	
	// get remote cluster uuid from the map 
	actualUuid, ok := poolsInfo[rm.RemoteClusterUuid]
	if !ok {
		// should never get here
		return errors.New("Could not get uuid of remote cluster.")
	}
	
	actualUuidStr, ok := actualUuid.(string)
	if !ok {
		// should never get here
		return errors.New(fmt.Sprintf("uuid of remote cluster is of wrong type. Expected type: string; Actual type: %s", reflect.TypeOf(actualUuid)))
	}
	
	// update uuid in ref to real value
	ref.Uuid = actualUuidStr
	ref.Id = metadata.RemoteClusterRefId(ref.Uuid)
	
	return nil
}

func connectToRemoteClusterThroughHttp(hostName, userName, password string) (*http.Response, error) {
	url := fmt.Sprintf("http://%s:%s@%s%s", userName, password, hostName, base.PoolsPath)
	request, err := http.NewRequest(base.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return utils.SendHttpRequest(request)
}

func connectToRemoteClusterThroughHttps(hostName, userName, password string, certificate []byte) (*http.Response, error) {
	sslPort, err := utils.GetXDCRSSLPort(hostName, userName, password)
	if err != nil {
		return nil, err
	}
	
	hostNode := strings.Split(hostName, base.UrlPortNumberDelimiter)[0]
	newHostName := utils.GetHostAddr(hostNode, sslPort)
	url := fmt.Sprintf("https://%s:%s@%s%s", userName, password, newHostName, base.PoolsPath)
	request, err := http.NewRequest(base.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return utils.SendHttpRequestThroughSSL(request, certificate) 
}

// this internal api differs from AddRemoteCluster in that it does not perform validation
func (service *RemoteClusterService) addRemoteCluster(ref *metadata.RemoteClusterReference) error {
	
	key := ref.Id
	value, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	service.logger.Debugf("Remote cluster being added: key=%v, value=%v\n", key, string(value))
	return service.metadata_svc.AddWithCatalog(RemoteClustersCatalogKey, key, value)
}
