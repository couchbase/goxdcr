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
	"encoding/json"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
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

func (service *RemoteClusterService) RemoteCluster(refId string) (*metadata.RemoteClusterReference, error) {
	result, err := service.metadata_svc.Get(refId)
	if err != nil {
		return nil, err
	}
	var ref = &metadata.RemoteClusterReference{}
	err = json.Unmarshal(result, ref) 
	return ref, err
}

// this assumes that the ref to be added is not yet in gometa
func (service *RemoteClusterService) AddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Adding remote cluster with referenceId %v\n", ref.Id)
	
	key := ref.Id
	
	// add key to catalog first
	err := AddKeyToCatalog(key, RemoteClustersCatalogKey, service.metadata_svc)
	if err != nil {
		return err
	}
	
	value, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	service.logger.Debugf("Remote cluster being added: key=%v, value=%v\n", key, string(value))
	return service.metadata_svc.Add(key, value)
}

func (service *RemoteClusterService) DelRemoteCluster(refId string) error {
	service.logger.Infof("Deleting remote cluster with referenceId %v\n", refId)
	
	err := service.metadata_svc.Del(refId)
	if err != nil {
		return err
	}
	
	// remove key from catalog
	return RemoveKeyFromCatalog(refId, RemoteClustersCatalogKey, service.metadata_svc)
}

func (service *RemoteClusterService) RemoteClusters() (map[string]*metadata.RemoteClusterReference, error) {
	service.logger.Infof("Getting remote clusters")
	
	refs := make(map[string]*metadata.RemoteClusterReference, 0)

	keys, err := GetKeysFromCatalog(RemoteClustersCatalogKey, service.metadata_svc)	
	service.logger.Debugf("keys for remote clusters %v\n", keys)
	if err != nil {
		return nil, err
	}
	
	if keys != nil {
		for _, key := range keys {
			// ignore error. it is ok for some keys in catalog to be invalid
			ref, _ := service.RemoteCluster(key)
			if ref != nil {
				refs[key] = ref
			}
		}
	}

	return refs, nil
}