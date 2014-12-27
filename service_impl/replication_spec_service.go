// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package service_impl

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

const (
	// the key to the metadata that stores the keys of all Replication Specs
	ReplicationSpecsCatalogKey = "replicationSpecsCatalog"
)

type ReplicationSpecService struct {
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger
}

func NewReplicationSpecService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) *ReplicationSpecService {
	return &ReplicationSpecService{
		metadata_svc: metadata_svc,
		logger:       log.NewLogger("ReplicationSpecService", logger_ctx),
	}
}

func (service *ReplicationSpecService) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	result, err := service.metadata_svc.Get(replicationId)
	if err != nil {
		return nil, err
	}
	var spec = &metadata.ReplicationSpecification{}
	err = json.Unmarshal(result, spec)
	return spec, err
}

// this assumes that the spec to be added is not yet in gometa
func (service *ReplicationSpecService) AddReplicationSpec(spec *metadata.ReplicationSpecification) error {
	key := spec.Id
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return service.metadata_svc.AddWithCatalog(ReplicationSpecsCatalogKey, key, value)
}

func (service *ReplicationSpecService) SetReplicationSpec(spec *metadata.ReplicationSpecification) error {
	key := spec.Id
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return service.metadata_svc.Set(key, value)
}

func (service *ReplicationSpecService) DelReplicationSpec(replicationId string) error {
	return service.metadata_svc.DelWithCatalog(ReplicationSpecsCatalogKey, replicationId)
}

func (service *ReplicationSpecService) ActiveReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)

	keys, err := service.metadata_svc.GetKeysFromCatalog(ReplicationSpecsCatalogKey)	
	if err != nil {
		return nil, err
	}

	if keys != nil {
		service.logger.Infof("active replications =%v\n", keys)
		for _, key := range keys {
			// ignore error. it is ok for some keys in catalog to be invalid
			spec, _ := service.ReplicationSpec(key)
			if spec != nil {
				specs[spec.Id] = spec
			}
		}
	}

	return specs, nil
}

func (service *ReplicationSpecService) ActiveReplicationSpecIdsForBucket(bucket string) ([]string, error) {
	var repIds []string
	keys, err := service.metadata_svc.GetKeysFromCatalog(ReplicationSpecsCatalogKey)
	if err != nil {
		return nil, err
	}

	service.logger.Infof("keys=%v", keys)
	
	if keys != nil {
		for _, key := range keys {
			if metadata.IsReplicationIdForSourceBucket(key, bucket) {
				repIds = append(repIds, key)
			}
		}
	}
	return repIds, nil
}
