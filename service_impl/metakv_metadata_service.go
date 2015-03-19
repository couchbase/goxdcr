// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging metakv
package service_impl

import (
	"fmt"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"strings"
	"time"
)

type MetaKVMetadataSvc struct {
	logger *log.CommonLogger
}

func NewMetaKVMetadataSvc(logger_ctx *log.LoggerContext) (*MetaKVMetadataSvc, error) {
	return &MetaKVMetadataSvc{
		logger: log.NewLogger("MetadataService", logger_ctx),
	}, nil
}

//Wrap metakv.Get with retries
//if the key is not found in metakv, return nil, nil, service_def.MetadataNotFoundErr
//if metakv operation failed after max number of retries, return nil, nil, service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) Get(key string) ([]byte, interface{}, error) {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to get %v to metakv, retried =%v\n", time.Since(start_time).Seconds(), key, i)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		value, rev, err := metakv.Get(getPathFromKey(key))
		if value == nil && rev == nil && err == nil {
			meta_svc.logger.Infof("Can't find key=%v", key)	
			return nil, nil, service_def.MetadataNotFoundErr
		} else if err == nil {
			return value, rev, nil
		} else {
			meta_svc.logger.Errorf("metakv.Get failed. path=%v, err=%v, num_of_retry=%v", getPathFromKey(key), err, i)
		}
	}

	return nil, nil, service_def.MetaKVFailedAfterMaxTries
}

//Wrap metakv.Add with retries
//if the key is already exist in metakv, return service_def.ErrorKeyAlreadyExist
//if metakv operation failed after max number of retries, return service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) Add(key string, value []byte) error {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to add %v to metakv, retried=%v\n", time.Since(start_time).Seconds(), key, i)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		err := metakv.Add(getPathFromKey(key), value)
		if err == metakv.ErrRevMismatch {
			return service_def.ErrorKeyAlreadyExist
		} else if err == nil {
			return nil
		} else {
			meta_svc.logger.Errorf("metakv.Add failed. key=%v, value=%v, err=%v, num_of_retry=%v\n", key, value, err, i)
		}
	}
	return service_def.MetaKVFailedAfterMaxTries
}

func (meta_svc *MetaKVMetadataSvc) AddWithCatalog(catalogKey, key string, value []byte) error {
	// ignore catalogKey
	return meta_svc.Add(key, value)
}

//Wrap metakv.Set with retries
//if the rev provided doesn't match with the rev metakv has, return service_def.ErrorRevisionMismatch
//if metakv operation failed after max number of retries, return service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) Set(key string, value []byte, rev interface{}) error {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to set %v to metakv, retried=%v\n", time.Since(start_time).Seconds(), key, i)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		err := metakv.Set(getPathFromKey(key), value, rev)
		if err == metakv.ErrRevMismatch {
			return service_def.ErrorRevisionMismatch
		} else if err == nil {
			return nil
		} else {
			meta_svc.logger.Errorf("metakv.Set failed. key=%v, value=%v, err=%v, num_of_retry=%v\n", key, value, err, i)
		}
	}
	return service_def.MetaKVFailedAfterMaxTries
}

//Wrap metakv.Del with retries
//if the rev provided doesn't match with the rev metakv has, return service_def.ErrorRevisionMismatch
//if metakv operation failed after max number of retries, return service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) Del(key string, rev interface{}) error {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to delete %v from metakv, retried=%v\n", time.Since(start_time).Seconds(), key, i)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		err := metakv.Delete(getPathFromKey(key), rev)
		if err == metakv.ErrRevMismatch {
			return service_def.ErrorRevisionMismatch
		} else if err == nil {
			return nil
		} else {
			meta_svc.logger.Errorf("metakv.Delete failed. key=%v, rev=%v, err=%v, num_of_retry=%v\n", key, rev, err, i)
		}
	}
	return service_def.MetaKVFailedAfterMaxTries
}

func (meta_svc *MetaKVMetadataSvc) DelWithCatalog(catalogKey, key string, rev interface{}) error {
	// ignore catalogKey
	return meta_svc.Del(key, rev)
}

//Wrap metakv.RecursiveDelete with retries
//if metakv operation failed after max number of retries, return service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) DelAllFromCatalog(catalogKey string) error {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to RecursiveDelete for catalogKey=%v to metakv, retried =%v\n", time.Since(start_time).Seconds(), catalogKey, i)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		err := metakv.RecursiveDelete(GetCatalogPathFromCatalogKey(catalogKey))
		if err == nil {
			return nil
		} else {
			meta_svc.logger.Errorf("metakv.RecursiveDelete failed. catalogKey=%v, err=%v, num_of_retry=%v\n", catalogKey, err, i)

		}
	}
	return service_def.MetaKVFailedAfterMaxTries
}

//Wrap metakv.ListAllChildren with retries
//if metakv operation failed after max number of retries, return service_def.MetaKVFailedAfterMaxTries
func (meta_svc *MetaKVMetadataSvc) GetAllMetadataFromCatalog(catalogKey string) ([]*service_def.MetadataEntry, error) {
	start_time := time.Now()
	var i int = 0
	defer meta_svc.logger.Debugf("Took %vs to ListAllChildren for catalogKey=%v to metakv, retried =%v\n", time.Since(start_time).Seconds(), catalogKey, i)
	var entries = make([]*service_def.MetadataEntry, 0)

	for i = 0; i < service_def.MaxNumOfRetries; i++ {
		kvEntries, err := metakv.ListAllChildren(GetCatalogPathFromCatalogKey(catalogKey))
		if err != nil {
			meta_svc.logger.Errorf("metakv.ListAllChildren failed. path=%v, err=%v, num_of_retry=%v\n", GetCatalogPathFromCatalogKey(catalogKey), err, i)
		} else {
			for _, kvEntry := range kvEntries {
				entries = append(entries, &service_def.MetadataEntry{GetKeyFromPath(kvEntry.Path), kvEntry.Value, kvEntry.Rev})
			}
			return entries, nil
		}
	}
	return entries, service_def.MetaKVFailedAfterMaxTries
}

// get all keys from a catalog
func (meta_svc *MetaKVMetadataSvc) GetAllKeysFromCatalog(catalogKey string) ([]string, error) {
	keys := make([]string, 0)

	metaEntries, err := meta_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		return nil, err
	}
	for _, metaEntry := range metaEntries {
		keys = append(keys, metaEntry.Key)
	}
	return keys, nil
}

// metakv requires that all paths start with "/"
func getPathFromKey(key string) string {
	return base.KeyPartsDelimiter + key
}

// the following are exposed since they are needed by metakv call back function

// metakv requires that all parent paths start and end with "/"
func GetCatalogPathFromCatalogKey(catalogKey string) string {
	return base.KeyPartsDelimiter + catalogKey + base.KeyPartsDelimiter
}

func GetKeyFromPath(path string) string {
	if strings.HasPrefix(path, base.KeyPartsDelimiter) {
		return path[len(base.KeyPartsDelimiter):]
	} else {
		// should never get here
		panic(fmt.Sprintf("path=%v doesn't start with '/'", path))
	}
}
