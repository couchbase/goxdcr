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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/cbauth/metakv"
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

func (meta_svc *MetaKVMetadataSvc) Get(key string) ([]byte, interface{}, error) {
	start_time := time.Now()
	defer meta_svc.logger.Debugf("Took %vs to get %v to metakv\n", time.Since(start_time).Seconds(), key)
	return metakv.Get(getPathFromKey(key))
}

func (meta_svc *MetaKVMetadataSvc) Add(key string, value []byte) error {
	start_time := time.Now()
	err := metakv.Add(getPathFromKey(key), value)
	if err == metakv.ErrRevMismatch {
		err = service_def.ErrorKeyAlreadyExist
	}
	meta_svc.logger.Debugf("Took %vs to add %v to metakv\n", time.Since(start_time).Seconds(), key)
	return err
}

func (meta_svc *MetaKVMetadataSvc) AddWithCatalog(catalogKey, key string, value []byte) error {
	// ignore catalogKey
	return meta_svc.Add(key, value)
}

func (meta_svc *MetaKVMetadataSvc) Set(key string, value []byte, rev interface{}) error {
	start_time := time.Now()
	defer meta_svc.logger.Debugf("Took %vs to set %v to metakv\n", time.Since(start_time).Seconds(), key)
	err := metakv.Set(getPathFromKey(key), value, rev)
	if err == metakv.ErrRevMismatch {
		err = service_def.ErrorRevisionMismatch
	}
	return err
}

func (meta_svc *MetaKVMetadataSvc) Del(key string, rev interface{}) error {
	err := metakv.Delete(getPathFromKey(key), rev)
	if err == metakv.ErrRevMismatch {
		err = service_def.ErrorRevisionMismatch
	}
	return err
}

func (meta_svc *MetaKVMetadataSvc) DelWithCatalog(catalogKey, key string, rev interface{}) error {
	// ignore catalogKey
	return meta_svc.Del(key, rev)
}

func (meta_svc *MetaKVMetadataSvc) DelAllFromCatalog (catalogKey string) error {
	return metakv.RecursiveDelete (GetCatalogPathFromCatalogKey(catalogKey))
}

func (meta_svc *MetaKVMetadataSvc) GetAllMetadataFromCatalog(catalogKey string) ([]*service_def.MetadataEntry, error) {
	var entries = make([]*service_def.MetadataEntry, 0)
	kvEntries, err := metakv.ListAllChildren(GetCatalogPathFromCatalogKey(catalogKey))
	if err != nil {
		meta_svc.logger.Errorf("Failed to list all children. err=%v\n", err)
		return nil, err
	}
	for _, kvEntry := range kvEntries {
		entries = append(entries, &service_def.MetadataEntry{GetKeyFromPath(kvEntry.Path), kvEntry.Value, kvEntry.Rev})
	}
	return entries, nil
}

// get all keys from a catalog
func (meta_svc *MetaKVMetadataSvc) GetAllKeysFromCatalog(catalogKey string) ([]string, error) {
	keys := make([]string, 0)

	kvEntries, err := metakv.ListAllChildren(GetCatalogPathFromCatalogKey(catalogKey))
	if err != nil {
		return nil, err
	}
	for _, kvEntry := range kvEntries {
		keys = append(keys, GetKeyFromPath(kvEntry.Path))
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
		return ""
	}
}
