// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// wrapper service that retrieves data from gometa service
package service_def

import (
	"errors"
	"fmt"
)

var MaxNumOfRetries = 5

var MetadataNotFoundErr error = errors.New("key not found")
var ErrorKeyAlreadyExist = errors.New("key being added already exists")
var ErrorRevisionMismatch = errors.New("revision number does not match")
var MetaKVFailedAfterMaxTries error = fmt.Errorf("metakv failed for max number of retries = %v", MaxNumOfRetries)

// struct for general metadata entry maintained by metadata service
type MetadataEntry struct {
	Key   string
	Value []byte
	Rev   interface{}
}

type MetadataSvc interface {
	Get(key string) ([]byte, interface{}, error)
	Add(key string, value []byte) error
	Set(key string, value []byte, rev interface{}) error
	Del(key string, rev interface{}) error

	// catalog related APIs
	AddWithCatalog(catalogKey, key string, value []byte) error
	DelWithCatalog(catalogKey, key string, rev interface{}) error
	GetAllMetadataFromCatalog(catalogKey string) ([]*MetadataEntry, error)
	GetAllKeysFromCatalog(catalogKey string) ([]string, error)
	DelAllFromCatalog(catalogKey string) error
}
