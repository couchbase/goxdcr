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

type MetadataSvc interface {
	Get(key string) ([] byte, error)
	Add(key string, value []byte) error
	Set(key string, value []byte) error
	Del(key string) error
	
	// catalog related APIs
	AddWithCatalog(catalogKey, key string, value []byte) error
	DelWithCatalog(catalogKey, key string) error
	GetKeysFromCatalog(catalogKey string) ([]string, error)
}
