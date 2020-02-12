// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/metadata"
)

type ManifestsService interface {
	UpsertSourceManifests(replSpec *metadata.ReplicationSpecification, src *metadata.ManifestsList) error
	UpsertTargetManifests(replSpec *metadata.ReplicationSpecification, tgt *metadata.ManifestsList) error
	GetSourceManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error)
	GetTargetManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error)
}
