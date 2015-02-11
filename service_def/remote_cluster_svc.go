// Copyright (c) 2013 Couchbase, Inc.
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

type RemoteClusterSvc interface {
	RemoteClusterByRefId(refId string, refresh bool) (*metadata.RemoteClusterReference, error)
	RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error)
	RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error)
	AddRemoteCluster(ref *metadata.RemoteClusterReference) error
	SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error
	DelRemoteCluster(refName string) error
	RemoteClusters(refresh bool) (map[string]*metadata.RemoteClusterReference, error)
	ValidateRemoteCluster(ref *metadata.RemoteClusterReference) error
}
