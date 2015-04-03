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
	ValidateAddRemoteCluster(ref *metadata.RemoteClusterReference) error
	AddRemoteCluster(ref *metadata.RemoteClusterReference) error
	ValidateSetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error
	SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error
	DelRemoteCluster(refName string) (*metadata.RemoteClusterReference, error)
	RemoteClusters(refresh bool) (map[string]*metadata.RemoteClusterReference, error)

	// used by auditing and ui logging
	GetRemoteClusterNameFromClusterUuid(uuid string) string

	// Remote cluster service could return two different types of errors:
	// 1. unexpected internal server error
	// 2. validation error indicating the remote cluster involved is not valid or does not exist
	// Distinction between the different types of errors is needed by adminport to decide what status code it should return to client
	// To enable the distinction, remote cluster service wraps validation errors with additional info.
	// This method checks which type the passed in error is, and unwraps the underlying error for validation errors,
	// so as to hide the wrapping implementation from callers.
	// This method returns
	// 1. false and the original error for internal server errors.
	// 2. true and unwrapped error for validation errors.
	CheckAndUnwrapRemoteClusterError(err error) (bool, error)

	// Service call back function for remote cluster changed event
	RemoteClusterServiceCallback(path string, value []byte, rev interface{}) (string, interface{}, interface{}, error)
}
