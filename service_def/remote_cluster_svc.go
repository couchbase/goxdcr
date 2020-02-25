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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
)

type RemoteClusterSvc interface {
	RemoteClusterByRefId(refId string, refresh bool) (*metadata.RemoteClusterReference, error)
	RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error)
	RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error)
	ValidateAddRemoteCluster(ref *metadata.RemoteClusterReference) error
	// skipConnectivityValidation is true when called from migration service
	AddRemoteCluster(ref *metadata.RemoteClusterReference, skipConnectivityValidation bool) error
	ValidateSetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error
	SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error
	ValidateRemoteCluster(ref *metadata.RemoteClusterReference) error
	DelRemoteCluster(refName string) (*metadata.RemoteClusterReference, error)
	// Gets a map of cloned remote cluster references, with unique ID being the key
	RemoteClusters() (map[string]*metadata.RemoteClusterReference, error)

	// get connection string for specified remote cluster
	// when isCapiReplication is false, return ref.activeHostName, which is rotated among target nodes for load balancing
	// when isCapiReplication is true, return the lexicographically smallest hostname in hostname list of ref,
	// so as to ensure that the same hostname is returned consistently
	// this is critical when the connection string returned will be used to retrieve target server vb map
	// otherwise different server vb maps may be returned by target due to an issue in elastic search plugin
	GetConnectionStringForRemoteCluster(ref *metadata.RemoteClusterReference, isCapiReplication bool) (string, error)

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
	RemoteClusterServiceCallback(path string, value []byte, rev interface{}) error

	// set the metadata change call back method
	// when the remote cluster service makes changes to remote cluster references, it needs to call the call back
	// explicitly, so that the actions can be taken immediately
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)

	// Checks to see if XDCR should use alternate addressing to contact the remote cluster
	ShouldUseAlternateAddress(ref *metadata.RemoteClusterReference) (bool, error)

	// Retrieves the last-updated capability matrix
	GetCapability(ref *metadata.RemoteClusterReference) (metadata.Capability, error)

	// Called by PipelineMgr to check to see if any pipelines should restart due to remoteClusterRef changes
	GetRefListForRestartAndClearState() ([]*metadata.RemoteClusterReference, error)
}
