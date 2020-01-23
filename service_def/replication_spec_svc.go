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

type ReplicationSpecSvc interface {
	ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error)
	// additionalInfo is an optional parameter, which, if provided, will be written to replication creation ui log
	AddReplicationSpec(spec *metadata.ReplicationSpecification, additionalInfo string) error
	ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, []string)
	ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap) (base.ErrorMap, error)
	SetReplicationSpec(spec *metadata.ReplicationSpecification) error
	DelReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error)
	DelReplicationSpecWithReason(replicationId string, reason string) (*metadata.ReplicationSpecification, error)
	AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error)
	AllActiveReplicationSpecsReadOnly() (map[string]*metadata.ReplicationSpecification, error)
	AllReplicationSpecIds() ([]string, error)
	AllReplicationSpecIdsForBucket(bucket string) ([]string, error)
	AllReplicationSpecsWithRemote(remoteClusterRef *metadata.RemoteClusterReference) ([]*metadata.ReplicationSpecification, error)

	// checks if an error returned by the replication spec service is an internal server error or a validation error,
	// e.g., an error indicating the replication spec involved should exist but does not, or the other way around
	// adminport needs this info to tell what status code it should return to client
	IsReplicationValidationError(err error) bool

	// Service call back function for replication spec changed event
	ReplicationSpecServiceCallback(path string, value []byte, rev interface{}) error

	ValidateAndGC(spec *metadata.ReplicationSpecification)

	// being used by unit tests only
	ConstructNewReplicationSpec(sourceBucketName, targetClusterUUID, targetBucketName string) (*metadata.ReplicationSpecification, error)

	//get the derived object (i.e. ReplicationStatus) for the specification
	//this is used to keep the derived object and replication spec in the same cache
	GetDerivedObj(specId string) (interface{}, error)

	//set the derived object (i.e ReplicationStatus) for the specification
	SetDerivedObj(specId string, derivedObj interface{}) error

	// set the metadata change call back method
	// when the replication spec service makes changes, it needs to call the call back
	// explicitly, so that the actions can be taken immediately
	SetMetadataChangeHandlerCallback(callBack base.MetadataChangeHandlerCallback)
}
