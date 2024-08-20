// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type UIWarnings interface {
	String() string
	Len() int

	GetSuccessfulWarningStrings() []string
	GetFieldWarningsOnly() map[string]interface{}

	AppendGeneric(warning string)
	AddWarning(key string, val string)
}

type ReplicationSpecSvc interface {
	ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error)
	ReplicationSpecReadOnly(replicationId string) (*metadata.ReplicationSpecification, error)
	// additionalInfo is an optional parameter, which, if provided, will be written to replication creation ui log
	AddReplicationSpec(spec *metadata.ReplicationSpecification, additionalInfo string) error
	ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, UIWarnings, *metadata.CollectionsManifestPair)
	ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (base.ErrorMap, error, UIWarnings)
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

	// set the metadata change call back methods
	// when the replication spec service makes changes, it needs to call the call back
	// explicitly, so that the actions can be taken immediately
	SetMetadataChangeHandlerCallback(id string, callBack base.MetadataChangeHandlerCallbackWithWg, add base.MetadataChangeHandlerPriority, del base.MetadataChangeHandlerPriority, mod base.MetadataChangeHandlerPriority)

	SetManifestsGetter(getter ManifestsGetter)
}
