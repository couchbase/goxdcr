// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/metadata"
)

type ManifestsService interface {
	UpsertSourceManifests(replSpec *metadata.ReplicationSpecification, src *metadata.ManifestsList) error
	UpsertTargetManifests(replSpec *metadata.ReplicationSpecification, tgt *metadata.ManifestsList) error
	GetSourceManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error)
	GetTargetManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error)
	DelManifests(replSpec *metadata.ReplicationSpecification) error
}
