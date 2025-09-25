// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
)

var _ metadata_svc.RemoteCngAgentDataProvider = &RemoteCngAgent{}

func (agent *RemoteCngAgent) OneTimeGetRemoteBucketManifest(bucketName string) (*metadata.CollectionsManifest, error) {
	// Darshan TODO: implement
	return nil, nil
}

func (agent *RemoteCngAgent) GetManifest(bucketName string, restAPIQuery bool) (*metadata.CollectionsManifest, error) {
	// Darshan TODO: implement
	return nil, nil
}
