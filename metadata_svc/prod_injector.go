//go:build !dev

// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type BackfillReplServiceProdInjector struct {
}

func NewBackfillReplServiceInjector() *BackfillReplServiceProdInjector {
	return &BackfillReplServiceProdInjector{}
}

func (b *BackfillReplServiceProdInjector) InjectBackfillReplDelay(spec *metadata.BackfillReplicationSpec) {
	// no op
}

func (b *BackfillReplServiceProdInjector) InjectRaiseCompleteBackfillDelay(replSpecSvc service_def.ReplicationSpecSvc, backfillSpecId string, logger *log.CommonLogger, injected *bool, err *error) {
}

func (b *BackfillReplServiceProdInjector) InjectRaiseCompleteBackfillDelaySleep(replSpecSvc service_def.ReplicationSpecSvc, logger *log.CommonLogger) {
}

type CollectionsManifestSvcProdInjector struct {
}

func NewCollectionsManifestSvcInjector() *CollectionsManifestSvcProdInjector {
	return &CollectionsManifestSvcProdInjector{}
}

func (i *CollectionsManifestSvcProdInjector) InjectDelay(newSpec *metadata.ReplicationSpecification, c *CollectionsManifestService) {
	// no op
}
