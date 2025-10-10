//go:build !dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type PipelineProdInjector struct{}

func NewPipelineInjector() *PipelineProdInjector {
	return &PipelineProdInjector{}
}

func (i *PipelineProdInjector) InjectDevPreCheckVBPoison(settings *metadata.ReplicationSettings, localMaxCasMap *base.VbSeqnoMapType) {
	// no op
}
