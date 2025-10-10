//go:build dev

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

type PipelineDevInjector struct{}

func NewPipelineInjector() *PipelineDevInjector {
	return &PipelineDevInjector{}
}

func (i *PipelineDevInjector) InjectDevPreCheckVBPoison(settings *metadata.ReplicationSettings, localMaxCasMap *base.VbSeqnoMapType) {
	devInjectionVB := settings.GetDevPreCheckVBPoison(settings)
	if devInjectionVB >= 0 {
		if _, exists := (*localMaxCasMap)[uint16(devInjectionVB)]; exists {
			// year 2033
			(*localMaxCasMap)[uint16(devInjectionVB)] = 2017616266604601370
		}
	}
}
