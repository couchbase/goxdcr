//go:build !dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type BaseConfigProdInjector struct{}

func NewBaseConfigInjector() *BaseConfigProdInjector {
	return &BaseConfigProdInjector{}
}

func (b *BaseConfigProdInjector) InitInjector(cfg *baseConfig, settings metadata.ReplicationSettingsMap) {
	// no op
}

func (b *BaseConfigProdInjector) Update(cfg *baseConfig, settings metadata.ReplicationSettingsMap, logger *log.CommonLogger, id string) {
	// no op
}

type DcpNozzleProdInjector struct{}

func NewDcpNozzleInjector() *DcpNozzleProdInjector {
	return &DcpNozzleProdInjector{}
}

func (d *DcpNozzleProdInjector) Init(dcp *DcpNozzle, settings metadata.ReplicationSettingsMap) {
	// no op
}
