//go:build !dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package factory

import (
	"github.com/couchbase/goxdcr/v8/metadata"
)

type XDCRFactoryProdInjector struct{}

func NewXDCRFactoryInjector() *XDCRFactoryProdInjector {
	return &XDCRFactoryProdInjector{}
}

// Implement no-ops for any type of injections
func (x *XDCRFactoryProdInjector) InjectDcpNozzle(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
}
func (x *XDCRFactoryProdInjector) InjectDcpNozzleUpdate(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
}
func (x *XDCRFactoryProdInjector) InjectXmemNozzle(in metadata.ReplicationSettings, out metadata.ReplicationSettingsMap) {
}
func (x *XDCRFactoryProdInjector) InjectXmemNozzleUpdate(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
}
func (x *XDCRFactoryProdInjector) InjectCheckpointMgr(in, out metadata.ReplicationSettingsMap) {}
func (x *XDCRFactoryProdInjector) InjectRouter(in, out metadata.ReplicationSettingsMap)        {}
