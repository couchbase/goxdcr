//go:build dev

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
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/metadata"
)

type RouterDevInjector struct{}

func NewRouterInjector() *RouterDevInjector {
	return &RouterDevInjector{}
}

func (r *RouterDevInjector) Inject(router *Router, settings metadata.ReplicationSettingsMap) {
	casDriftInjectDocKey, ok := settings[metadata.DevCasDriftForceDocKey].(string)
	if ok && casDriftInjectDocKey != "" {
		router.devCasDriftKey = casDriftInjectDocKey
		atomic.StoreUint32(&router.devCasDriftInjectOn, 1)
	}
}
