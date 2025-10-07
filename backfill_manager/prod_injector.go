//go:build !dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package backfill_manager

import "github.com/couchbase/goxdcr/v8/metadata"

type BackfillMgrProdInjector struct {
}

func NewBackfillMgrInjector() *BackfillMgrProdInjector {
	return &BackfillMgrProdInjector{}
}

func (i *BackfillMgrProdInjector) InjectVbsTaskDoneNotifierDelay(newSpec *metadata.ReplicationSpecification, b *BackfillMgr) {
	// no-op
}

type BackfillReqHandlerProdInjector struct {
}

func NewBackfillReqHandlerInjector() *BackfillReqHandlerProdInjector {
	return &BackfillReqHandlerProdInjector{}
}

func (i *BackfillReqHandlerProdInjector) InjectStartDelay(b *BackfillRequestHandler) {
	// no-op
}

func (i *BackfillReqHandlerProdInjector) InitVbDelayInjection(b *BackfillRequestHandler) {
	// no-op
}

func (i *BackfillReqHandlerProdInjector) InjectVbDelay(*BackfillRequestHandler, uint16) {
	// no-op
}
