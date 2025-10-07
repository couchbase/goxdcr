//go:build dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package backfill_manager

import (
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/metadata"
)

type BackfillMgrDevInjector struct {
}

func NewBackfillMgrInjector() *BackfillMgrDevInjector {
	return &BackfillMgrDevInjector{}
}

func (i *BackfillMgrDevInjector) InjectVbsTaskDoneNotifierDelay(newSpec *metadata.ReplicationSpecification, b *BackfillMgr) {
	if newSpec.Settings.GetBoolSettingValue(metadata.DevBackfillMgrVbsTasksDoneNotifierDelay) {
		atomic.StoreUint32(&b.debugVbsTaskDoneNotifierHang, 1)
	}
}

type BackfillReqHandlerDevInjector struct {
}

func NewBackfillReqHandlerInjector() *BackfillReqHandlerDevInjector {
	return &BackfillReqHandlerDevInjector{}
}

func (i *BackfillReqHandlerDevInjector) InjectStartDelay(b *BackfillRequestHandler) {
	startDelaySec := b.spec.Settings.GetIntSettingValue(metadata.DevBackfillReqHandlerStartOnceDelay)
	if startDelaySec > 0 {
		b.logger.Warnf("Dev injecton Sleeping %v sec for backfill repl service to load the spec before we poll it", startDelaySec)
		time.Sleep(time.Duration(startDelaySec) * time.Second)
	}
}

func (i *BackfillReqHandlerDevInjector) InitVbDelayInjection(b *BackfillRequestHandler) {
	vbDoneDelayEnabled := b.spec.Settings.GetBoolSettingValue(metadata.DevBackfillReqHandlerHandleVBTaskDoneHang)
	if vbDoneDelayEnabled {
		b.logger.Warnf("vbNotToBeDone delay injection is enabled")
		b.devVbDelayEnabled = true
	}
}

func (i *BackfillReqHandlerDevInjector) InjectVbDelay(b *BackfillRequestHandler, vbno uint16) {
	if b.devVbDelayEnabled {
		// dev injection.
		b.logger.Warnf("Dev inj HandleVBTaskDone waiting for 120s after P2P push is done setting VB to %v", vbno)
		time.Sleep(120 * time.Second)
	}
}
