//go:build dev

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
	"github.com/couchbase/goxdcr/v8/parts"
)

type XDCRFactoryDevInjector struct{}

func NewXDCRFactoryInjector() *XDCRFactoryDevInjector {
	return &XDCRFactoryDevInjector{}
}

func (i *XDCRFactoryDevInjector) InjectDcpNozzle(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
	out[parts.DCP_DEV_MAIN_ROLLBACK_VB] = metadata.GetSettingFromSettingsMap(in, metadata.DevMainPipelineRollbackTo0VB, -1)
	out[parts.DCP_DEV_BACKFILL_ROLLBACK_VB] = metadata.GetSettingFromSettingsMap(in, metadata.DevBackfillRollbackTo0VB, -1)
}

func (i *XDCRFactoryDevInjector) InjectDcpNozzleUpdate(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
	devMainVbRollback, ok := in[metadata.DevMainPipelineRollbackTo0VB]
	if ok {
		out[parts.DCP_DEV_MAIN_ROLLBACK_VB] = devMainVbRollback
	}

	devBackfillVbRollback, ok := in[metadata.DevBackfillRollbackTo0VB]
	if ok {
		out[parts.DCP_DEV_BACKFILL_ROLLBACK_VB] = devBackfillVbRollback
	}
}

func (i *XDCRFactoryDevInjector) InjectXmemNozzle(in metadata.ReplicationSettings, out metadata.ReplicationSettingsMap) {
	out[parts.XMEM_DEV_MAIN_SLEEP_DELAY] = in.GetDevMainPipelineDelay(&in)
	out[parts.XMEM_DEV_BACKFILL_SLEEP_DELAY] = in.GetDevBackfillPipelineDelay(&in)
}

func (i *XDCRFactoryDevInjector) InjectXmemNozzleUpdate(in metadata.ReplicationSettingsMap, out metadata.ReplicationSettingsMap) {
	mainSleepDelay, ok := in[metadata.DevMainPipelineSendDelay]
	if ok {
		out[parts.XMEM_DEV_MAIN_SLEEP_DELAY] = mainSleepDelay
	}

	backfillSleepDelay, ok := in[metadata.DevBackfillPipelineSendDelay]
	if ok {
		out[parts.XMEM_DEV_BACKFILL_SLEEP_DELAY] = backfillSleepDelay
	}
}

func (i *XDCRFactoryDevInjector) InjectCheckpointMgr(settings, outSettigs metadata.ReplicationSettingsMap) {
	outSettigs[metadata.DevCkptMgrForceGCWaitSec] = metadata.GetSettingFromSettingsMap(settings, metadata.DevCkptMgrForceGCWaitSec, metadata.XDCRDevCkptGcWaitConfig.Default())
}

func (i *XDCRFactoryDevInjector) InjectRouter(in, out metadata.ReplicationSettingsMap) {
	devCasDriftForceDocKey, ok := in[metadata.DevCasDriftForceDocKey]
	if ok {
		out[metadata.DevCasDriftForceDocKey] = devCasDriftForceDocKey
	}
}
