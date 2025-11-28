// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

//go:build dev

package replication_manager

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

// This file contains development-only settings that will only be included
// when the code is compiled with the "dev" build tag
func init() {
	// Add development settings to the main configuration map
	devKeyToSettingsMap := map[string]string{
		base.DevMainPipelineSendDelay:                  metadata.DevMainPipelineSendDelay,
		base.DevBackfillPipelineSendDelay:              metadata.DevBackfillPipelineSendDelay,
		base.DevMainPipelineRollbackTo0VB:              metadata.DevMainPipelineRollbackTo0VB,
		base.DevBackfillRollbackTo0VB:                  metadata.DevBackfillRollbackTo0VB,
		base.DevCkptMgrForceGCWaitSec:                  metadata.DevCkptMgrForceGCWaitSec,
		base.DevColManifestSvcDelaySec:                 metadata.DevColManifestSvcDelaySec,
		base.DevNsServerPortSpecifier:                  metadata.DevNsServerPortSpecifier,
		base.DevBackfillReplUpdateDelay:                metadata.DevBackfillReplUpdateDelay,
		base.DevCasDriftForceDocKey:                    metadata.DevCasDriftForceDocKey,
		base.DevPreCheckCasDriftForceVbKey:             metadata.DevPreCheckCasDriftForceVbKey,
		base.DevPreCheckMaxCasErrorInjection:           metadata.DevPreCheckMaxCasErrorInjection,
		base.DevBackfillReqHandlerStartOnceDelay:       metadata.DevBackfillReqHandlerStartOnceDelay,
		base.DevBackfillReqHandlerHandleVBTaskDoneHang: metadata.DevBackfillReqHandlerHandleVBTaskDoneHang,
		base.DevBackfillUnrecoverableErrorInj:          metadata.DevBackfillUnrecoverableErrorInj,
		base.DevBackfillMgrVbsTasksDoneNotifierDelay:   metadata.DevBackfillMgrVbsTasksDoneNotifierDelay,
		base.DevXmemNozzleNetworkIOFaultProbability:    metadata.DevXmemNozzleNetworkIOFaultProbability,
	}

	// Merge the dev settings into the main configuration map
	for k, v := range devKeyToSettingsMap {
		RestKeyToSettingsKeyMap[k] = v
	}

	devSettingsKeyToKeyMap := map[string]string{
		metadata.DevMainPipelineSendDelay:                  base.DevMainPipelineSendDelay,
		metadata.DevBackfillPipelineSendDelay:              base.DevBackfillPipelineSendDelay,
		metadata.DevMainPipelineRollbackTo0VB:              base.DevMainPipelineRollbackTo0VB,
		metadata.DevBackfillRollbackTo0VB:                  base.DevBackfillRollbackTo0VB,
		metadata.DevCkptMgrForceGCWaitSec:                  base.DevCkptMgrForceGCWaitSec,
		metadata.DevColManifestSvcDelaySec:                 base.DevColManifestSvcDelaySec,
		metadata.DevNsServerPortSpecifier:                  base.DevNsServerPortSpecifier,
		metadata.DevBackfillReplUpdateDelay:                base.DevBackfillReplUpdateDelay,
		metadata.DevCasDriftForceDocKey:                    base.DevCasDriftForceDocKey,
		metadata.DevPreCheckCasDriftForceVbKey:             base.DevPreCheckCasDriftForceVbKey,
		metadata.DevPreCheckMaxCasErrorInjection:           base.DevPreCheckMaxCasErrorInjection,
		metadata.DevBackfillReqHandlerStartOnceDelay:       base.DevBackfillReqHandlerStartOnceDelay,
		metadata.DevBackfillReqHandlerHandleVBTaskDoneHang: base.DevBackfillReqHandlerHandleVBTaskDoneHang,
		metadata.DevBackfillUnrecoverableErrorInj:          base.DevBackfillUnrecoverableErrorInj,
		metadata.DevBackfillMgrVbsTasksDoneNotifierDelay:   base.DevBackfillMgrVbsTasksDoneNotifierDelay,
		metadata.DevXmemNozzleNetworkIOFaultProbability:    base.DevXmemNozzleNetworkIOFaultProbability,
	}

	for k, v := range devSettingsKeyToKeyMap {
		SettingsKeyToRestKeyMap[k] = v
	}
}
