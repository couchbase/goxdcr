//go:build dev

// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package base

// XDCR Dev hidden replication settings
const DevMainPipelineSendDelay = "xdcrDevMainSendDelayMs"
const DevBackfillPipelineSendDelay = "xdcrDevBackfillSendDelayMs"
const DevMainPipelineRollbackTo0VB = "xdcrDevMainRollbackTo0VB"
const DevBackfillRollbackTo0VB = "xdcrDevBackfillRollbackTo0VB"
const DevCkptMgrForceGCWaitSec = "xdcrDevCkptMgrForceGCWaitSec"
const DevColManifestSvcDelaySec = "xdcrDevColManifestSvcDelaySec"
const DevNsServerPortSpecifier = "xdcrDevNsServerPort"
const DevBackfillReplUpdateDelay = "xdcrDevBackfillReplUpdateDelayMs"
const DevCasDriftForceDocKey = "xdcrDevCasDriftInjectDocKey"
const DevPreCheckCasDriftForceVbKey = "xdcrDevPreCheckCasDriftInjectVb"
const DevPreCheckMaxCasErrorInjection = "xdcrDevPreCheckMaxCasErrorInjection"
const DevBackfillReqHandlerStartOnceDelay = "xdcrDevBackfillReqHandlerStartOnceDelaySec"
const DevBackfillReqHandlerHandleVBTaskDoneHang = "xdcrDevBackfillReqHandlerHandleVBTaskDoneHang"
const DevBackfillUnrecoverableErrorInj = "xdcrDevBackfillUnrecoverableErrorInj"
const DevBackfillMgrVbsTasksDoneNotifierDelay = "xdcrDevBackfillMgrVbsTasksDoneNotifierDelay"
