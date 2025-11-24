//go:build dev

// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"github.com/couchbase/goxdcr/v8/base"
)

const (
	DevMainPipelineSendDelay                  = base.DevMainPipelineSendDelay
	DevBackfillPipelineSendDelay              = base.DevBackfillPipelineSendDelay
	DevBackfillRollbackTo0VB                  = base.DevBackfillRollbackTo0VB
	DevMainPipelineRollbackTo0VB              = base.DevMainPipelineRollbackTo0VB
	DevCkptMgrForceGCWaitSec                  = base.DevCkptMgrForceGCWaitSec
	DevColManifestSvcDelaySec                 = base.DevColManifestSvcDelaySec
	DevNsServerPortSpecifier                  = base.DevNsServerPortSpecifier
	DevBackfillReplUpdateDelay                = base.DevBackfillReplUpdateDelay
	DevCasDriftForceDocKey                    = base.DevCasDriftForceDocKey
	DevPreCheckCasDriftForceVbKey             = base.DevPreCheckCasDriftForceVbKey
	DevPreCheckMaxCasErrorInjection           = base.DevPreCheckMaxCasErrorInjection
	DevBackfillReqHandlerStartOnceDelay       = base.DevBackfillReqHandlerStartOnceDelay
	DevBackfillReqHandlerHandleVBTaskDoneHang = base.DevBackfillReqHandlerHandleVBTaskDoneHang
	DevBackfillUnrecoverableErrorInj          = base.DevBackfillUnrecoverableErrorInj
	DevBackfillMgrVbsTasksDoneNotifierDelay   = base.DevBackfillMgrVbsTasksDoneNotifierDelay
)

var XDCRDevMainPipelineSendDelayConfig = &SettingsConfig{0 /*ms*/, &Range{0, 10000}}
var XDCRDevBackfillPipelineSendDelayConfig = &SettingsConfig{0 /*ms*/, &Range{0, 10000}}
var XDCRDevMainPipelineRollbackConfig = &SettingsConfig{-1 /*vbno*/, &Range{-1, 1023}}
var XDCRDevBackfillPipelineRollbackConfig = &SettingsConfig{-1 /*vbno*/, &Range{-1, 1023}}
var XDCRDevCkptGcWaitConfig = &SettingsConfig{0 /*sec*/, &Range{0, 3600}}
var XDCRDevColManifestSvcDelayConfig = &SettingsConfig{0 /*sec*/, &Range{0, 3600}}
var XDCRDevNsServerPortSpecifierConfig = &SettingsConfig{0 /*not specified*/, &Range{0, 65535}}
var XDCRDevBackfillReplUpdateDelayConfig = &SettingsConfig{0 /*not specified*/, &Range{0, 100000}}
var XDCRDevCasDriftForceDocConfig = &SettingsConfig{"", nil}
var XDCRDevPreCheckCasDriftForceVBConfig = &SettingsConfig{-1, &Range{-1, 1023}}
var XDCRDevPreCheckMaxCasErrorInjectionConfig = &SettingsConfig{false, nil}
var XDCRDevBackfillReqHandlerStartOnceDelayConfig = &SettingsConfig{0, &Range{0, 1000}}
var XDCRDevBackfillReqHandlerHandleVBTaskDoneHangConfig = &SettingsConfig{false, nil}
var XDCRDevBackfillUnrecoverableErrorInjConfig = &SettingsConfig{false, nil}
var XDCRDevBackfillMgrVbsTasksDoneNotifierDelayConfig = &SettingsConfig{false, nil}

// This file contains development-only settings that will only be included
// when the code is compiled with the "dev" build tag
func init() {
	// Add development settings to the main configuration map
	devSettings := map[string]*SettingsConfig{
		// DevMainPipelineDelay is used to inject delay (in ms) in main replication pipeline for testing purposes
		// It enforces a delay between each document send
		DevMainPipelineSendDelay: XDCRDevMainPipelineSendDelayConfig, // TODO

		// DevBackfillPipelineDelay is used to inject delay (in ms) in backfill replication pipeline for testing purposes
		// It enforces a delay between each document send
		DevBackfillPipelineSendDelay: XDCRDevBackfillPipelineSendDelayConfig,

		// DevMainPipelineRollbackTo0VB is used to force main replication pipeline to rollback to vb 0 for testing purposes
		// The setting takes a VB number, and upon receiving the VB, simulates a rollback to 0 from DCP
		DevMainPipelineRollbackTo0VB: XDCRDevMainPipelineRollbackConfig,

		// DevBackfillRollbackTo0VB is used to force backfill replication pipeline to rollback to vb 0 for testing purposes
		// The setting takes a VB number, and upon receiving the VB, simulates a rollback to 0 from DCP
		DevBackfillRollbackTo0VB: XDCRDevBackfillPipelineRollbackConfig,

		// DevCkptMgrForceGCWaitSec is used to inject a wait time (in seconds) to simulate the time it takes to
		// acquire a stop-the-world lock
		DevCkptMgrForceGCWaitSec: XDCRDevCkptGcWaitConfig,

		// DevColManifestSvcDelaySec is used to inject delay (in seconds) in the collection manifest service
		// Before calling the callback
		DevColManifestSvcDelaySec: XDCRDevColManifestSvcDelayConfig,

		// DevNsServerPortSpecifier is used to specify a custom port for ns_server to ensure the delay
		// only applies to a specific goxdcr process in a cluster_run environment
		DevNsServerPortSpecifier: XDCRDevNsServerPortSpecifierConfig,

		// DevBackfillReplUpdateDelay is used to inject delay (in ms) in backfill replication
		// spec update process to simulate a busy system
		DevBackfillReplUpdateDelay: XDCRDevBackfillReplUpdateDelayConfig,

		// DevCasDriftForceDocKey is used to force cas drift check to be performed on a specific document key
		DevCasDriftForceDocKey: XDCRDevCasDriftForceDocConfig,

		// DevPreCheckCasDriftForceVbKey is used to force cas drift pre-check to be performed on a specific vb
		DevPreCheckCasDriftForceVbKey: XDCRDevPreCheckCasDriftForceVBConfig,

		// DevPreCheckMaxCasErrorInjection is used to inject max cas error during pre-check phase
		DevPreCheckMaxCasErrorInjection: XDCRDevPreCheckMaxCasErrorInjectionConfig,

		// DevBackfillReqHandlerStartOnceDelay is used to inject delay (in secs) before starting backfill req handler
		DevBackfillReqHandlerStartOnceDelay: XDCRDevBackfillReqHandlerStartOnceDelayConfig,

		// DevBackfillReqHandlerHandleVBTaskDoneHang is used to inject a hang in HandleVBTaskDone to simulate slow running system
		DevBackfillReqHandlerHandleVBTaskDoneHang: XDCRDevBackfillReqHandlerHandleVBTaskDoneHangConfig,

		// DevBackfillUnrecoverableErrorInj is used to inject an unrecoverable error during backfill processing for testing purposes
		DevBackfillUnrecoverableErrorInj: XDCRDevBackfillUnrecoverableErrorInjConfig,

		// DevBackfillMgrVbsTasksDoneNotifierDelay is used to inject delay in backfill mgr's notifier for vb tasks done
		DevBackfillMgrVbsTasksDoneNotifierDelay: XDCRDevBackfillMgrVbsTasksDoneNotifierDelayConfig,
	}

	// Merge the dev settings into the main configuration map
	for k, v := range devSettings {
		ReplicationSettingsConfigMap[k] = v
	}
}

type ReplicationSettingsDevInjections struct{}

func NewReplicationSettingInjections() *ReplicationSettingsDevInjections {
	return &ReplicationSettingsDevInjections{}
}

func (s *ReplicationSettingsDevInjections) GetDevMainPipelineDelay(settings *ReplicationSettings) int {
	return settings.GetIntSettingValue(DevMainPipelineSendDelay)
}

func (s *ReplicationSettingsDevInjections) GetDevPreCheckVBPoison(settings *ReplicationSettings) int {
	return settings.GetIntSettingValue(DevPreCheckCasDriftForceVbKey)
}

func (s *ReplicationSettingsDevInjections) GetDevBackfillPipelineDelay(settings *ReplicationSettings) int {
	return settings.GetIntSettingValue(DevBackfillPipelineSendDelay)
}

func (s *ReplicationSettingsDevInjections) GetDevPreCheckMaxCasErrorInjection(settings *ReplicationSettings) bool {
	return settings.GetBoolSettingValue(DevPreCheckMaxCasErrorInjection)
}

func (s *ReplicationSettingsDevInjections) GetDevMainPipelineRollbackTo0VB(settings *ReplicationSettings) int {
	return settings.GetIntSettingValue(DevMainPipelineRollbackTo0VB)
}

func (s *ReplicationSettingsDevInjections) GetDevBackfillPipelineRollbackTo0VB(settings *ReplicationSettings) int {
	return settings.GetIntSettingValue(DevBackfillRollbackTo0VB)
}

func (s *ReplicationSettingsDevInjections) GetCasDriftInjectDocKey(settings *ReplicationSettings) string {
	return settings.GetStringSettingValue(DevCasDriftForceDocKey)
}
