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
		DevMainPipelineSendDelay:                  XDCRDevMainPipelineSendDelayConfig, // TODO
		DevBackfillPipelineSendDelay:              XDCRDevBackfillPipelineSendDelayConfig,
		DevMainPipelineRollbackTo0VB:              XDCRDevMainPipelineRollbackConfig,
		DevBackfillRollbackTo0VB:                  XDCRDevBackfillPipelineRollbackConfig,
		DevCkptMgrForceGCWaitSec:                  XDCRDevCkptGcWaitConfig,
		DevColManifestSvcDelaySec:                 XDCRDevColManifestSvcDelayConfig,
		DevNsServerPortSpecifier:                  XDCRDevNsServerPortSpecifierConfig,
		DevBackfillReplUpdateDelay:                XDCRDevBackfillReplUpdateDelayConfig,
		DevCasDriftForceDocKey:                    XDCRDevCasDriftForceDocConfig,
		DevPreCheckCasDriftForceVbKey:             XDCRDevPreCheckCasDriftForceVBConfig,
		DevPreCheckMaxCasErrorInjection:           XDCRDevPreCheckMaxCasErrorInjectionConfig,
		DevBackfillReqHandlerStartOnceDelay:       XDCRDevBackfillReqHandlerStartOnceDelayConfig,
		DevBackfillReqHandlerHandleVBTaskDoneHang: XDCRDevBackfillReqHandlerHandleVBTaskDoneHangConfig,
		DevBackfillUnrecoverableErrorInj:          XDCRDevBackfillUnrecoverableErrorInjConfig,
		DevBackfillMgrVbsTasksDoneNotifierDelay:   XDCRDevBackfillMgrVbsTasksDoneNotifierDelayConfig,
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
