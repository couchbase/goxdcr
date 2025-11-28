//go:build !dev

// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

// Prod replication settings do not involve additional

type ReplicationSettingsProdInjections struct{}

func NewReplicationSettingInjections() *ReplicationSettingsProdInjections {
	return &ReplicationSettingsProdInjections{}
}

// All following should be no-op values

func (s *ReplicationSettingsProdInjections) GetDevMainPipelineDelay(settings *ReplicationSettings) int {
	return 0
}

func (s *ReplicationSettingsProdInjections) GetDevPreCheckVBPoison(settings *ReplicationSettings) int {
	return -1
}

func (s *ReplicationSettingsProdInjections) GetDevBackfillPipelineDelay(settings *ReplicationSettings) int {
	return 0
}

func (s *ReplicationSettingsProdInjections) GetDevPreCheckMaxCasErrorInjection(settings *ReplicationSettings) bool {
	return false
}

func (s *ReplicationSettingsProdInjections) GetDevMainPipelineRollbackTo0VB(settings *ReplicationSettings) int {
	return -1
}

func (s *ReplicationSettingsProdInjections) GetDevBackfillPipelineRollbackTo0VB(settings *ReplicationSettings) int {
	return -1
}

func (s *ReplicationSettingsProdInjections) GetCasDriftInjectDocKey(settings *ReplicationSettings) string {
	return ""
}

func (s *ReplicationSettingsProdInjections) GetXmemNozzleNetworkIOFaultPercent(settings *ReplicationSettings) int {
	return 0
}
