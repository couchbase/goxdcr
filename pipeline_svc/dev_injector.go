//go:build dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_svc

import "github.com/couchbase/goxdcr/v8/metadata"

type CheckpointManagerDevInjector struct {
}

func NewCheckpointManagerInjector() *CheckpointManagerDevInjector {
	return &CheckpointManagerDevInjector{}
}

func (c *CheckpointManagerDevInjector) InjectGcWaitSec(ckmgr *CheckpointManager, settings metadata.ReplicationSettingsMap) {
	devInjWaitSec, err := ckmgr.utils.GetIntSettingFromSettings(settings, metadata.DevCkptMgrForceGCWaitSec)
	if err == nil && devInjWaitSec >= 0 {
		ckmgr.xdcrDevInjectGcWaitSec = uint32(devInjWaitSec)
	}
}
