//go:build !dev

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

type CheckpointManagerProdInjector struct {
}

func NewCheckpointManagerInjector() *CheckpointManagerProdInjector {
	return &CheckpointManagerProdInjector{}
}

func (c *CheckpointManagerProdInjector) InjectGcWaitSec(ckmgr *CheckpointManager, settings metadata.ReplicationSettingsMap) {
	// no op
}
