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

import (
	"strconv"
	"strings"
	"sync"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type CheckpointManagerDevInjector struct {
	// State for the removeSrcFailoverLogVBsOnce injection. Held here (not on CheckpointManager)
	// so that the destructive failover-log mutation lives entirely in the dev build.
	removeSrcFailoverLogMtx   sync.Mutex
	removeSrcFailoverLogVbs   map[uint16]bool
	removeSrcFailoverLogFired bool
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

func (c *CheckpointManagerDevInjector) InjectRemoveSrcFailoverLogVbsOnce(ckmgr *CheckpointManager, settings metadata.ReplicationSettingsMap) {
	val, ok := settings[metadata.DevRemoveSrcFailoverLogVBsOnce]
	if !ok {
		return
	}
	csv, ok := val.(string)
	if !ok || strings.TrimSpace(csv) == "" {
		return
	}
	vbs := make(map[uint16]bool)
	for _, tok := range strings.Split(csv, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		vb, err := strconv.ParseUint(tok, 10, 16)
		if err != nil {
			ckmgr.logger.Warnf("DEV injection removeSrcFailoverLogVBsOnce: ignoring invalid vb token %q: %v", tok, err)
			continue
		}
		vbs[uint16(vb)] = true
	}
	c.removeSrcFailoverLogMtx.Lock()
	defer c.removeSrcFailoverLogMtx.Unlock()
	c.removeSrcFailoverLogVbs = vbs
	c.removeSrcFailoverLogFired = false
}

// MaybeRemoveSrcFailoverLogVbs removes the dev-injected VBs from the source failover log map the
// first time any of them is present, simulating an incomplete source failover log fetch on a
// freshly rebalanced-in node. This reproduces the silent checkpoint drop in
// filterInvalidCkptsBasedOnSourceFailover (VBs absent from the failover log map are skipped).
func (c *CheckpointManagerDevInjector) MaybeRemoveSrcFailoverLogVbs(ckmgr *CheckpointManager, failoverLogMap map[uint16]*mcc.FailoverLog) {
	c.removeSrcFailoverLogMtx.Lock()
	defer c.removeSrcFailoverLogMtx.Unlock()
	if c.removeSrcFailoverLogFired || len(c.removeSrcFailoverLogVbs) == 0 {
		return
	}
	var removed []uint16
	for vb := range c.removeSrcFailoverLogVbs {
		if _, present := failoverLogMap[vb]; present {
			delete(failoverLogMap, vb)
			removed = append(removed, vb)
		}
	}
	if len(removed) > 0 {
		c.removeSrcFailoverLogFired = true
		ckmgr.logger.Infof("DEV injection removeSrcFailoverLogVBsOnce: removed src failover log vbs %v (once); remaining failover log entries=%d", removed, len(failoverLogMap))
	}
}
