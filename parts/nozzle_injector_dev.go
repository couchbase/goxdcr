//go:build dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type BaseConfigDevInjector struct{}

func NewBaseConfigInjector() *BaseConfigDevInjector {
	return &BaseConfigDevInjector{}
}

func (b *BaseConfigDevInjector) InitInjector(cfg *baseConfig, settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]; ok {
		cfg.devMainSendDelay = uint32(val.(int))
	}
	if val, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]; ok {
		cfg.devBackfillSendDelay = uint32(val.(int))
	}
}

func (b *BaseConfigDevInjector) Update(cfg *baseConfig, settings metadata.ReplicationSettingsMap, logger *log.CommonLogger, id string) {
	devMainDelay, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]
	if ok {
		devMainDelayInt := devMainDelay.(int)
		oldMainDelayInt := int(atomic.LoadUint32(&cfg.devMainSendDelay))
		atomic.StoreUint32(&cfg.devMainSendDelay, uint32(devMainDelayInt))
		if oldMainDelayInt != devMainDelayInt {
			logger.Infof("%v updated main pipeline delay to %v millisecond(s)\n", id, devMainDelayInt)
		}
	}
	devBackfillDelay, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]
	if ok {
		devBackfillDelayInt := devBackfillDelay.(int)
		oldBackfillDelayInt := int(atomic.LoadUint32(&cfg.devBackfillSendDelay))
		atomic.StoreUint32(&cfg.devBackfillSendDelay, uint32(devBackfillDelayInt))
		if oldBackfillDelayInt != devBackfillDelayInt {
			logger.Infof("%v updated backfill pipeline delay to %v millisecond(s)\n", id, devBackfillDelay)
		}
	}
}

type DcpNozzleDevInjector struct{}

func NewDcpNozzleInjector() *DcpNozzleDevInjector {
	return &DcpNozzleDevInjector{}
}

func (d *DcpNozzleDevInjector) Init(dcp *DcpNozzle, settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[DCP_DEV_MAIN_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionMainRollbackVb = intVal
		}
	}

	if val, ok := settings[DCP_DEV_BACKFILL_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionBackfillRollbackVb = intVal
		}
	}
}
