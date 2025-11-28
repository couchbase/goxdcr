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
	"reflect"

	"github.com/couchbase/goxdcr/v8/base"
)

const (
	DCP_DEV_MAIN_ROLLBACK_VB     = base.DevMainPipelineRollbackTo0VB
	DCP_DEV_BACKFILL_ROLLBACK_VB = base.DevBackfillRollbackTo0VB

	XMEM_DEV_MAIN_SLEEP_DELAY             = base.DevMainPipelineSendDelay
	XMEM_DEV_BACKFILL_SLEEP_DELAY         = base.DevBackfillPipelineSendDelay
	XMEM_DEV_NETWORK_IO_FAULT_PROBABILITY = base.DevXmemNozzleNetworkIOFaultProbability
)

func init() {
	// Add development settings to the main configuration map
	xmem_setting_defs[XMEM_DEV_MAIN_SLEEP_DELAY] = base.NewSettingDef(reflect.TypeOf((*int)(nil)), false)
	xmem_setting_defs[XMEM_DEV_BACKFILL_SLEEP_DELAY] = base.NewSettingDef(reflect.TypeOf((*int)(nil)), false)
	xmem_setting_defs[XMEM_DEV_NETWORK_IO_FAULT_PROBABILITY] = base.NewSettingDef(reflect.TypeOf((*int)(nil)), false)
}
