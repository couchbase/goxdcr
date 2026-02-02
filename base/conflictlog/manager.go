/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
)

// Manager defines behaviour for conflict manager
type Manager interface {
	NewLogger(logger *log.CommonLogger, replId string, eventsProducer common.PipelineEventsProducer, opts ...LoggerOpt) (l Logger, err error)
	ConnPool() iopool.ConnPool
	SetConnLimit(limit int)
	SetIOPSLimit(limit int64)
	StartMonitor(monitor Monitor)
}
