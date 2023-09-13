/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package service_def

import (
	"github.com/couchbase/goxdcr/crMeta"
)

type ResolverSvcIface interface {
	ResolveAsync(params *crMeta.ConflictParams, finish_ch chan bool)
	InitDefaultFunc()
	Start(sourceKVHost string, xdcrRestPort uint16)
	Started() bool
	CheckMergeFunction(fname string) error
}
