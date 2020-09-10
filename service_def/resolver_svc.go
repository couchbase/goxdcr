package service_def

import "github.com/couchbase/goxdcr/base"

type ResolverSvcIface interface {
	ResolveAsync(params *base.ConflictParams, finish_ch chan bool)
	InitDefaultFunc()
	Start(sourceKVHost string, xdcrRestPort uint16)
	Started() bool
	CheckMergeFunction(fname string) error
}
