package service_def

import "github.com/couchbase/goxdcr/base"

type ResolverSvcIface interface {
	ResolveAsync(params *base.ConflictParams, finish_ch chan bool)
	Start()
}
