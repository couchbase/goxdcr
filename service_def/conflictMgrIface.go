package service_def

import (
	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/base"
)

type ConflictManagerIface interface {
	ResolveConflict(source *base.WrappedMCRequest, target *mc.MCResponse, sourceId, targetId []byte) error
}
