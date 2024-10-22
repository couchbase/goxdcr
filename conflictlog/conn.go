package conflictlog

import (
	"io"
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/base"
)

// gConnId is the counter for all connections created to the cluster
var gConnId int64

// Connection is the abstract connection to the cluster
type Connection interface {
	io.Closer

	// Id is the unique id for the connection. This is useful when tracking connections
	// in logs when they are generated and released back to the pool
	Id() int64

	// SetMeta sets internal metadata for doc. The call is synchronous.
	SetMeta(key string, val []byte, dataType uint8, target base.ConflictLogTarget) (err error)
}

// NewConnId generates new unique connection Id. This is used by the implementations
// of the Connection interface
func NewConnId() int64 {
	return atomic.AddInt64(&gConnId, 1)
}
