package parts

import (
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
)

type XDCRPart interface {
	common.Part
	HeartBeat_sync() bool
	HeartBeat_async(respchan chan []interface{}) error
}
