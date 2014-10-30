package parts

import (
	"github.com/Xiaomei-Zhang/goxdcr/common"
)

type XDCRPart interface {
	common.Part
	HeartBeat_sync() bool
	HeartBeat_async(respchan chan []interface{}) error
}
