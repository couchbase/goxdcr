package service_def

import ()

type CurrentSeqnoGetter func(topic string) (map[uint16]uint64, error)

type BackfillMgrIface interface {
	Start() error
	Stop()
}
