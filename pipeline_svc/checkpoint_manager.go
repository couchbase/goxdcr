package pipeline_svc

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"

)

type CheckpointManager struct {
}

func (ckmgr *CheckpointManager) Attach(pipeline common.Pipeline) error {
	//TODO:
	return nil
}

func (ckmgr *CheckpointManager) Start(settings map[string]interface{}) error {
	//TODO:
	return nil
}

func (ckmgr *CheckpointManager) Stop() error {
	//TODO:
	return nil
}

func (ckmgr *CheckpointManager) StartSequenceNum (topic string) []uint64 {
	//TODO: implement
	ret := make ([]uint64, 1024)
	for i:=0; i<1024; i++ {
		ret[i] = 0
	}
	return ret
}
