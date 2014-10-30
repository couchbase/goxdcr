// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_svc

import (
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	base "github.com/Xiaomei-Zhang/goxdcr/base"
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

func (ckmgr *CheckpointManager) StartSequenceNum(topic string) []uint64 {
	//TODO: implement
	ret := make([]uint64, 1024)
	for i := 0; i < 1024; i++ {
		ret[i] = 0
	}
	return ret
}

func (ckmgr *CheckpointManager) VBTimestamps(topic string) map[uint16]*base.VBTimestamp {
	//TODO: implement
    ret := make(map[uint16]*base.VBTimestamp)
    for i := 0; i < 1024; i++ {
    	vbts := &base.VBTimestamp{}
        vbts.Vbno = uint16(i)
        ret[uint16(i)] = vbts
    }
    return ret
}
