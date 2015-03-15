// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/common"
)

type ThroughSeqnoTrackerSvc interface {
	Attach(pipeline common.Pipeline) error
	// get through seqno for a vb
	GetThroughSeqno(vbno uint16) uint64
	// get through seqnos for all vbs managed by the pipeline
	GetThroughSeqnos() map[uint16]uint64
	SetStartSeqnos(start_seqno_map map[uint16]uint64)
}
