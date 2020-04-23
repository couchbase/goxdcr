// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBackfillReplMarshal(t *testing.T) {
	fmt.Println("============== Test case start: TestBackfillReplMarshal =================")
	assert := assert.New(t)

	namespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}

	vb0Task0 := NewBackfillTask(ts0, namespaceMapping)

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	vb0Task1 := NewBackfillTask(ts1, namespaceMapping)
	_, err := json.Marshal(vb0Task0)
	assert.Nil(err)

	var vb0Tasks BackfillTasks
	vb0Tasks = append(vb0Tasks, vb0Task0)
	vb0Tasks = append(vb0Tasks, vb0Task1)

	ts2 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{1, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{1, 0, 5000, 500, 500, manifestsIdPair},
	}
	vb1Task0 := NewBackfillTask(ts2, namespaceMapping)

	var vb1Tasks BackfillTasks
	vb1Tasks = append(vb1Tasks, vb1Task0)

	_, err = json.Marshal(vb1Tasks)
	assert.Nil(err)

	vbTasksMap := make(map[uint16]*BackfillTasks)
	vbTasksMap[0] = &vb0Tasks
	vbTasksMap[1] = &vb1Tasks

	testId := "testId"
	testInternalId := "testInternalId"
	testSpec := &BackfillReplicationSpec{
		Id:         testId,
		InternalId: testInternalId,
		VBTasksMap: vbTasksMap,
	}

	marshalledSpec, err := json.Marshal(testSpec)
	assert.Nil(err)

	checkSpec := &BackfillReplicationSpec{
		VBTasksMap: make(map[uint16]*BackfillTasks),
	}
	err = json.Unmarshal(marshalledSpec, &checkSpec)
	assert.Nil(err)
	assert.True(checkSpec.SameAs(testSpec))

	assert.Equal(2, len(*(testSpec.VBTasksMap[0])))
	assert.Equal(1, len(*(testSpec.VBTasksMap[1])))
	assert.Nil(testSpec.VBTasksMap[2])

	// Test append
	vb0TasksClone := vb0Tasks.Clone()
	ts0_append := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 7000, 700, 700, manifestsIdPair},
	}
	vb0AppendTask0 := NewBackfillTask(ts0_append, namespaceMapping)
	ts2_append := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{1, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{1, 0, 10000, 1000, 1000, manifestsIdPair},
	}
	vb1AppendTask0 := NewBackfillTask(ts2_append, namespaceMapping)
	var vb0TasksAppend BackfillTasks
	vb0TasksAppend = append(vb0TasksAppend, vb0AppendTask0)
	var vb1TasksAppend BackfillTasks
	vb1TasksAppend = append(vb1TasksAppend, vb1AppendTask0)
	appendTasksMap := make(map[uint16]*BackfillTasks)
	appendTasksMap[0] = &vb0TasksAppend
	appendTasksMap[1] = &vb1TasksAppend
	// Add an create for good measure
	appendTasksMap[2] = &vb0TasksClone

	testSpec.AppendTasks(appendTasksMap)

	assert.Equal(3, len(*(testSpec.VBTasksMap[0])))
	assert.Equal(2, len(*(testSpec.VBTasksMap[1])))
	assert.Equal(2, len(*(testSpec.VBTasksMap[2])))
	fmt.Println("============== Test case end: TestBackfillReplMarshal =================")
}
