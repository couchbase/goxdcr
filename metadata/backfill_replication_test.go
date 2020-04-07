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

	fmt.Println("============== Test case end: TestBackfillReplMarshal =================")
}
