// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
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
	ts0.Sanitize()

	vb0Task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	ts1.Sanitize()
	vb0Task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{namespaceMapping})
	_, err := json.Marshal(vb0Task0)
	assert.Nil(err)

	vb0Tasks := NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task0)
	vb0Tasks.List = append(vb0Tasks.List, vb0Task1)

	ts2 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{1, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{1, 0, 5000, 500, 500, manifestsIdPair},
	}
	ts2.Sanitize()
	vb1Task0 := NewBackfillTask(ts2, []CollectionNamespaceMapping{namespaceMapping})

	vb1Tasks := NewBackfillTasks()
	vb1Tasks.List = append(vb1Tasks.List, vb1Task0)

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
		VBTasksMap: NewVBTasksMapWithMTasks(vbTasksMap),
	}

	marshalledSpec, err := json.Marshal(testSpec)
	assert.Nil(err)

	checkSpec := &BackfillReplicationSpec{}
	err = json.Unmarshal(marshalledSpec, &checkSpec)
	checkSpec.PostUnmarshalInit()
	assert.Nil(err)

	// No sha service, so the SameAs checks only the shas
	assert.True(checkSpec.SameAs(testSpec))

	assert.Equal(2, testSpec.VBTasksMap.VBTasksMap[0].Len())
	assert.Equal(1, testSpec.VBTasksMap.VBTasksMap[1].Len())
	assert.Nil(testSpec.VBTasksMap.VBTasksMap[2])
	assert.True(testSpec.VBTasksMap.VBTasksMap[0].Contains(checkSpec.VBTasksMap.VBTasksMap[0]))
	assert.True(testSpec.VBTasksMap.VBTasksMap[0].SameAs(checkSpec.VBTasksMap.VBTasksMap[0]))

	assert.NotNil(checkSpec.VBTasksMap.VBTasksMap[0].mutex)
	assert.NotNil(checkSpec.VBTasksMap.VBTasksMap[0].List[0].mutex)

	fmt.Println("============== Test case end: TestBackfillReplMarshal =================")
}

func TestVBTimestampAccomodate(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestVBTimestampAccomodate =================")

	origBeg := &base.VBTimestamp{Seqno: 10}
	origEnd := &base.VBTimestamp{Seqno: 50}
	origTimestamp := &BackfillVBTimestamps{origBeg, origEnd}

	fullyAccomodateTs := &BackfillVBTimestamps{origBeg, origEnd}
	fullyAccomodated, unableToMerge, smallerOob, largerOob := origTimestamp.Accomodate(fullyAccomodateTs)

	assert.True(fullyAccomodated)
	assert.Nil(smallerOob.StartingTimestamp)
	assert.Nil(smallerOob.EndingTimestamp)
	assert.Nil(largerOob.StartingTimestamp)
	assert.Nil(largerOob.EndingTimestamp)
	assert.False(unableToMerge)

	extraBeg := &base.VBTimestamp{Seqno: 0}
	trailingTail := &BackfillVBTimestamps{extraBeg, origEnd}
	fullyAccomodated, unableToMerge, smallerOob, largerOob = origTimestamp.Accomodate(trailingTail)
	assert.False(fullyAccomodated)
	assert.Nil(largerOob.StartingTimestamp)
	assert.Nil(largerOob.EndingTimestamp)
	assert.Equal(uint64(0), smallerOob.StartingTimestamp.Seqno)
	assert.Equal(origBeg.Seqno, smallerOob.EndingTimestamp.Seqno)
	assert.False(unableToMerge)

	extraEnd := &base.VBTimestamp{Seqno: 60}
	fullyTrail := &BackfillVBTimestamps{extraBeg, extraEnd}
	fullyAccomodated, unableToMerge, smallerOob, largerOob = origTimestamp.Accomodate(fullyTrail)
	assert.False(fullyAccomodated)
	assert.Equal(uint64(0), smallerOob.StartingTimestamp.Seqno)
	assert.Equal(origBeg.Seqno, smallerOob.EndingTimestamp.Seqno)
	assert.Equal(origEnd.Seqno, largerOob.StartingTimestamp.Seqno)
	assert.Equal(extraEnd.Seqno, largerOob.EndingTimestamp.Seqno)
	assert.False(unableToMerge)

	fmt.Println("============== Test case end: TestVBTimestampAccomodate =================")
}

func TestMergeTask(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMergeTask =================")
	namespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	namespaceMapping2 := make(CollectionNamespaceMapping)
	namespace2 := &base.CollectionNamespace{"dummy", "dummy"}
	namespaceMapping2.AddSingleMapping(namespace2, namespace2)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}
	assert.False(ts0.tsSnapIsValid())
	ts0.Sanitize()
	assert.True(ts0.tsSnapIsValid())
	task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 100, 500, 500, manifestsIdPair},
	}
	assert.False(ts1.tsSnapIsValid())
	ts1.Sanitize()
	assert.True(ts1.tsSnapIsValid())
	task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{namespaceMapping})

	canFullyMerge, unableToMerge, subTask1, subTask2 := task0.MergeIncomingTask(task1)
	assert.True(canFullyMerge)
	assert.False(unableToMerge)
	assert.Nil(subTask1)
	assert.Nil(subTask2)

	ts2 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 0, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 100, 500, 500, manifestsIdPair},
	}
	ts2.Sanitize()
	task2 := NewBackfillTask(ts2, []CollectionNamespaceMapping{namespaceMapping2})
	canFullyMerge, unableToMerge, subTask1, subTask2 = task0.MergeIncomingTask(task2)
	assert.False(canFullyMerge)
	assert.False(unableToMerge)
	assert.NotNil(subTask1)
	assert.True(subTask1.Timestamps.tsSnapIsValid())
	assert.Equal(uint64(0), subTask1.Timestamps.StartingTimestamp.Seqno)
	assert.Equal(uint64(5), subTask1.Timestamps.EndingTimestamp.Seqno)
	assert.Nil(subTask2)
	assert.Equal(2, len(task0.requestedCollections_))

	ts3 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5001, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 10000, 500, 500, manifestsIdPair},
	}
	ts3.Sanitize()
	task3 := NewBackfillTask(ts3, []CollectionNamespaceMapping{namespaceMapping})
	canFullyMerge, unableToMerge, subTask1, subTask2 = task0.MergeIncomingTask(task3)
	assert.False(canFullyMerge)
	assert.True(unableToMerge)
	assert.Nil(subTask1)
	assert.Nil(subTask2)
	fmt.Println("============== Test case end: TestMergeTask =================")
}

func TestMergeTasks(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMergeTasks =================")

	namespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}
	ts0.Sanitize()

	vb0Task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	ts1.Sanitize()
	vb0Task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{namespaceMapping})

	totalTasks := NewBackfillTasks()
	totalTasks.List = append(totalTasks.List, vb0Task0)
	totalTasks.List = append(totalTasks.List, vb0Task1)

	// Now try to merge a task that should overlaps twice
	ts2 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 0, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 20000, 500, 500, manifestsIdPair},
	}
	ts2.Sanitize()
	vb0Task2 := NewBackfillTask(ts2, []CollectionNamespaceMapping{namespaceMapping})

	unmergableTasks := NewBackfillTasks()
	assert.Equal(0, unmergableTasks.Len())
	totalTasks.MergeIncomingTaskIntoTasksNoLock(vb0Task2, &unmergableTasks, 0)
	assert.Equal(3, unmergableTasks.Len())

	assert.True(unmergableTasks.containsStartEndRange(0, 5))
	for _, task := range unmergableTasks.List {
		assert.True(task.Timestamps.tsSnapIsValid())
	}
	assert.True(unmergableTasks.containsStartEndRange(5000, 5005))
	assert.True(unmergableTasks.containsStartEndRange(15005, 20000))

	// Now try to merge a task that overlap with the second on the list
	ts3 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5100, 5100, 15100, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15000, 15000, 500, manifestsIdPair},
	}
	ts3.Sanitize()
	vb0Task3 := NewBackfillTask(ts3, []CollectionNamespaceMapping{namespaceMapping})
	unmergableTasks = NewBackfillTasks()
	assert.Equal(0, unmergableTasks.Len())
	totalTasks.MergeIncomingTaskIntoTasksNoLock(vb0Task3, &unmergableTasks, 0)
	assert.Equal(0, unmergableTasks.Len())
	fmt.Println("============== Test case end: TestMergeTasks =================")
}

func TestMergeTasksIntoSpec(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeTasksIntoSpec =================")
	assert := assert.New(t)

	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespace2 := &base.CollectionNamespace{"scope2", "collection2"}
	namespace3 := &base.CollectionNamespace{"scope3", "collection3"}

	namespaceMapping := make(CollectionNamespaceMapping)
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)
	namespaceMapping.AddSingleMapping(namespace2, namespace2)

	defaultNamespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	namespace2Mapping := make(CollectionNamespaceMapping)
	namespace2Mapping.AddSingleMapping(namespace2, namespace2)

	assert.True(namespace2Mapping.IsSubset(namespaceMapping))
	assert.False(namespace2Mapping.IsSame(namespaceMapping))
	assert.True(namespace2Mapping.IsSame(namespace2Mapping))

	superSetMapping := namespaceMapping.Clone()
	superSetMapping.AddSingleMapping(namespace3, namespace3)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}

	vb0Task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	vb0Task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{namespaceMapping})

	vb0Tasks := NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task0)
	vb0Tasks.List = append(vb0Tasks.List, vb0Task1)

	vbTasksMap := make(map[uint16]*BackfillTasks)
	vbTasksMap[0] = &vb0Tasks

	testId := "testId"
	testInternalId := "testInternalId"
	testSpec := &BackfillReplicationSpec{
		Id:         testId,
		InternalId: testInternalId,
		VBTasksMap: NewVBTasksMapWithMTasks(vbTasksMap),
	}

	// This test will also ensure that the first one is skipped to simulate that
	// the first task is undergoing backfill
	// So when the new task comes in, it needs to re-attempt to refill from seqno 5
	newTs := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 20000, 500, 500, manifestsIdPair},
	}
	newVb0Task := NewBackfillTask(newTs, []CollectionNamespaceMapping{namespaceMapping})
	newVb0Tasks := NewBackfillTasks()
	newVb0Tasks.List = append(newVb0Tasks.List, newVb0Task)
	newVbTaskMap := make(map[uint16]*BackfillTasks)
	newVbTaskMap[0] = &newVb0Tasks

	testSpec.MergeNewTasks(NewVBTasksMapWithMTasks(newVbTaskMap), true /*skipFirst*/)

	// The new unmerged tasks are combined with the second on the list
	assert.Equal(3, testSpec.VBTasksMap.VBTasksMap[0].Len())
	// This is the first ongoing backfill
	assert.True(testSpec.VBTasksMap.VBTasksMap[0].containsStartEndRange(5, 5000))
	// Taking the first one away, we are left with
	assert.True(testSpec.VBTasksMap.VBTasksMap[0].containsStartEndRange(5, 5005))
	assert.True(testSpec.VBTasksMap.VBTasksMap[0].containsStartEndRange(5005, 20000))

	// Test removing a namespace
	dummyNamespaceMapping := make(CollectionNamespaceMapping)
	dummyNamespace := base.CollectionNamespace{
		ScopeName:      "Dummy",
		CollectionName: "Dummy",
	}
	dummyNamespaceMapping.AddSingleMapping(&dummyNamespace, &dummyNamespace)
	assert.False(testSpec.VBTasksMap.RemoveNamespaceMappings(dummyNamespaceMapping))
	assert.Equal(3, testSpec.VBTasksMap.VBTasksMap[0].Len())
	oldShaMap := testSpec.VBTasksMap.GetAllCollectionNamespaceMappings()
	assert.Equal(1, len(oldShaMap))

	// Make backups for tests
	testSpec2 := testSpec.Clone()
	testSpec3 := testSpec.Clone()

	assert.True(testSpec.VBTasksMap.RemoveNamespaceMappings(namespaceMapping))
	// Since all the VBTasksMap only contain the namespaceMapping, removing it means the whole
	// backfill spec should be nil
	assert.Nil(testSpec.VBTasksMap.VBTasksMap[0])
	newShaMap := testSpec.VBTasksMap.GetAllCollectionNamespaceMappings()
	assert.Equal(0, len(newShaMap))
	assert.Equal(0, len(testSpec.VBTasksMap.VBTasksMap))

	// Test remove a subset of the namespace mapping
	assert.True(testSpec2.VBTasksMap.RemoveNamespaceMappings(defaultNamespaceMapping))
	checkMapping := testSpec2.VBTasksMap.VBTasksMap[0].GetAllCollectionNamespaceMappings()
	assert.Equal(1, len(checkMapping))
	for _, v := range checkMapping {
		assert.True(v.IsSame(namespace2Mapping))
	}

	// Test remove a superset
	assert.True(testSpec3.VBTasksMap.RemoveNamespaceMappings(superSetMapping))
	assert.Nil(testSpec3.VBTasksMap.VBTasksMap[0])
	newShaMap = testSpec3.VBTasksMap.GetAllCollectionNamespaceMappings()
	assert.Equal(0, len(newShaMap))
	assert.Equal(0, testSpec3.VBTasksMap.Len())

	fmt.Println("============== Test case end: TestMergeTasksIntoSpec =================")
}

func TestSameAsWriteLock(t *testing.T) {
	fmt.Println("============== Test case start: TestSameAsWriteLock =================")
	defer fmt.Println("============== Test case end: TestSameAsWriteLock =================")
	assert := assert.New(t)

	startTs := base.VBTimestamp{}
	endTs := base.VBTimestamp{Seqno: 100}
	backfillTs := BackfillVBTimestamps{StartingTimestamp: &startTs, EndingTimestamp: &endTs}
	oneTask := NewBackfillTask(&backfillTs, []CollectionNamespaceMapping{})

	backfillTasks1 := NewBackfillTasks()
	backfillTasks1.List = append(backfillTasks1.List, oneTask)
	backfillTasks2 := NewBackfillTasks()
	backfillTasks2.List = append(backfillTasks2.List, oneTask)

	assert.True(backfillTasks1.SameAs(&backfillTasks2))
	assert.True(backfillTasks1.SameAs(&backfillTasks2))

	// locks should be unlocked so that lock here should succeed
	backfillTasks1.mutex.Lock()
	backfillTasks2.mutex.Lock()
}

func TestPartialVBMap(t *testing.T) {
	fmt.Println("============== Test case start: TestSameAsWriteLock =================")
	defer fmt.Println("============== Test case end: TestSameAsWriteLock =================")
	assert := assert.New(t)

	vbTaskMap := NewVBTasksMap()
	tasks := NewBackfillTasks()
	tasks.List = append(tasks.List, &BackfillTask{})
	vbTaskMap.VBTasksMap[0] = &tasks
	vbTaskMap.VBTasksMap[1] = &tasks

	assert.Equal(2, vbTaskMap.Len())
	assert.Equal(1, vbTaskMap.LenWithVBs([]uint16{0}))
	assert.Equal(0, vbTaskMap.LenWithVBs([]uint16{2}))
}

func TestDiffTasksCleanup(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSameAsWriteLock =================")
	defer fmt.Println("============== Test case end: TestSameAsWriteLock =================")

	startTs := base.VBTimestamp{}
	endTs := base.VBTimestamp{Seqno: 100}
	backfillTs := BackfillVBTimestamps{StartingTimestamp: &startTs, EndingTimestamp: &endTs}

	nsMapping := make(CollectionNamespaceMapping)
	srcMapping := &base.CollectionNamespace{"S1", "col1"}
	tgtMapping := &base.CollectionNamespace{"S1T", "col1t"}
	nsMapping.AddSingleMapping(srcMapping, tgtMapping)

	nsMapping2 := make(CollectionNamespaceMapping)
	srcMapping2 := &base.CollectionNamespace{"S2", "col2"}
	tgtMapping2 := &base.CollectionNamespace{"S2T", "col2t"}
	nsMapping2.AddSingleMapping(srcMapping2, tgtMapping2)

	// Do a "layering of tasks"
	oneTask := NewBackfillTask(&backfillTs, []CollectionNamespaceMapping{nsMapping})
	duoTask := NewBackfillTask(&backfillTs, []CollectionNamespaceMapping{nsMapping, nsMapping2})

	// One Layer of task
	uniLayerTasks := NewBackfillTasks()
	uniLayerTasks.List = append(uniLayerTasks.List, oneTask)

	duoLayerTasks := NewBackfillTasks()
	duoLayerTasks.List = append(duoLayerTasks.List, duoTask)

	// VB 0 has one layer, VB1 has two layers
	vbTaskMap := NewVBTasksMap()
	vbTaskMap.VBTasksMap[0] = &uniLayerTasks
	vbTaskMap.VBTasksMap[1] = &duoLayerTasks

	modified := vbTaskMap.RemoveNamespaceMappings(nsMapping)
	assert.True(modified)

	assert.Nil(vbTaskMap.VBTasksMap[0])
	assert.NotNil(vbTaskMap.VBTasksMap[1])

	modified = vbTaskMap.RemoveNamespaceMappings(nsMapping2)
	assert.True(modified)

	assert.Nil(vbTaskMap.VBTasksMap[0])
	assert.Nil(vbTaskMap.VBTasksMap[1])
	assert.Equal(0, vbTaskMap.Len())
}

// A few MBs were filed where the "MergeNewTasks" call would deadlock even though all the locking mechanisms
// were in place correctly (Read lock -> Read unlock, lock->unlock, locking order, etc)
// This unit test was written to test a theory that when a same task that is being merged again, a deadlock could happen
// This is because the in-memory backfill task for a VB is empty, and we simply shallow-copy the incoming task being
// merged as the first pass.
// When the second pass occurs, what happens is that the original shallow copy will actually point to the same obj
// and a deadlock will occur
func TestIdenticalReMerge(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestIdenticalReMerge =================")
	defer fmt.Println("============== Test case end: TestIdenticalReMerge =================")

	// Copied from test above
	startTs := base.VBTimestamp{}
	endTs := base.VBTimestamp{Seqno: 100}
	backfillTs := BackfillVBTimestamps{StartingTimestamp: &startTs, EndingTimestamp: &endTs}

	nsMapping := make(CollectionNamespaceMapping)
	srcMapping := &base.CollectionNamespace{"S1", "col1"}
	tgtMapping := &base.CollectionNamespace{"S1T", "col1t"}
	nsMapping.AddSingleMapping(srcMapping, tgtMapping)

	nsMapping2 := make(CollectionNamespaceMapping)
	srcMapping2 := &base.CollectionNamespace{"S2", "col2"}
	tgtMapping2 := &base.CollectionNamespace{"S2T", "col2t"}
	nsMapping2.AddSingleMapping(srcMapping2, tgtMapping2)

	// Do a "layering of tasks"
	oneTask := NewBackfillTask(&backfillTs, []CollectionNamespaceMapping{nsMapping})
	duoTask := NewBackfillTask(&backfillTs, []CollectionNamespaceMapping{nsMapping, nsMapping2})

	// One Layer of task
	uniLayerTasks := NewBackfillTasks()
	uniLayerTasks.List = append(uniLayerTasks.List, oneTask)

	duoLayerTasks := NewBackfillTasks()
	duoLayerTasks.List = append(duoLayerTasks.List, duoTask)

	// VB 0 has one layer, VB1 has two layers
	vbTaskMap := NewVBTasksMap()
	vbTaskMap.VBTasksMap[0] = &uniLayerTasks
	vbTaskMap.VBTasksMap[1] = &duoLayerTasks

	backfillSpec := NewBackfillReplicationSpec("testId", "intID", nil, nil, 0)
	assert.NotNil(backfillSpec)

	// First pass should work
	backfillSpec.MergeNewTasks(vbTaskMap, false)
	// Without fix, second pass will deadlock
	backfillSpec.MergeNewTasks(vbTaskMap, false)
}

func TestRollbackTo0(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestRollbackTo0 =================")
	defer fmt.Println("============== Test case end: TestRollbackTo0 =================")

	namespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 500, manifestsIdPair},
	}
	ts0.Sanitize()

	vb0Task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	differentNsMapping := make(CollectionNamespaceMapping)
	diffTargetNamespace := &base.CollectionNamespace{"nonDefaultScope", "nonDefaultCollection"}
	differentNsMapping.AddSingleMapping(defaultNamespace, diffTargetNamespace)
	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5005, 10, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 15005, 500, 500, manifestsIdPair},
	}
	ts1.Sanitize()
	vb0Task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{differentNsMapping})

	totalTasks := NewBackfillTasks()
	totalTasks.List = append(totalTasks.List, vb0Task0)
	totalTasks.List = append(totalTasks.List, vb0Task1)

	// Rolling back to 0 will have a single range + 2 mappings
	totalTasks.RollbackTo0(0)
	assert.Equal(1, totalTasks.Len())
	backfillTask, exists, unlockFunc := totalTasks.GetRO(0)
	assert.True(exists)
	assert.Equal(uint64(0), backfillTask.GetStartingTimestampSeqno())
	assert.Equal(uint64(15005), backfillTask.GetEndingTimestampSeqno())
	unlockFunc()
	assert.Len(totalTasks.GetAllCollectionNamespaceMappings(), 2)
}

func TestBackfillVBTimestamps_IsValidForStart(t *testing.T) {
	type fields struct {
		StartingTimestamp *base.VBTimestamp
		EndingTimestamp   *base.VBTimestamp
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "Valid start",
			fields: fields{
				StartingTimestamp: &base.VBTimestamp{Seqno: 0},
				EndingTimestamp:   &base.VBTimestamp{Seqno: 6},
			},
			want: true,
		},
		{
			name: "Invalid for ckptmgr start",
			fields: fields{
				StartingTimestamp: nil,
				EndingTimestamp:   &base.VBTimestamp{Seqno: 5},
			},
			want: false,
		},
		{
			name: "Invalid for ckptmgr start2",
			fields: fields{
				StartingTimestamp: &base.VBTimestamp{Seqno: 5},
				EndingTimestamp:   nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BackfillVBTimestamps{
				StartingTimestamp: tt.fields.StartingTimestamp,
				EndingTimestamp:   tt.fields.EndingTimestamp,
			}
			assert.Equalf(t, tt.want, b.IsValidForStart(), "IsValidForStart()")
		})
	}
}

func TestMergeTasksIntoSpecWithNilTask(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeTasksIntoSpecWithNilTask =================")
	defer fmt.Println("============== Test case end: TestMergeTasksIntoSpecWithNilTask =================")
	assert := assert.New(t)

	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespace2 := &base.CollectionNamespace{"scope2", "collection2"}
	namespace3 := &base.CollectionNamespace{"scope3", "collection3"}

	namespaceMapping := make(CollectionNamespaceMapping)
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)
	namespaceMapping.AddSingleMapping(namespace2, namespace2)

	defaultNamespaceMapping := make(CollectionNamespaceMapping)
	defaultNamespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)

	namespace2Mapping := make(CollectionNamespaceMapping)
	namespace2Mapping.AddSingleMapping(namespace2, namespace2)

	assert.True(namespace2Mapping.IsSubset(namespaceMapping))
	assert.False(namespace2Mapping.IsSame(namespaceMapping))
	assert.True(namespace2Mapping.IsSame(namespace2Mapping))

	superSetMapping := namespaceMapping.Clone()
	superSetMapping.AddSingleMapping(namespace3, namespace3)

	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}
	ts0 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 0, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 6000, manifestsIdPair},
	}

	vb0Task0 := NewBackfillTask(ts0, []CollectionNamespaceMapping{namespaceMapping})

	vb0Tasks := NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task0)

	vbTasksMap := make(map[uint16]*BackfillTasks)
	vbTasksMap[0] = &vb0Tasks

	testId := "testId"
	testInternalId := "testInternalId"
	testSpec := &BackfillReplicationSpec{
		Id:         testId,
		InternalId: testInternalId,
		VBTasksMap: NewVBTasksMapWithMTasks(vbTasksMap),
	}

	newTs := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 6, 0, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5005, 500, 6000, manifestsIdPair},
	}
	newVb0Task := NewBackfillTask(newTs, []CollectionNamespaceMapping{namespaceMapping})
	newVb0Tasks := NewBackfillTasks()
	newVb0Tasks.List = append(newVb0Tasks.List, newVb0Task)
	newVbTaskMap := make(map[uint16]*BackfillTasks)
	newVbTaskMap[0] = &newVb0Tasks

	for _, task := range testSpec.VBTasksMap.VBTasksMap[0].List {
		assert.NotNil(task)
	}
	testSpec.MergeNewTasks(NewVBTasksMapWithMTasks(newVbTaskMap), false /*skipFirst*/)
	for _, task := range testSpec.VBTasksMap.VBTasksMap[0].List {
		assert.NotNil(task)
	}

	ts1 := &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 0, 10, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 5000, 500, 6000, manifestsIdPair},
	}

	vb0Task1 := NewBackfillTask(ts1, []CollectionNamespaceMapping{namespaceMapping})

	vb0Tasks = NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task1)

	vbTasksMap = make(map[uint16]*BackfillTasks)
	vbTasksMap[0] = &vb0Tasks

	testSpec = &BackfillReplicationSpec{
		Id:         testId,
		InternalId: testInternalId,
		VBTasksMap: NewVBTasksMapWithMTasks(vbTasksMap),
	}

	newTs = &BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 0, 0, 5, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 10, 7, 500, manifestsIdPair},
	}
	newVb1Task := NewBackfillTask(newTs, []CollectionNamespaceMapping{namespaceMapping})
	newVb1Tasks := NewBackfillTasks()
	newVb1Tasks.List = append(newVb0Tasks.List, newVb1Task)
	newVbTaskMap = make(map[uint16]*BackfillTasks)
	newVbTaskMap[0] = &newVb0Tasks

	for _, task := range testSpec.VBTasksMap.VBTasksMap[0].List {
		assert.NotNil(task)
	}
	testSpec.MergeNewTasks(NewVBTasksMapWithMTasks(newVbTaskMap), false /*skipFirst*/)
	for _, task := range testSpec.VBTasksMap.VBTasksMap[0].List {
		assert.NotNil(task)
	}
}
