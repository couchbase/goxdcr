// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"sync"
)

type BackfillReplicationSpec struct {
	// Id of the ReplicationSpec - as BackfillReplicationSpec is a "child" under the "parent" context of a ReplicationSpec
	Id string `json:"id"`

	// Stored InternalID of the parent-replicationSpec
	InternalId string `json:"internalId"`

	// Soft link to corresponding actual ReplicationSpec - not stored as they should be pulled dynamically
	// Should not be nil, and should match signatures above
	replicationSpec_ *ReplicationSpecification

	// Backfill Replication spec is composed of jobs per VB that needs to be handled
	// For vb's that have backfill needs, each vb contains a list of ordered jobs to backfill
	VBTasksMap *VBTasksMapType

	// revision number to be used by metadata service. not included in json
	revision interface{}
}

func NewBackfillReplicationSpec(id, internalId string, vbTasksMap *VBTasksMapType, parentSpec *ReplicationSpecification) *BackfillReplicationSpec {
	spec := &BackfillReplicationSpec{
		Id:               id,
		InternalId:       internalId,
		VBTasksMap:       vbTasksMap,
		replicationSpec_: parentSpec,
	}
	spec.PostUnmarshalInit()
	return spec
}

func (b *BackfillReplicationSpec) PostUnmarshalInit() {
	if b.VBTasksMap.IsNil() {
		taskMap := NewVBTasksMap()
		b.VBTasksMap = taskMap
	}
	b.VBTasksMap.PostUnmarshalInit()
}

func (b *BackfillReplicationSpec) SameAs(other *BackfillReplicationSpec) bool {
	if b == nil && other == nil {
		return true
	} else if (b == nil && other != nil) || (b != nil && other == nil) {
		return false
	} else {
		return b.Id == other.Id && b.InternalId == other.InternalId && b.VBTasksMap.SameAs(other.VBTasksMap)
	}
}

func (b *BackfillReplicationSpec) GetReplicationSpec() *ReplicationSpecification {
	return b.ReplicationSpec()
}

func (b *BackfillReplicationSpec) GetBackfillSpec() *BackfillReplicationSpec {
	return b
}

func (b *BackfillReplicationSpec) SetReplicationSpec(spec *ReplicationSpecification) {
	b.replicationSpec_ = spec
}

func (b *BackfillReplicationSpec) Type() ReplicationType {
	return BackfillReplication
}

func (b *BackfillReplicationSpec) GetFullId() string {
	return base.CompileBackfillPipelineSpecId(b.Id)
}

func (b *BackfillReplicationSpec) ReplicationSpec() *ReplicationSpecification {
	return b.replicationSpec_
}

func (b *BackfillReplicationSpec) Revision() interface{} {
	return b.revision
}

func (b *BackfillReplicationSpec) SetRevision(rev interface{}) {
	b.revision = rev
}

func (b *BackfillReplicationSpec) Clone() *BackfillReplicationSpec {
	if b == nil {
		return nil
	}
	clonedSpec := &BackfillReplicationSpec{
		Id:         b.Id,
		InternalId: b.InternalId,
	}
	if b.replicationSpec_ != nil {
		clonedSpec.replicationSpec_ = b.replicationSpec_.Clone()
	}
	clonedSpec.VBTasksMap = b.VBTasksMap.Clone()
	return clonedSpec
}

func (b *BackfillReplicationSpec) Redact() *BackfillReplicationSpec {
	if b != nil && b.replicationSpec_ != nil {
		b.replicationSpec_.Settings.Redact()
	}
	return b
}

// Given a specifc new task, efficiently modify the current spec's task to incorporate the incoming tasks
// The idea is to let the new tasks coming in piggy back off of whatever tasks ranges there are by merging
// into the exist ones, and then whatever is leftover not covered is then appended at the end
// If skipFirst is set, then the first task in existence is not modified
// It's possible that the first task is actively being fulfilled
// Keeps the current tasks in order, and whatever ask cannot be incorporated then append a new task
// to cover it
func (b *BackfillReplicationSpec) MergeNewTasks(vbTasksMap *VBTasksMapType, skipFirst bool) {
	vbTasksMap.mutex.Lock()
	defer vbTasksMap.mutex.Unlock()

	for vb, newTasks := range vbTasksMap.VBTasksMap {
		if newTasks == nil {
			continue
		}

		backfillTasksForVB, exists, unlockFunc := b.VBTasksMap.Get(vb, true)
		if !exists {
			b.VBTasksMap.VBTasksMap[vb] = newTasks
			unlockFunc()
			continue
		}

		newTasks.mutex.Lock()
		// Need to merge
		for _, newTask := range newTasks.List {
			var unmergableTasks BackfillTasks
			var index int
			if skipFirst {
				index++
			}

			backfillTasksForVB.mutex.Lock()
			backfillTasksForVB.MergeIncomingTaskIntoTasksNoLock(newTask, &unmergableTasks, index)

			for _, unmergableTask := range unmergableTasks.List {
				backfillTasksForVB.List = append(backfillTasksForVB.List, unmergableTask)
			}

			backfillTasksForVB.mutex.Unlock()
		}
		newTasks.mutex.Unlock()
		unlockFunc()
	}
}

// When traffic is bursty, it's possible that multiple routers will be raising the same task
// Returns true if incoming taskmap duplicates a portion of current spec
func (b *BackfillReplicationSpec) Contains(vbTasksMap *VBTasksMapType) bool {
	b.VBTasksMap.mutex.RLock()
	defer b.VBTasksMap.mutex.RUnlock()
	for vb, newTasks := range vbTasksMap.VBTasksMap {
		currentTasks, exists, unlockFunc := b.VBTasksMap.Get(vb, false)
		if !exists {
			unlockFunc()
			return false
		}

		if !currentTasks.Contains(newTasks) {
			unlockFunc()
			return false
		}
		unlockFunc()
	}
	return true
}

func (b *BackfillReplicationSpec) SameSpecGeneric(other GenericSpecification) bool {
	return b.SameAs(other.(*BackfillReplicationSpec))
}

func (b *BackfillReplicationSpec) CloneGeneric() GenericSpecification {
	return b.Clone()
}

func (b *BackfillReplicationSpec) RedactGeneric() GenericSpecification {
	return b.Redact()
}

type VBTasksMapType struct {
	VBTasksMap map[uint16]*BackfillTasks
	mutex      *sync.RWMutex
}

func NewVBTasksMap() *VBTasksMapType {
	taskMap := make(map[uint16]*BackfillTasks)
	obj := VBTasksMapType{
		VBTasksMap: taskMap,
		mutex:      &sync.RWMutex{},
	}
	return &obj
}

func NewVBTasksMapWithMTasks(taskMap map[uint16]*BackfillTasks) *VBTasksMapType {
	return &VBTasksMapType{
		VBTasksMap: taskMap,
		mutex:      &sync.RWMutex{},
	}
}

// Given a list of VBs and a namespace mapping, create a VBTasksMap that includes this this mapping
func NewBackfillVBTasksMap(namespaceMap CollectionNamespaceMapping, vbs []uint16, endSeqnos map[uint16]uint64) (*VBTasksMapType, error) {
	vbTasksMap := NewVBTasksMap()

	if len(vbs) == 0 {
		return vbTasksMap, base.ErrorNoSourceKV
	}

	errorMap := make(base.ErrorMap)
	for _, vb := range vbs {
		endSeqno, exists := endSeqnos[vb]
		if !exists {
			errorMap[fmt.Sprintf("seqno for vb %v", vb)] = base.ErrorNotFound
			continue
		}

		startTs := &base.VBTimestamp{Vbno: vb}
		endTs := &base.VBTimestamp{Vbno: vb, Seqno: endSeqno}
		timestamps := &BackfillVBTimestamps{startTs, endTs}

		tasks := NewBackfillTasks()
		task := NewBackfillTask(timestamps, []CollectionNamespaceMapping{namespaceMap})
		tasks.mutex.Lock()
		tasks.List = append(tasks.List, task)
		tasks.mutex.Unlock()
		vbTasksMap.mutex.Lock()
		vbTasksMap.VBTasksMap[vb] = &tasks
		vbTasksMap.mutex.Unlock()
	}
	return vbTasksMap, nil
}

func (v *VBTasksMapType) IsNil() bool {
	return v == nil || v.VBTasksMap == nil
}

func (v *VBTasksMapType) String() string {
	if v == nil {
		return ""
	}
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return fmt.Sprintf("%v", v.VBTasksMap)
}

func (v *VBTasksMapType) MarshalJSON() ([]byte, error) {
	if v == nil {
		return nil, base.ErrorNilPtr
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return json.Marshal(v.VBTasksMap)
}

func (v *VBTasksMapType) UnmarshalJSON(b []byte) error {
	if v == nil {
		return base.ErrorNilPtr
	}

	if v.mutex == nil {
		v.mutex = &sync.RWMutex{}
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()
	return json.Unmarshal(b, &v.VBTasksMap)
}

func (v *VBTasksMapType) Len() int {
	if v == nil {
		return 0
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()
	return len(v.VBTasksMap)
}

func (v *VBTasksMapType) GetLock() *sync.RWMutex {
	if v == nil {
		// instead of returning a nil lock, return an empty lock
		return &sync.RWMutex{}
	}

	if v.mutex != nil {
		return v.mutex
	} else {
		return &sync.RWMutex{}
	}
}

func (v *VBTasksMapType) ContainsAtLeastOneTask() bool {
	v.mutex.RLock()
	defer v.mutex.RUnlock()
	for _, tasks := range v.VBTasksMap {
		if tasks != nil && tasks.Len() > 0 {
			return true
		}
	}
	return false
}

// Remember to call unlockFunc even if exists is false
func (v *VBTasksMapType) Get(vbno uint16, writeRequested bool) (tasks *BackfillTasks, exists bool, unlockFunc func()) {
	if writeRequested {
		unlockFunc = func() {
			v.mutex.Unlock()
		}
	} else {
		unlockFunc = func() {
			v.mutex.RUnlock()
		}
	}

	if writeRequested {
		v.mutex.Lock()
	} else {
		v.mutex.RLock()
	}

	tasks, exists = v.VBTasksMap[vbno]
	return
}

func (this *VBTasksMapType) SameAs(other *VBTasksMapType) bool {
	if this.Len() != other.Len() {
		return false
	}

	this.mutex.RLock()
	defer this.mutex.RUnlock()
	for k, v := range this.VBTasksMap {
		otherv, exists, unlockFunc := other.Get(k, false)
		if !exists {
			unlockFunc()
			return false
		}
		if !v.SameAs(otherv) {
			unlockFunc()
			return false
		}
		unlockFunc()
	}

	return true
}

func (this *VBTasksMapType) Clone() *VBTasksMapType {
	clonedMap := NewVBTasksMap()
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	for k, v := range this.VBTasksMap {
		clonedTasks := v.Clone()
		clonedMap.VBTasksMap[k] = clonedTasks
	}
	return clonedMap
}

// TODO Expensive - optimize later
func (v *VBTasksMapType) LoadFromMappingsShaMap(shaToCollectionNsMap ShaToCollectionNamespaceMap) error {
	if v == nil {
		return base.ErrorInvalidInput
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	errMap := make(base.ErrorMap)
	for _, tasks := range v.VBTasksMap {
		tasks.mutex.Lock()
		for _, task := range tasks.List {
			task.mutex.Lock()
			task.requestedCollections_ = task.requestedCollections_[:0]
			for _, oneRequestedCollectionSha := range task.RequestedCollectionsShas {
				collectionNsMapping, exists := shaToCollectionNsMap[oneRequestedCollectionSha]
				if !exists {
					errMap[oneRequestedCollectionSha] = base.ErrorNotFound
					continue
				}
				task.requestedCollections_ = append(task.requestedCollections_, *collectionNsMapping)
			}
			task.mutex.Unlock()
		}
		tasks.mutex.Unlock()
	}
	if len(errMap) > 0 {
		return fmt.Errorf("Unable to find the following sha-collection namespace maps: %v", base.FlattenErrorMap(errMap))
	}
	return nil
}

func (v *VBTasksMapType) GetAllCollectionNamespaceMappings() ShaToCollectionNamespaceMap {
	if v == nil {
		return ShaToCollectionNamespaceMap{}
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()

	returnMap := make(ShaToCollectionNamespaceMap)
	for _, tasks := range v.VBTasksMap {
		if tasks == nil {
			continue
		}
		tasksMap := tasks.GetAllCollectionNamespaceMappings()
		for k, v := range tasksMap {
			if _, exists := returnMap[k]; !exists {
				returnMap[k] = v
			}
		}
	}

	return returnMap
}

// When the top VB task is done, remove it from the task list
func (v *VBTasksMapType) MarkOneVBTaskDone(vbno uint16) {
	if v == nil {
		return
	}

	// Start without writeRequest
	tasksPtr, exists, unlockFunc := v.Get(vbno, false)
	if !exists || tasksPtr == nil || tasksPtr.Len() == 0 {
		unlockFunc()
		return
	}

	if tasksPtr.Len() == 1 {
		// need to upgrade lock
		unlockFunc()
		// upgrade lock
		v.mutex.Lock()
		delete(v.VBTasksMap, vbno)
		unlockFunc = func() {
			v.mutex.Unlock()
		}
	} else {
		// Chop off the 0'th element
		tasksPtr.RemoveFirstElem()
	}
	unlockFunc()
}

func (v *VBTasksMapType) RemoveNamespaceMappings(removed CollectionNamespaceMapping) (modified bool) {
	if v == nil {
		return
	}

	var hasKeysToDel bool

	v.mutex.Lock()
	defer v.mutex.Unlock()

	var keysToDel []uint16
	for vbno, backfillTasks := range v.VBTasksMap {
		oneModified := backfillTasks.RemoveNamespaceMappings(removed)
		if oneModified {
			modified = true
			if backfillTasks.Len() == 0 {
				hasKeysToDel = true
				keysToDel = append(keysToDel, vbno)
			}
		}
	}

	if hasKeysToDel {
		for _, vbno := range keysToDel {
			delete(v.VBTasksMap, vbno)
		}
	}
	return
}

func (v *VBTasksMapType) GetTopTasksOnlyClone() *VBTasksMapType {
	retMap := NewVBTasksMap()
	if v == nil || v.Len() == 0 {
		return retMap
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()
	for vb, tasks := range v.VBTasksMap {
		var subTasksList *BackfillTasks
		if tasks != nil && tasks.Len() > 0 {
			subTasksList = tasks.CloneTopTask()
		}
		if subTasksList != nil {
			retMap.VBTasksMap[vb] = subTasksList
		}
	}
	return retMap
}

func (v *VBTasksMapType) DebugString() string {
	var buffer bytes.Buffer
	for i := uint16(0); i < base.NumberOfVbs; i++ {
		tasks, exists, unlockFunc := v.Get(i, false)
		if exists {
			buffer.WriteString(fmt.Sprintf("VB %v : %v", i, tasks.PrettyPrint()))
		}
		unlockFunc()
	}
	return buffer.String()
}

func (v *VBTasksMapType) AllStartsWithSeqno0() bool {
	if v == nil {
		return true
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()
	for _, tasks := range v.VBTasksMap {
		if tasks == nil {
			continue
		}

		tasks.mutex.RLock()
		for _, task := range tasks.List {
			task.mutex.RLock()
			if task.Timestamps == nil || task.Timestamps.StartingTimestamp == nil {
				// Very odd
				continue
			}
			if task.Timestamps.StartingTimestamp.Seqno > 0 {
				task.mutex.RUnlock()
				tasks.mutex.RUnlock()
				return false
			}
			task.mutex.RUnlock()
		}
		tasks.mutex.RUnlock()
	}
	return true
}

func (v *VBTasksMapType) GetDeduplicatedSourceNamespaces() []*SourceNamespace {
	var retList []*SourceNamespace
	var dedupMap = make(CollectionNamespaceMapping)

	v.mutex.RLock()
	defer v.mutex.RUnlock()

	for _, backfillTasks := range v.VBTasksMap {
		if backfillTasks == nil {
			continue
		}
		backfillTasks.mutex.RLock()
		for _, backfillTask := range backfillTasks.List {
			if backfillTask == nil {
				continue
			}

			requestedCollections, unlockFunc := backfillTask.RequestedCollections(false)
			for _, requestedCollection := range requestedCollections {
				for srcNamespace, _ := range requestedCollection {
					dedupMap.AddSingleSourceNsMapping(srcNamespace, &base.CollectionNamespace{})
				}
			}
			unlockFunc()
		}
		backfillTasks.mutex.RUnlock()
	}

	for srcNs, _ := range dedupMap {
		retList = append(retList, srcNs)
	}
	return retList
}

func (v *VBTasksMapType) PostUnmarshalInit() {
	if v.mutex == nil {
		v.mutex = &sync.RWMutex{}
	}

	v.mutex.RLock()
	defer v.mutex.RUnlock()
	for _, tasks := range v.VBTasksMap {
		tasks.PostUnmarshalInit()
	}
}

// Backfill tasks are ordered list of backfill jobs, and to be handled in sequence
type BackfillTasks struct {
	List  []*BackfillTask
	mutex *sync.RWMutex
}

func NewBackfillTasks() BackfillTasks {
	return BackfillTasks{
		List:  []*BackfillTask{},
		mutex: &sync.RWMutex{},
	}
}

func NewBackfillTasksWithTask(incomingTask *BackfillTask) BackfillTasks {
	return BackfillTasks{
		List:  []*BackfillTask{incomingTask},
		mutex: &sync.RWMutex{},
	}
}

func (b *BackfillTasks) GetLock() *sync.RWMutex {
	return b.mutex
}

func (b *BackfillTasks) Append(incoming *BackfillTasks) {
	if b == nil {
		return
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	incoming.mutex.RLock()
	defer incoming.mutex.RUnlock()

	b.List = append(b.List, incoming.List...)
	return
}

func (b *BackfillTasks) RemoveFirstElem() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.List) > 0 {
		b.List = b.List[1:]
	}
}

func (b *BackfillTasks) Clone() *BackfillTasks {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	clonedTasks := NewBackfillTasks()
	for _, task := range b.List {
		if task == nil {
			continue
		}
		oneClonedTask := task.Clone()
		clonedTasks.List = append(clonedTasks.List, &oneClonedTask)
	}
	return &clonedTasks
}

func (b *BackfillTasks) Len() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.List)
}

func (b *BackfillTasks) CloneTopTask() *BackfillTasks {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if len(b.List) == 0 {
		return nil
	}

	topTaskClone := b.List[0].Clone()
	cloneTask := NewBackfillTasksWithTask(&topTaskClone)
	return &cloneTask
}

func (b *BackfillTasks) GetRO(idx int) (*BackfillTask, bool, func()) {
	b.mutex.RLock()

	unlockFunc := func() {
		b.mutex.RUnlock()
	}

	if len(b.List) <= idx {
		return nil, false, unlockFunc
	}
	return b.List[idx], true, unlockFunc
}

func (b *BackfillTasks) SameAs(other *BackfillTasks) bool {
	if b.Len() != other.Len() {
		return false
	}

	bLen := b.Len()

	for i := 0; i < bLen; i++ {
		bElement, exists, unlockB := b.GetRO(i)
		otherElem, otherExists, unlockOther := other.GetRO(i)
		if exists && otherExists {
			if !bElement.SameAs(otherElem) {
				unlockOther()
				unlockB()
				return false
			}
		} else {
			unlockOther()
			unlockB()
			return false
		}
	}
	return true
}

func (b *BackfillTasks) Contains(subsetOfTasks *BackfillTasks) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	subsetOfTasks.mutex.RLock()
	defer subsetOfTasks.mutex.RUnlock()

	for _, task := range subsetOfTasks.List {
		if task == nil {
			continue
		}

		var found bool
		for _, curTask := range b.List {
			if task.Contains(curTask) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (b *BackfillTasks) PrettyPrint() string {
	if b.Len() == 0 {
		return ""
	}

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%v Backfill tasks: ", len(b.List)))
	for _, task := range b.List {
		buffer.WriteString(task.String())
		buffer.WriteString("|")
	}

	return buffer.String()
}

// Returns a map of sha -> mapping
func (b *BackfillTasks) GetAllCollectionNamespaceMappings() ShaToCollectionNamespaceMap {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	returnMap := make(ShaToCollectionNamespaceMap)
	for _, task := range b.List {
		if task == nil {
			continue
		}
		for i, requestedSha := range task.RequestedCollectionsShas {
			if _, exists := returnMap[requestedSha]; !exists {
				requestedCols := task.requestedCollections_[i]
				returnMap[requestedSha] = &requestedCols
			}
		}
	}
	return returnMap
}

/*
 * This uses recursion to try to merge a task into the current list of tasks as much as possible
 * and then returns a list of unmergable tasks (caller needs to allocate)
 * The algorithm simply goes through a list of tasks in order
 * Take the input task, try to merge it from the task in the list
 * If it cannot be merged fully, and has intersection, then break the input task into 3 parts:
 * 1. Left of the intersection
 * 2. Intersection
 * 3. Right of the intersection
 * Take the (1), and run it through the rest of the Backfilltasks recursively
 * Take (3) and run it through the rest of the backfilltasks recursively
 * At the base case, if it cannot be merged, add it to the unmergable tasks
 *
 * For example, given list of 2 existing tasks ranges:
 * 5-5000
 * 5005-15005
 * Try to merge a new task of 0-20000
 * 1. First 5-5000 will soak up the middle, leaving 0-5 (subtask1) and 5000-20000 (subtask2)
 * 2. Run subtask1 through 0-20000, and subtask2 through 0-20000
 * Each subtask recursively will figure out what couldn't be "soaked" up and add the unmergable part to the list on the stack
 *
 * No locking is to be done as this will be called recursively. top level call should lock it accordingly
 */
func (b *BackfillTasks) MergeIncomingTaskIntoTasksNoLock(task *BackfillTask, unmergableTasks *BackfillTasks, index int) {
	if b == nil || len(b.List) == 0 || index >= len(b.List) {
		unmergableTasks.List = append(unmergableTasks.List, task)
		return
	}

	fullyMerged, unableToMerge, subTask1, subTask2 := (b.List[index]).MergeIncomingTask(task)
	if fullyMerged {
		return
	}
	if unableToMerge {
		unmergableTasks.List = append(unmergableTasks.List, task)
		return
	}

	index++
	b.MergeIncomingTaskIntoTasksNoLock(subTask1, unmergableTasks, index)
	b.MergeIncomingTaskIntoTasksNoLock(subTask2, unmergableTasks, index)
	return
}

func (b *BackfillTasks) containsStartEndRange(startSeqno, endSeqno uint64) bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for _, task := range b.List {
		if task.Timestamps.StartingTimestamp.Seqno == startSeqno && task.Timestamps.EndingTimestamp.Seqno == endSeqno {
			return true
		}
	}
	return false
}

func (b *BackfillTasks) RemoveNamespaceMappings(removed CollectionNamespaceMapping) (modified bool) {
	var needCleanup bool
	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, task := range b.List {
		oneModified := task.RemoveCollectionNamespaceMapping(removed)
		if oneModified && !modified {
			modified = true
			if task.RequestedCollectionsLen() == 0 {
				needCleanup = true
			}
		}
	}
	if !needCleanup {
		return
	}

	var doneCleanedUp bool
	for !doneCleanedUp {
		doneCleanedUp = true
		var i int
		var task *BackfillTask
		for i, task = range b.List {
			if task.RequestedCollectionsLen() == 0 {
				doneCleanedUp = false
				break
			}
		}
		if !doneCleanedUp {
			b.List = append((b.List)[:i], (b.List)[i+1:]...)
		}
	}
	return
}

func (b *BackfillTasks) PostUnmarshalInit() {
	if b.mutex == nil {
		b.mutex = &sync.RWMutex{}
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	for _, task := range b.List {
		task.PostUnmarshalInit()
	}

}

// Each backfill task should be RO once created
// And new tasks can be created to split/merge existing ones
type BackfillTask struct {
	// ts contains vbno
	// The timestamps here in the backfill tasks only cares about seqnos
	Timestamps *BackfillVBTimestamps
	// The namespace mappings are NOT stored as part of JSON marshalling
	// They will be stored/retrieved by the ShaRefCounterservice
	// The shas and the actual mappings are linked by the same index
	requestedCollections_    []CollectionNamespaceMapping
	RequestedCollectionsShas []string
	mutex                    *sync.RWMutex
}

func NewBackfillTask(ts *BackfillVBTimestamps, requestedCollectionMappings []CollectionNamespaceMapping) *BackfillTask {
	shas := generateShas(requestedCollectionMappings)
	var clonedMapping []CollectionNamespaceMapping
	for _, orig := range requestedCollectionMappings {
		clonedMapping = append(clonedMapping, orig.Clone())
	}
	return &BackfillTask{Timestamps: ts,
		requestedCollections_:    clonedMapping,
		RequestedCollectionsShas: shas,
		mutex:                    &sync.RWMutex{},
	}
}

func generateShas(requestedCollectionMappings []CollectionNamespaceMapping) []string {
	var shas []string
	for _, reqCols := range requestedCollectionMappings {
		shaSlice, _ := reqCols.Sha256()
		shas = append(shas, fmt.Sprintf("%x", shaSlice[:]))
	}
	return shas
}

func (b *BackfillTask) GetLock() *sync.RWMutex {
	return b.mutex
}

func (b *BackfillTask) GetEndingTimestampSeqno() uint64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.Timestamps != nil && b.Timestamps.EndingTimestamp != nil {
		return b.Timestamps.EndingTimestamp.Seqno
	}
	return 0
}

func (b *BackfillTask) GetStartingTimestampSeqno() uint64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.Timestamps != nil && b.Timestamps.StartingTimestamp != nil {
		return b.Timestamps.StartingTimestamp.Seqno
	}
	return 0
}

func (b *BackfillTask) String() string {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Backfill task (VB %v) [%v,%v) with the following mappings:\n",
		b.Timestamps.StartingTimestamp.Vbno, b.Timestamps.StartingTimestamp.Seqno, b.Timestamps.EndingTimestamp.Seqno))
	for i, col := range b.requestedCollections_ {
		buffer.WriteString(fmt.Sprintf("Sha: %v ", b.RequestedCollectionsShas[i]))
		buffer.WriteString(col.String())
		buffer.WriteString("\n")
	}
	return buffer.String()
}

func (b *BackfillTask) GetTimestampsClone() *BackfillVBTimestamps {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	if b.Timestamps != nil {
		clone := b.Timestamps.Clone()
		return &clone
	} else {
		return nil
	}
}

// When given a new task, try to merge the new tasks's request into this current task
// Potentially returns 2 subtasks
// Subtask1 represents the smaller range of seqno that this task cannot accomodate
// Subtask2 represents the larger range of seqnos that this task cannot accomodate
func (b *BackfillTask) MergeIncomingTask(task *BackfillTask) (canFullyMerge, unableToMerge bool, subtask1, subtask2 *BackfillTask) {
	if b == nil || task == nil {
		unableToMerge = true
		return
	}

	b.mutex.Lock()
	task.mutex.RLock()
	defer b.mutex.Unlock()
	defer task.mutex.RUnlock()

	tsAccomodated, tsUnableToMerge, tsSmallerBound, tsLargerBound := b.Timestamps.Accomodate(task.Timestamps)
	if tsUnableToMerge {
		unableToMerge = true
		return
	}

	// Some sort of merge happened
	for _, oneMapping := range task.requestedCollections_ {
		b.AddCollectionNamespaceMappingNoLock(oneMapping)
	}

	if tsAccomodated {
		canFullyMerge = true
		return
	}

	// Otherwise, need to pop back out subset of tasks, one before and one after, that wasn't able to be merged
	if !tsSmallerBound.IsEmpty() {
		subtask1 = NewBackfillTask(&tsSmallerBound, task.requestedCollections_)
	}

	if !tsLargerBound.IsEmpty() {
		subtask2 = NewBackfillTask(&tsLargerBound, task.requestedCollections_)
	}

	return
}

func (b *BackfillTask) RequestedCollectionsLen() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.requestedCollections_)
}

func (b *BackfillTask) RequestedCollections(writeRequested bool) ([]CollectionNamespaceMapping, func()) {
	unlockFunc := func() {
		if writeRequested {
			b.mutex.Unlock()
		} else {
			b.mutex.RUnlock()
		}
	}
	if writeRequested {
		b.mutex.Lock()
	} else {
		b.mutex.RLock()
	}
	return b.requestedCollections_, unlockFunc
}

func (b *BackfillTask) AddCollectionNamespaceMappingNoLock(nsMapping CollectionNamespaceMapping) {
	incomingSha, _ := nsMapping.Sha256()
	incomingShaStr := fmt.Sprintf("%x", incomingSha)
	for _, checkSha := range b.RequestedCollectionsShas {
		if checkSha == incomingShaStr {
			// Mapping already exists
			return
		}
	}

	b.requestedCollections_ = append(b.requestedCollections_, nsMapping)
	b.RequestedCollectionsShas = append(b.RequestedCollectionsShas, incomingShaStr)
}

func (b *BackfillTask) RemoveCollectionNamespaceMapping(nsMapping CollectionNamespaceMapping) (modified bool) {
	var i int
	var oneMapping CollectionNamespaceMapping

	b.mutex.Lock()
	defer b.mutex.Unlock()

	// There are 3 cases of removal
	// 1. nsMapping contains the same exact mapping as the one in backfillTask
	// 2. nsMapping is a subset of the one in backfillTask
	// 3. nsMapping is a superset of the one in backfillTask

	// First, take care of case 1 - because it requires changing b.requestedCollections_
	for i, oneMapping = range b.requestedCollections_ {
		if oneMapping.IsSame(nsMapping) {
			modified = true
			break
		}
	}

	if modified {
		b.requestedCollections_ = append(b.requestedCollections_[:i], b.requestedCollections_[i+1:]...)
	}

	// Then look for case 3 - because it also requires modification
	var foundCase3Match = true
	for foundCase3Match {
		foundCase3Match = false
		for i, oneMapping = range b.requestedCollections_ {
			if oneMapping.IsSubset(nsMapping) {
				modified = true
				foundCase3Match = true
				break
			}
		}
		if foundCase3Match {
			b.requestedCollections_ = append(b.requestedCollections_[:i], b.requestedCollections_[i+1:]...)
		}
	}

	// Finally, look for case 2, which does not require slice modification
	toBeModifiedMap := make(map[int]*CollectionNamespaceMapping)
	for i, oneMapping = range b.requestedCollections_ {
		if nsMapping.IsSubset(oneMapping) {
			cleanedUpMapping := oneMapping.Delete(nsMapping)
			toBeModifiedMap[i] = &cleanedUpMapping
		}
	}
	if len(toBeModifiedMap) > 0 {
		modified = true
		for i, cleanedUpMapping := range toBeModifiedMap {
			b.requestedCollections_[i] = *cleanedUpMapping
		}
	}

	if modified {
		b.RequestedCollectionsShas = generateShas(b.requestedCollections_)
	}
	return
}

// NOTE - this short circuits and check the SHA's only
// It depends on ShaRefService to correctly restore the mappings
func (b *BackfillTask) Contains(other *BackfillTask) bool {
	if b == nil && other != nil {
		return false
	} else if b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	if b.Timestamps.StartingTimestamp.Seqno > other.Timestamps.StartingTimestamp.Seqno {
		return false
	}

	if b.Timestamps.EndingTimestamp.Seqno < other.Timestamps.EndingTimestamp.Seqno {
		return false
	}

	sortedShas := base.SortStringList(b.RequestedCollectionsShas)
	otherShas := base.SortStringList(other.RequestedCollectionsShas)
	if len(sortedShas) != len(otherShas) {
		return false
	}

	for i := 0; i < len(sortedShas); i++ {
		if sortedShas[i] != otherShas[i] {
			return false
		}
	}

	return true
}

// NOTE - this short circuits and check the SHA's only
// It depends on ShaRefService to correctly restore the mappings
func (b *BackfillTask) SameAs(other *BackfillTask) bool {
	if b == nil && other != nil {
		return false
	} else if b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	b.mutex.RLock()
	defer b.mutex.RUnlock()
	other.mutex.RLock()
	defer other.mutex.RUnlock()
	sortedShas := base.SortStringList(b.RequestedCollectionsShas)
	otherShas := base.SortStringList(other.RequestedCollectionsShas)
	if len(sortedShas) != len(otherShas) {
		return false
	}

	for i := 0; i < len(sortedShas); i++ {
		if sortedShas[i] != otherShas[i] {
			return false
		}
	}

	// Last thing to check is the timestamps
	return b.Timestamps.SameAs(other.Timestamps)
}

func (b *BackfillTask) Clone() BackfillTask {
	clonedTs := b.Timestamps.Clone()
	var clonedList []CollectionNamespaceMapping
	for _, nsMapping := range b.requestedCollections_ {
		clonedList = append(clonedList, nsMapping.Clone())
	}
	// Sha is automatically calculated
	task := NewBackfillTask(&clonedTs, clonedList)
	return *task
}

func (b *BackfillTask) ToDcpNozzleTask(latestSrcManifest *CollectionsManifest) (seqnoEnd uint64, filter *mcc.CollectionsFilter, err error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.Timestamps == nil || b.Timestamps.EndingTimestamp == nil || b.Timestamps.StartingTimestamp == nil || latestSrcManifest == nil {
		err = base.ErrorInvalidInput
		return
	}
	if len(b.requestedCollections_) == 0 {
		err = fmt.Errorf("No collections requested")
		return
	}
	errMap := make(base.ErrorMap)
	seqnoEnd = b.Timestamps.EndingTimestamp.Seqno
	var filterCollectionList []uint32
	for _, requestedCollection := range b.requestedCollections_ {
		for sourceNamespace, _ := range requestedCollection {
			var scopeName string = sourceNamespace.ScopeName
			var collectionName string = sourceNamespace.CollectionName
			if sourceNamespace.GetType() == SourceDefaultCollectionFilter {
				scopeName = base.DefaultScopeCollectionName
				collectionName = base.DefaultScopeCollectionName
			}
			cid, err := latestSrcManifest.GetCollectionId(scopeName, collectionName)
			if err != nil {
				errMap[fmt.Sprintf("Manifest %v looking up %v:%v", latestSrcManifest.Uid(), sourceNamespace.ScopeName, sourceNamespace.CollectionName)] = err
				continue
			}
			filterCollectionList = append(filterCollectionList, cid)
		}
	}
	if len(errMap) > 0 {
		err = fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	filter = &mcc.CollectionsFilter{
		UseManifestUid:  b.Timestamps.StartingTimestamp.Seqno > 0,
		ManifestUid:     b.Timestamps.StartingTimestamp.ManifestIDs.SourceManifestId,
		CollectionsList: filterCollectionList,
	}
	return
}

func (b *BackfillTask) PostUnmarshalInit() {
	if b.mutex == nil {
		b.mutex = &sync.RWMutex{}
	}
}

// This is specifically used to indicate the start and end of a backfill
type BackfillVBTimestamps struct {
	StartingTimestamp *base.VBTimestamp
	EndingTimestamp   *base.VBTimestamp
}

func (b BackfillVBTimestamps) IsEmpty() bool {
	return b.StartingTimestamp == nil && b.EndingTimestamp == nil
}

func (b BackfillVBTimestamps) Clone() BackfillVBTimestamps {
	startClone := b.StartingTimestamp.Clone()
	endClone := b.EndingTimestamp.Clone()
	return BackfillVBTimestamps{&startClone, &endClone}
}

func (b *BackfillVBTimestamps) SameAs(other *BackfillVBTimestamps) bool {
	return b.StartingTimestamp.SameAs(other.StartingTimestamp) &&
		b.EndingTimestamp.SameAs(other.EndingTimestamp)
}

// Given an incoming set of VB Timestamps
// Try its best to see if the incoming timestamps can be accomodated by the current range
// If the incoming timestamps spans more than the current coverage, then
// return 2 ranges:
//    range 1 - range smaller than this b could handle
//    range 2 - range larger than this b could handle
// This call modifies the existing timestamps
func (b *BackfillVBTimestamps) Accomodate(incoming *BackfillVBTimestamps) (fullyAccomodated, unableToMerge bool, smallerOutOfBounds, largerOutOfBounds BackfillVBTimestamps) {
	// Sanity check
	if b == nil || incoming == nil || b.StartingTimestamp.Vbno != incoming.StartingTimestamp.Vbno || b.EndingTimestamp.Vbno != incoming.EndingTimestamp.Vbno {
		return
	}
	var beginningAccomodated bool
	if b.StartingTimestamp.Seqno <= incoming.StartingTimestamp.Seqno {
		if b.EndingTimestamp.Seqno < incoming.StartingTimestamp.Seqno {
			// no intersection
			unableToMerge = true
			return
		}
		// Can accomodate incoming's beginning
		b.StartingTimestamp.ManifestIDs.Accomodate(incoming.StartingTimestamp.ManifestIDs)
		beginningAccomodated = true
	} else {
		if incoming.EndingTimestamp.Seqno < b.StartingTimestamp.Seqno {
			// No intersection
			unableToMerge = true
			return
		}
		// Incoming's starting point is earlier than b's starting point
		smallerOutOfBounds.StartingTimestamp = incoming.StartingTimestamp
		smallerOutOfBounds.EndingTimestamp = b.StartingTimestamp
	}

	if b.EndingTimestamp.Seqno >= incoming.EndingTimestamp.Seqno {
		fullyAccomodated = beginningAccomodated
		// Can fully accomodate seqno - need to ensure manifestID range is covered too
		if fullyAccomodated {
			b.EndingTimestamp.ManifestIDs.Accomodate(incoming.EndingTimestamp.ManifestIDs)
		}
	} else {
		// Incoming's end is longer than b
		largerOutOfBounds.StartingTimestamp = b.EndingTimestamp
		largerOutOfBounds.EndingTimestamp = incoming.EndingTimestamp
	}

	return
}
