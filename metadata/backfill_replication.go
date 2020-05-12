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
	"fmt"
	"github.com/couchbase/goxdcr/base"
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
	VBTasksMap VBTasksMapType

	// revision number to be used by metadata service. not included in json
	revision interface{}
}

func NewBackfillReplicationSpec(id, internalId string, vbTasksMap VBTasksMapType, parentSpec *ReplicationSpecification) *BackfillReplicationSpec {
	return &BackfillReplicationSpec{
		Id:               id,
		InternalId:       internalId,
		VBTasksMap:       vbTasksMap,
		replicationSpec_: parentSpec,
	}
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
func (b *BackfillReplicationSpec) MergeNewTasks(vbTasksMap VBTasksMapType, skipFirst bool) {
	for vb, newTasks := range vbTasksMap {
		_, exists := b.VBTasksMap[vb]
		if !exists {
			b.VBTasksMap[vb] = newTasks
			continue
		}

		if newTasks == nil {
			continue
		}

		// Need to merge
		for _, newTask := range *newTasks {
			mergeCopy := *(b.VBTasksMap[vb])
			firstTask := mergeCopy[0]
			var unmergableTasks BackfillTasks
			if skipFirst {
				mergeCopy = mergeCopy[1:]
			}
			mergeCopy.MergeIncomingTaskIntoTasks(newTask, &unmergableTasks)

			for _, unmergableTask := range unmergableTasks {
				mergeCopy = append(mergeCopy, unmergableTask)
			}

			if skipFirst {
				// Need to prepend first task back
				mergeCopy = append(mergeCopy, nil)
				copy(mergeCopy[1:], mergeCopy[0:])
				mergeCopy[0] = firstTask
			}

			// Just replace
			b.VBTasksMap[vb] = &mergeCopy
		}
	}
}

// Given a list of tasks for VBs, add (append) them to the list of currently existing tasks
func (b *BackfillReplicationSpec) AppendTasks(vbTasksMap VBTasksMapType) {
	if b == nil {
		return
	}

	for vb, tasks := range vbTasksMap {
		_, exists := b.VBTasksMap[vb]
		if !exists {
			b.VBTasksMap[vb] = tasks
		} else {
			*(b.VBTasksMap[vb]) = append(*(b.VBTasksMap[vb]), (*tasks)...)
		}
	}
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

type VBTasksMapType map[uint16]*BackfillTasks

// Given a list of VBs and a namespace mapping, create a VBTasksMap that includes this this mapping
func NewBackfillVBTasksMap(namespaceMap CollectionNamespaceMapping, vbs []uint16, endSeqnos map[uint16]uint64) (VBTasksMapType, error) {
	var vbTasksMap VBTasksMapType

	if len(vbs) == 0 {
		return vbTasksMap, base.ErrorNoSourceKV
	}

	vbTasksMap = make(VBTasksMapType)
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

		var tasks BackfillTasks
		task := NewBackfillTask(timestamps, []CollectionNamespaceMapping{namespaceMap})
		tasks = append(tasks, task)
		vbTasksMap[vb] = &tasks
	}
	return vbTasksMap, nil
}

func (this VBTasksMapType) SameAs(other VBTasksMapType) bool {
	if len(this) != len(other) {
		return false
	}

	for k, v := range this {
		otherv, exists := other[k]
		if !exists {
			return false
		}
		if !v.SameAs(*otherv) {
			return false
		}
	}

	return true
}

func (this VBTasksMapType) Clone() VBTasksMapType {
	clonedMap := make(VBTasksMapType)
	for k, v := range this {
		clonedTasks := v.Clone()
		clonedMap[k] = &clonedTasks
	}
	return clonedMap
}

// TODO Expensive - optimize later
func (v *VBTasksMapType) LoadFromMappingsShaMap(shaToCollectionNsMap ShaToCollectionNamespaceMap) error {
	if v == nil {
		return base.ErrorInvalidInput
	}

	errMap := make(base.ErrorMap)
	for _, tasks := range *v {
		for _, task := range *tasks {
			task.requestedCollections_ = task.requestedCollections_[:0]
			for _, oneRequestedCollectionSha := range task.RequestedCollectionsShas {
				collectionNsMapping, exists := shaToCollectionNsMap[oneRequestedCollectionSha]
				if !exists {
					errMap[oneRequestedCollectionSha] = base.ErrorNotFound
					continue
				}
				task.requestedCollections_ = append(task.requestedCollections_, *collectionNsMapping)
			}
		}
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

	returnMap := make(ShaToCollectionNamespaceMap)
	for _, tasks := range *v {
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

// Backfill tasks are ordered list of backfill jobs, and to be handled in sequence
type BackfillTasks []*BackfillTask

func (b BackfillTasks) Clone() BackfillTasks {
	var clonedTasks BackfillTasks
	for _, task := range b {
		if task == nil {
			continue
		}
		oneClonedTask := task.Clone()
		clonedTasks = append(clonedTasks, &oneClonedTask)
	}
	return clonedTasks
}

func (b BackfillTasks) SameAs(other BackfillTasks) bool {
	if len(b) != len(other) {
		return false
	}

	for i := 0; i < len(b); i++ {
		if !b[i].SameAs(other[i]) {
			return false
		}
	}
	return true
}

// Returns a map of sha -> mapping
func (b BackfillTasks) GetAllCollectionNamespaceMappings() ShaToCollectionNamespaceMap {
	returnMap := make(ShaToCollectionNamespaceMap)
	for _, task := range b {
		if task == nil {
			continue
		}
		for i, requestedSha := range task.RequestedCollectionsShas {
			if _, exists := returnMap[requestedSha]; !exists {
				requestedCols := task.RequestedCollections()[i]
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
 */
func (b *BackfillTasks) MergeIncomingTaskIntoTasks(task *BackfillTask, unmergableTasks *BackfillTasks) {
	if b == nil || len(*b) == 0 {
		(*unmergableTasks) = append(*unmergableTasks, task)
		return
	}

	fullyMerged, unableToMerge, subTask1, subTask2 := ((*b)[0]).MergeIncomingTask(task)
	if fullyMerged {
		return
	}
	if unableToMerge {
		(*unmergableTasks) = append((*unmergableTasks), task)
		return
	}

	nextBackfillTasks := (*b)[1:]
	nextBackfillTasks.MergeIncomingTaskIntoTasks(subTask1, unmergableTasks)
	nextBackfillTasks.MergeIncomingTaskIntoTasks(subTask2, unmergableTasks)
	return
}

func (b BackfillTasks) containsStartEndRange(startSeqno, endSeqno uint64) bool {
	for _, task := range b {
		if task.Timestamps.StartingTimestamp.Seqno == startSeqno && task.Timestamps.EndingTimestamp.Seqno == endSeqno {
			return true
		}
	}
	return false
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
}

func NewBackfillTask(ts *BackfillVBTimestamps, requestedCollectionMappings []CollectionNamespaceMapping) *BackfillTask {
	var shas []string
	for _, reqCols := range requestedCollectionMappings {
		shaSlice, _ := reqCols.Sha256()
		shas = append(shas, fmt.Sprintf("%x", shaSlice[:]))
	}
	return &BackfillTask{Timestamps: ts, requestedCollections_: requestedCollectionMappings, RequestedCollectionsShas: shas}
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

	tsAccomodated, tsUnableToMerge, tsSmallerBound, tsLargerBound := b.Timestamps.Accomodate(task.Timestamps)
	if tsUnableToMerge {
		unableToMerge = true
		return
	}

	// Some sort of merge happened
	for _, oneMapping := range task.RequestedCollections() {
		b.AddCollectionNamespaceMapping(oneMapping)
	}

	if tsAccomodated {
		canFullyMerge = true
		return
	}

	// Otherwise, need to pop back out subset of tasks, one before and one after, that wasn't able to be merged
	if !tsSmallerBound.IsEmpty() {
		subtask1 = NewBackfillTask(&tsSmallerBound, task.RequestedCollections())
	}

	if !tsLargerBound.IsEmpty() {
		subtask2 = NewBackfillTask(&tsLargerBound, task.RequestedCollections())
	}

	return
}

func (b *BackfillTask) RequestedCollections() []CollectionNamespaceMapping {
	return b.requestedCollections_
}

func (b *BackfillTask) AddCollectionNamespaceMapping(nsMapping CollectionNamespaceMapping) {
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

func (b *BackfillTask) SameAs(other *BackfillTask) bool {
	if b == nil && other != nil {
		return false
	} else if b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	sortedShas := base.SortStringList(b.RequestedCollectionsShas)
	otherShas := base.SortStringList(other.RequestedCollectionsShas)
	if len(sortedShas) != len(otherShas) {
		return false
	}

	// Save work and just compare digests instead of actually diving into mappings
	for i := 0; i < len(sortedShas); i++ {
		if sortedShas[i] != otherShas[i] {
			return false
		}
	}

	// Last thing to check is the timestamps
	return b.Timestamps.SameAs(other.Timestamps)
}

func (b BackfillTask) Clone() BackfillTask {
	clonedTs := b.Timestamps.Clone()
	var clonedList []CollectionNamespaceMapping
	for _, nsMapping := range b.requestedCollections_ {
		clonedList = append(clonedList, nsMapping.Clone())
	}
	// Sha is automatically calculated
	task := NewBackfillTask(&clonedTs, clonedList)
	return *task
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
