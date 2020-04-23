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
		task := NewBackfillTask(timestamps, namespaceMap)
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

func (v *VBTasksMapType) LoadFromMappingsShaMap(shaToCollectionNsMap ShaToCollectionNamespaceMap) error {
	if v == nil {
		return base.ErrorInvalidInput
	}

	errMap := make(base.ErrorMap)
	for _, tasks := range *v {
		for _, task := range *tasks {
			collectionNsMapping, exists := shaToCollectionNsMap[task.RequestedCollectionsSha]
			if !exists {
				errMap[task.RequestedCollectionsSha] = base.ErrorNotFound
				continue
			}
			task.requestedCollections_ = *collectionNsMapping
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
		if _, exists := returnMap[task.RequestedCollectionsSha]; !exists {
			requestedCols := task.RequestedCollections()
			returnMap[task.RequestedCollectionsSha] = &requestedCols
		}
	}
	return returnMap
}

// Each backfill task should be RO once created
// And new tasks can be created to split/merge existing ones
type BackfillTask struct {
	// ts contains vbno
	Timestamps *BackfillVBTimestamps
	// The namespace mapping is NOT stored as part of JSON marshalling
	// It will be stored/retrieved by the ShaRefCounterservice
	requestedCollections_   CollectionNamespaceMapping
	RequestedCollectionsSha string
}

func NewBackfillTask(ts *BackfillVBTimestamps, reqCols CollectionNamespaceMapping) *BackfillTask {
	shaSlice, _ := reqCols.Sha256()
	return &BackfillTask{ts, reqCols, fmt.Sprintf("%x", shaSlice[:])}
}

func (b *BackfillTask) RequestedCollections() CollectionNamespaceMapping {
	return b.requestedCollections_
}

func (b *BackfillTask) SameAs(other *BackfillTask) bool {
	return b.RequestedCollectionsSha == other.RequestedCollectionsSha && b.RequestedCollections().IsSame(other.RequestedCollections()) && b.Timestamps.SameAs(other.Timestamps)
}

func (b BackfillTask) Clone() BackfillTask {
	clonedTs := b.Timestamps.Clone()
	// Sha is automatically calculated
	task := NewBackfillTask(&clonedTs, b.RequestedCollections().Clone())
	return *task
}

// This is specifically used to indicate the start and end of a backfill
type BackfillVBTimestamps struct {
	StartingTimestamp *base.VBTimestamp
	EndingTimestamp   *base.VBTimestamp
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
