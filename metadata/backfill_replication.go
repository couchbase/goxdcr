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
}

func (b *BackfillReplicationSpec) SameAs(other *BackfillReplicationSpec) bool {
	return b.Id == other.Id && b.InternalId == other.InternalId && b.VBTasksMap.SameAs(other.VBTasksMap)
}

func (b *BackfillReplicationSpec) SetReplicationSpec(spec *ReplicationSpecification) {
	b.replicationSpec_ = spec
}

func (b *BackfillReplicationSpec) ReplicationSpec() *ReplicationSpecification {
	return b.replicationSpec_
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

type BackfillTask struct {
	// ts contains vbno
	Timestamps           *BackfillVBTimestamps
	RequestedCollections CollectionNamespaceMapping
	// TODO - have a collections mapping service
}

func NewBackfillTask(ts *BackfillVBTimestamps, reqCols CollectionNamespaceMapping) *BackfillTask {
	return &BackfillTask{ts, reqCols}
}

func (b *BackfillTask) SameAs(other *BackfillTask) bool {
	return b.RequestedCollections.IsSame(other.RequestedCollections) && b.Timestamps.SameAs(other.Timestamps)
}

func (b BackfillTask) Clone() BackfillTask {
	clonedTs := b.Timestamps.Clone()
	return BackfillTask{&clonedTs, b.RequestedCollections.Clone()}
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
