package metadata

import ()

type BackfillReplicationSpec struct {
	//id of the replication
	Id string `json:"id"`

	// Stored backfill request
	BackfillTasks *BackfillPersistInfo `json:"BackfillTasks"`

	// Soft link to corresponding actual ReplicationSpec - not stored as they should be pulled dynamically
	// Should not be nil
	ReplicationSpec *ReplicationSpecification
}

func NewBackfillReplicationSpec(id string,
	tasks *BackfillPersistInfo) *BackfillReplicationSpec {
	return &BackfillReplicationSpec{
		Id:            id,
		BackfillTasks: tasks,
	}
}

func (b *BackfillReplicationSpec) SameSpec(other *BackfillReplicationSpec) bool {
	if b == nil && other != nil || b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	return b.Id == other.Id && b.BackfillTasks.Same(other.BackfillTasks) &&
		b.ReplicationSpec.SameSpec(other.ReplicationSpec)
}

func (b *BackfillReplicationSpec) Clone() *BackfillReplicationSpec {
	if b == nil {
		return nil
	}
	spec := &BackfillReplicationSpec{
		Id:            b.Id,
		BackfillTasks: b.BackfillTasks.Clone(),
	}
	if b.ReplicationSpec != nil {
		spec.ReplicationSpec = b.ReplicationSpec.Clone()
	}
	return spec
}

func (b *BackfillReplicationSpec) Redact() *BackfillReplicationSpec {
	if b != nil && b.ReplicationSpec != nil {
		b.ReplicationSpec.Settings.Redact()
	}
	return b
}

func (b *BackfillReplicationSpec) SameSpecGeneric(other GenericSpecification) bool {
	return b.SameSpec(other.(*BackfillReplicationSpec))
}

func (b *BackfillReplicationSpec) CloneGeneric() GenericSpecification {
	return b.Clone()
}

func (b *BackfillReplicationSpec) RedactGeneric() GenericSpecification {
	return b.Redact()
}
