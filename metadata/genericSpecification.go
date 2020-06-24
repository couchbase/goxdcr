package metadata

type GenericSpecification interface {
	SameSpecGeneric(other GenericSpecification) bool
	CloneGeneric() GenericSpecification
	RedactGeneric() GenericSpecification

	GetReplicationSpec() *ReplicationSpecification
	GetBackfillSpec() *BackfillReplicationSpec
	Type() ReplicationType
	GetFullId() string
}

type ReplicationType int

func (t ReplicationType) String() string {
	switch t {
	case MainReplication:
		return "MainReplication"
	case BackfillReplication:
		return "BackfillReplication"
	default:
		return "?? (ReplicationType)"
	}
}

const (
	MainReplication     ReplicationType = iota
	BackfillReplication ReplicationType = iota
)
