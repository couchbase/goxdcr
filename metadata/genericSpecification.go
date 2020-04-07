package metadata

import ()

type GenericSpecification interface {
	SameSpecGeneric(other GenericSpecification) bool
	CloneGeneric() GenericSpecification
	RedactGeneric() GenericSpecification
}
