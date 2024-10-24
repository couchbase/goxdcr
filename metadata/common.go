package metadata

import (
	"github.com/couchbase/goxdcr/v8/base"

	"fmt"
)

type CompressedMappings struct {
	NsMappingRecords CompressedShaMappingList `json:"NsMappingRecords"`

	// internal id of repl spec - for detection of repl spec deletion and recreation event
	SpecInternalId string `json:"specInternalId"`

	//revision number
	revision interface{}
}

func (c *CompressedMappings) SameAs(other *CompressedMappings) bool {
	if c == nil && other != nil {
		return false
	} else if c != nil && other == nil {
		return false
	} else if c == nil && other == nil {
		return true
	}

	return c.SpecInternalId == other.SpecInternalId && c.NsMappingRecords.SameAs(other.NsMappingRecords)
}

func (b *CompressedMappings) LoadShaMap(shaMap map[string]SnappyCompressableVal) error {
	if b == nil {
		return base.ErrorInvalidInput
	}
	errorMap := make(base.ErrorMap)
	b.NsMappingRecords = b.NsMappingRecords[:0]

	for sha, snappyCompressableVal := range shaMap {
		if snappyCompressableVal == nil {
			continue
		}
		compressedMapping, err := snappyCompressableVal.ToSnappyCompressed()
		if err != nil {
			errorMap[sha] = err
			continue
		}

		oneRecord := &CompressedShaMapping{compressedMapping, sha}
		b.NsMappingRecords.SortedInsert(oneRecord)
	}

	if len(errorMap) > 0 {
		return fmt.Errorf("Error LoadingShaMap - sha -> err: %v", base.FlattenErrorMap(errorMap))
	}
	return nil
}

func (b *CompressedMappings) LoadCompressedShaMap(shaMap map[string][]byte) error {
	if b == nil {
		return base.ErrorNilPtr
	}

	errorMap := make(base.ErrorMap)
	b.NsMappingRecords = b.NsMappingRecords[:0]
	for sha, compressedBytes := range shaMap {
		oneRecord := &CompressedShaMapping{compressedBytes, sha}
		b.NsMappingRecords.SortedInsert(oneRecord)
	}

	if len(errorMap) > 0 {
		return fmt.Errorf("Error LoadingCompressedShaMap - sha -> err: %v", base.FlattenErrorMap(errorMap))
	}
	return nil
}

// Append elements in "other" only if it doesn't already exist in "b"
func (b *CompressedMappings) UniqueAppend(other *CompressedMappings) error {
	if b == nil {
		return base.ErrorNilPtr
	}
	if other == nil {
		// Do nothing
		return nil
	}

	bDedup := make(map[string]bool)
	for _, oneRecord := range (*b).NsMappingRecords {
		bDedup[oneRecord.Sha256Digest] = true
	}

	for _, oneRecord := range (*other).NsMappingRecords {
		_, exists := bDedup[oneRecord.Sha256Digest]
		if !exists {
			(*b).NsMappingRecords.SortedInsert(oneRecord)
		}
	}
	return nil
}

type SnappyCompressableVal interface {
	ToSnappyCompressed() ([]byte, error)
}
