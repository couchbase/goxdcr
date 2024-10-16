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

type SnappyCompressableVal interface {
	ToSnappyCompressed() ([]byte, error)
}
