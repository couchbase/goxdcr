// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/metadata"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

// Call upsertMapping first and then DelAndCleanup
func TestShaRefCounter_cleanup_deadlock(t *testing.T) {
	metadataSvc := &service_def.MetadataSvc{}
	utilities := &utilities.UtilsIface{}
	topic := "test-topic"
	internalId := "internal-id-123"

	nsmap := &metadata.CollectionNsMappingsDoc{
		NsMappingRecords: []*metadata.CompressedShaMapping{
			{
				CompressedMapping: make([]byte, 10),
				Sha256Digest:      "abcd",
			},
		},
		SpecInternalId: "internal-id-123",
	}

	buf, err := json.Marshal(nsmap)
	assert.Nil(t, err)

	utilities.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration { return 0 })
	metadataSvc.On("Del", mock.Anything, mock.Anything).Return(nil)
	metadataSvc.On("Get", mock.Anything).Return(buf, nil, nil)
	metadataSvc.On("Set", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// We simulate a slow "Set" call for DelAndCleanup to have a chance to run in parallel with upsertMapping
		// A delay here lines-up the locking in such a way that caused a deadlock before the fix
		time.Sleep(3 * time.Second)
	}).Return(nil)

	t.Run("Call upsertMapping first and then DelAndCleanup", func(t *testing.T) {
		counter := NewMapShaRefCounterWithInternalId(topic, internalId, metadataSvc, "meta-kv-op", nil)
		counter.Init()
		counter.needToSync = true

		go func() {
			counter.upsertMapping(internalId, true)
		}()
		time.Sleep(1 * time.Second)
		counter.DelAndCleanup()
	})

	t.Run("Call DelAndCleanup first and then upsertMapping", func(t *testing.T) {
		counter := NewMapShaRefCounterWithInternalId(topic, internalId, metadataSvc, "meta-kv-op", nil)
		counter.Init()
		counter.needToSync = true

		go counter.DelAndCleanup()
		time.Sleep(1 * time.Second)
		counter.upsertMapping(internalId, true)
	})
}

func TestShaRefCounter_GetShaNamespaceMap_ReturnsClone(t *testing.T) {
	assert := assert.New(t)
	metadataSvc := &service_def.MetadataSvc{}
	topic := "test-topic"
	internalId := "internal-id-123"

	// Create counter and initialize
	counter := NewMapShaRefCounterWithInternalId(topic, internalId, metadataSvc, "meta-kv-op", nil)
	counter.Init()

	// Add a mapping to the internal map
	mapping := &metadata.CollectionNamespaceMapping{}
	testSha := "test-sha-256"
	counter.shaToMapping[testSha] = mapping

	// Get the map (should be a clone)
	retrievedMap := counter.GetShaNamespaceMap()

	assert.False(maps.Equal(retrievedMap, counter.shaToMapping))
}

func TestShaRefCounter_GetShaGlobalInfoMap_ReturnsClone(t *testing.T) {
	assert := assert.New(t)
	metadataSvc := &service_def.MetadataSvc{}
	topic := "test-topic"
	internalId := "internal-id-123"

	// Create counter and initialize
	counter := NewMapShaRefCounterWithInternalId(topic, internalId, metadataSvc, "meta-kv-op", nil)
	counter.Init()

	// Add a GlobalTimestamp to the internal map
	globalTimestamp := make(metadata.GlobalTimestamp)
	globalTimestamp[0] = &metadata.GlobalVBTimestamp{}
	testSha := "test-sha-256"
	counter.shaToGlobalInfo[testSha] = &globalTimestamp

	// Get the map (should be a clone)
	retrievedMap := counter.GetShaGlobalInfoMap()

	assert.False(maps.Equal(retrievedMap, counter.shaToGlobalInfo))
}

func TestShaRefCounter_GetShaGlobalInfoMap_ConcurrentAccess(t *testing.T) {
	assert := assert.New(t)
	metadataSvc := &service_def.MetadataSvc{}
	topic := "test-topic"
	internalId := "internal-id-123"

	// Create counter and initialize
	counter := NewMapShaRefCounterWithInternalId(topic, internalId, metadataSvc, "meta-kv-op", nil)
	counter.Init()

	// Pre-populate with some test data
	for i := 0; i < 10; i++ {
		globalTimestamp := make(metadata.GlobalTimestamp)
		globalTimestamp[uint16(i)] = &metadata.GlobalVBTimestamp{}
		testSha := fmt.Sprintf("test-sha-%d", i)
		counter.shaToGlobalInfo[testSha] = &globalTimestamp
	}

	const numReaders = 50
	const numWriters = 50
	const iterations = 100

	done := make(chan bool)
	errChan := make(chan error, numReaders+numWriters)

	// Launch reader goroutines
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("reader %d panicked: %v", id, r)
				}
				done <- true
			}()

			for j := 0; j < iterations; j++ {
				// Get the map (should not cause data races)
				retrievedMap := counter.GetShaGlobalInfoMap()

				// Verify we got a map back
				if retrievedMap == nil {
					errChan <- fmt.Errorf("reader %d: got nil map", id)
					return
				}

				// do a read operation on the map
				testSha := fmt.Sprintf("test-sha-%d", id%10)
				_, exists := retrievedMap[testSha]
				if !exists {
					errChan <- fmt.Errorf("reader %d: test-sha-%d not found", id, id%10)
					return
				}

				// Modify the retrieved clone (should not affect internal state)
				newGlobalTimestamp := make(metadata.GlobalTimestamp)
				newGlobalTimestamp[0] = &metadata.GlobalVBTimestamp{}
				retrievedMap[fmt.Sprintf("reader-%d-%d", id, j)] = &newGlobalTimestamp

				// Small delay to increase chance of concurrent access
				if j%10 == 0 {
					time.Sleep(1 * time.Microsecond)
				}
			}
		}(i)
	}

	// Launch writer goroutines that use RecordOneCount (the proper API)
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("writer %d panicked: %v", id, r)
				}
				done <- true
			}()

			for j := 0; j < iterations; j++ {
				// Create a new global timestamp
				globalTimestamp := make(metadata.GlobalTimestamp)
				globalTimestamp[0] = &metadata.GlobalVBTimestamp{}
				testSha := fmt.Sprintf("writer-sha-%d-%d", id, j)

				// Use the proper API to record a count
				counter.RecordOneCount(testSha, &globalTimestamp)

				// Small delay to increase chance of concurrent access
				if j%10 == 0 {
					time.Sleep(1 * time.Microsecond)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numReaders+numWriters; i++ {
		<-done
	}
	close(errChan)

	// Check for any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}
	assert.Empty(errors, "Expected no errors from concurrent access, got: %v", errors)

	// Verify the internal state is still valid
	finalMap := counter.GetShaGlobalInfoMap()
	assert.NotNil(finalMap)

	// Verify none of the "reader-*" keys exist in the internal map
	// (they should only exist in the clones)
	for key := range finalMap {
		assert.NotContains(key, "reader-", "Reader modifications should not affect internal map")
	}

	// Verify at least the original test data and writer data exist
	assert.GreaterOrEqual(len(finalMap), 60, "Should have at least the original 60 entries")
}
