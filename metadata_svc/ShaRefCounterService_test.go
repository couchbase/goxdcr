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
		NsMappingRecords: []*metadata.CompressedColNamespaceMapping{
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
