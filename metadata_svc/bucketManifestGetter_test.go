// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestBucketManifestGetter(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketManifestGetter =================")
	assert := assert.New(t)

	getterMock := &service_def.CollectionsManifestOps{}
	getterMock.On("CollectionManifestGetter", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(100 * time.Millisecond) }).Return(nil, fmt.Errorf("dummy"))

	getter := NewBucketManifestGetter("TestBucketGetter", getterMock, 15*time.Second, nil)
	assert.NotNil(getter)

	// Background processes should get the same manifest
	var manifest1Ptr *metadata.CollectionsManifest
	var manifest2Ptr *metadata.CollectionsManifest
	var manifest3Ptr *metadata.CollectionsManifest
	var manifest4Ptr *metadata.CollectionsManifest
	var wg sync.WaitGroup

	// When the manifest returned is nil, the getter should return a default manifest
	bgFunc := func(ptr **metadata.CollectionsManifest, wg *sync.WaitGroup) {
		*ptr = getter.GetManifest(false /*forceRefresh */)
		wg.Done()
	}

	bgFunc1 := func(ptr **metadata.CollectionsManifest, wg *sync.WaitGroup) {
		*ptr = getter.GetManifest(true /*forceRefresh */)
		wg.Done()
	}

	wg.Add(4)
	go bgFunc(&manifest1Ptr, &wg)
	go bgFunc1(&manifest2Ptr, &wg)
	go bgFunc(&manifest3Ptr, &wg)
	go bgFunc1(&manifest4Ptr, &wg)

	wg.Wait()

	assert.True(manifest1Ptr.IsSameAs(manifest2Ptr))
	assert.True(manifest2Ptr.IsSameAs(manifest3Ptr))
	assert.True(manifest3Ptr.IsSameAs(manifest4Ptr))

	assert.Equal(stateNone, getter.getterState)
	fmt.Println("============== Test case end: TestBucketManifestGetter =================")
}
