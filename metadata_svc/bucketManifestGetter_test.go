// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata_svc

import (
	"fmt"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

func TestBucketManifestGetter(t *testing.T) {
	fmt.Println("============== Test case start: TestBucketManifestGetter =================")
	assert := assert.New(t)

	getterMock := &service_def.CollectionsManifestOps{}
	getterMock.On("CollectionManifestGetter", mock.Anything).Run(func(args mock.Arguments) { time.Sleep(100 * time.Millisecond) }).Return(nil, fmt.Errorf("dummy"))

	getter := NewBucketManifestGetter("TestBucketGetter", getterMock, 15*time.Second)
	assert.NotNil(getter)

	// Background processes should get the same manifest
	var manifest1Ptr *metadata.CollectionsManifest
	var manifest2Ptr *metadata.CollectionsManifest
	var manifest3Ptr *metadata.CollectionsManifest
	var wg sync.WaitGroup

	// When the manifest returned is nil, the getter should return a default manifest
	bgFunc := func(ptr **metadata.CollectionsManifest, wg *sync.WaitGroup) {
		*ptr = getter.GetManifest()
		wg.Done()
	}

	wg.Add(3)
	go bgFunc(&manifest1Ptr, &wg)
	go bgFunc(&manifest2Ptr, &wg)
	go bgFunc(&manifest3Ptr, &wg)
	wg.Wait()

	assert.True(manifest1Ptr.IsSameAs(manifest2Ptr))
	assert.True(manifest2Ptr.IsSameAs(manifest3Ptr))

	assert.Equal(stateNone, getter.getterState)
	fmt.Println("============== Test case end: TestBucketManifestGetter =================")
}
