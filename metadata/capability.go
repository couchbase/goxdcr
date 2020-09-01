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
	"github.com/couchbase/goxdcr/log"
	"sync/atomic"
)

/**
 * Capabilities of a remote cluster
 */
type Capability struct {
	initialized uint32
	// We don't anticipate future clusters upgrades having deprecated capabilities
	// So use atomics to roll-forward and avoid locking
	collection uint32
}

func (c *Capability) Reset() {
	atomic.StoreUint32(&c.collection, 0)
}

func (c Capability) Clone() (newCap Capability) {
	newCap.LoadFromOther(c)
	return
}

func (c Capability) HasInitialized() bool {
	return atomic.LoadUint32(&c.initialized) > 0
}

func (c Capability) HasCollectionSupport() bool {
	return atomic.LoadUint32(&c.collection) > 0
}

func (c Capability) IsSameAs(otherCap Capability) bool {
	// No slices or other reference types so simple comparison
	return c.HasCollectionSupport() == otherCap.HasCollectionSupport()
}

func (c *Capability) LoadFromOther(otherCap Capability) {
	// CollectionEnabled can only roll forward
	if otherCap.HasCollectionSupport() {
		atomic.StoreUint32(&c.collection, 1)
	}
	atomic.CompareAndSwapUint32(&c.initialized, 0, 1)
}

func (c *Capability) LoadFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) error {
	nodeList, err := base.GetNodeListFromInfoMap(defaultPoolInfo, logger)
	if err != nil {
		return err
	}

	clusterCompatibility, err := base.GetClusterCompatibilityFromNodeList(nodeList)
	if err != nil {
		return err
	}

	// Roll forward collection if necessary
	if !c.HasCollectionSupport() && base.IsClusterCompatible(clusterCompatibility, base.VersionForCollectionSupport) {
		atomic.StoreUint32(&c.collection, 1)
	}

	atomic.CompareAndSwapUint32(&c.initialized, 0, 1)
	return nil
}

// Unit Test Only
func UnitTestGetDefaultCapability() Capability {
	return Capability{1, 0}
}

func UnitTestGetCollectionsCapability() Capability {
	return Capability{1, 1}
}
