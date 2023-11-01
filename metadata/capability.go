// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"sync/atomic"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
)

/**
 * Capabilities of a remote cluster
 */
type Capability struct {
	initialized uint32
	// We don't anticipate future clusters upgrades having deprecated capabilities
	// So use atomics to roll-forward and avoid locking
	collection uint32

	// Advanced Xerror error map support
	advErrorMapSupport uint32

	// Remote cluster to accept heartbeat from sources
	heartbeatSupport uint32
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

func (c Capability) HasAdvErrorMapSupport() bool {
	return atomic.LoadUint32(&c.advErrorMapSupport) > 0
}

func (c Capability) HasHeartbeatSupport() bool {
	return atomic.LoadUint32(&c.heartbeatSupport) > 0
}

func (c Capability) IsSameAs(otherCap Capability) bool {
	// No slices or other reference types so simple comparison
	return c.HasCollectionSupport() == otherCap.HasCollectionSupport() &&
		c.HasAdvErrorMapSupport() == otherCap.HasAdvErrorMapSupport() &&
		c.HasHeartbeatSupport() == otherCap.HasHeartbeatSupport()
}

func (c *Capability) LoadFromOther(otherCap Capability) {
	// CollectionEnabled can only roll forward
	if otherCap.HasCollectionSupport() {
		atomic.StoreUint32(&c.collection, 1)
	}
	if otherCap.HasAdvErrorMapSupport() {
		atomic.StoreUint32(&c.advErrorMapSupport, 1)
	}
	if otherCap.HasHeartbeatSupport() {
		atomic.StoreUint32(&c.heartbeatSupport, 1)
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

	// Roll forward everything else if necessary
	if !c.HasCollectionSupport() && base.IsClusterCompatible(clusterCompatibility, base.VersionForCollectionSupport) {
		atomic.StoreUint32(&c.collection, 1)
	}
	if !c.HasAdvErrorMapSupport() && base.IsClusterCompatible(clusterCompatibility, base.VersionForAdvErrorMapSupport) {
		atomic.StoreUint32(&c.advErrorMapSupport, 1)
	}
	if !c.HasHeartbeatSupport() && base.IsClusterCompatible(clusterCompatibility, base.VersionForHeartbeatSupport) {
		atomic.StoreUint32(&c.heartbeatSupport, 1)
	}

	atomic.CompareAndSwapUint32(&c.initialized, 0, 1)
	return nil
}

// Unit Test Only
func UnitTestGetDefaultCapability() Capability {
	return Capability{1, 0, 0, 0}
}

func UnitTestGetCollectionsCapability() Capability {
	return Capability{1, 1, 0, 1}
}
