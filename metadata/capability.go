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

	// Remote cluster is Enterprise only if ALL nodes are running EE.
	// It's assumed to only roll-forward (i.e. an EE cluster should not be downgraded to CE).
	isClusterEnterprise uint32 // 0 = CE, 1 = EE
}

// CNG TODO: temparily return all enabled capability
func AllEnabledCapability() *Capability {
	return &Capability{
		initialized:         1,
		collection:          1,
		advErrorMapSupport:  1,
		heartbeatSupport:    1,
		isClusterEnterprise: 1,
	}
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

// Remote cluster is Enterprise only if ALL nodes are running EE
func (c Capability) IsClusterEnterprise() bool {
	return atomic.LoadUint32(&c.isClusterEnterprise) > 0
}

func (c Capability) IsSameAs(otherCap Capability) bool {
	// No slices or other reference types so simple comparison
	return c.HasCollectionSupport() == otherCap.HasCollectionSupport() &&
		c.HasAdvErrorMapSupport() == otherCap.HasAdvErrorMapSupport() &&
		c.HasHeartbeatSupport() == otherCap.HasHeartbeatSupport() &&
		c.IsClusterEnterprise() == otherCap.IsClusterEnterprise()
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
	if otherCap.IsClusterEnterprise() {
		atomic.StoreUint32(&c.isClusterEnterprise, 1)
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
	if !c.IsClusterEnterprise() {
		if clusterEnterprise, err := base.IsRemoteClusterFullyEnterprise(nodeList); err != nil {
			logger.Errorf("unable to determine remote cluster's EE status from node list, leaving unchanged: %v", err)
			return err
		} else if clusterEnterprise {
			atomic.StoreUint32(&c.isClusterEnterprise, 1)
		}
	}

	atomic.CompareAndSwapUint32(&c.initialized, 0, 1)
	return nil
}

// Unit Test Only
func UnitTestGetDefaultCapability() Capability {
	return Capability{initialized: 1}
}

func UnitTestGetInitialisedCapability() Capability {
	return Capability{initialized: 1, collection: 1, heartbeatSupport: 1, isClusterEnterprise: 1}
}

func UnitTestGetInitialisedCapabilityForCE() Capability {
	cap := UnitTestGetInitialisedCapability()
	atomic.StoreUint32(&cap.isClusterEnterprise, 0)
	return cap
}

func UnitTestGetInitialisedCapabilityForEE() Capability {
	cap := UnitTestGetInitialisedCapability()
	atomic.StoreUint32(&cap.isClusterEnterprise, 1)
	return cap
}
