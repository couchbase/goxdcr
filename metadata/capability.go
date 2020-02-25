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
)

/**
 * Capabilities of a remote cluster
 */
type Capability struct {
	Collection bool
}

func (c *Capability) Reset() {
	c.Collection = false
}

func (c Capability) Clone() (newCap Capability) {
	// No slices or other reference types so simple assignment is fine
	newCap = c
	return
}

func (c Capability) IsSameAs(otherCap Capability) bool {
	// No slices or other reference types so simple comparison
	return c == otherCap
}

func (c *Capability) LoadFromOther(otherCap Capability) {
	// CollectionEnabled can only roll forward
	if otherCap.Collection {
		c.Collection = otherCap.Collection
	}
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

	// Check different versions here
	c.Collection = base.IsClusterCompatible(clusterCompatibility, base.VersionForCollectionSupport)

	return nil
}
