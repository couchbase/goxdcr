// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
)

type fakeWatcher struct {
	result map[string]interface{}
}

func (f *fakeWatcher) Start() {}
func (f *fakeWatcher) Stop()  {}
func (f *fakeWatcher) GetResult() map[string]interface{} {
	return f.result
}

func TestPeerNodesAdminAddrsSkipsInactive(t *testing.T) {
	securitySvc := &mocks.SecuritySvc{}
	securitySvc.On("IsClusterEncryptionStrictOrAll").Return(false)

	top := &XDCRTopologySvc{
		adminport:   8091,
		securitySvc: securitySvc,
		clusterWatcher: &fakeWatcher{result: map[string]interface{}{
			base.NodesKey: []interface{}{
				map[string]interface{}{base.HostNameKey: "n1:8091", base.ServicesKey: []interface{}{"kv"}, base.ClusterMembershipKey: "inactiveFailed"},
				map[string]interface{}{base.HostNameKey: "n2:8091", base.ServicesKey: []interface{}{"kv"}, base.ClusterMembershipKey: base.ClusterMembership_Active},
			},
		}},
	}

	peers, err := top.PeerNodesAdminAddrs()
	assert.NoError(t, err)
	assert.Equal(t, []string{"n2:8091"}, peers)
}

func TestPeerNodesAdminAddrsSkipsInactiveAdded(t *testing.T) {
	securitySvc := &mocks.SecuritySvc{}
	securitySvc.On("IsClusterEncryptionStrictOrAll").Return(false)

	top := &XDCRTopologySvc{
		adminport:   8091,
		securitySvc: securitySvc,
		clusterWatcher: &fakeWatcher{result: map[string]interface{}{
			base.NodesKey: []interface{}{
				map[string]interface{}{base.HostNameKey: "n1:8091", base.ServicesKey: []interface{}{"kv"}, base.ClusterMembershipKey: "inactiveAdded"},
				map[string]interface{}{base.HostNameKey: "n2:8091", base.ServicesKey: []interface{}{"kv"}, base.ClusterMembershipKey: base.ClusterMembership_Active},
			},
		}},
	}

	peers, err := top.PeerNodesAdminAddrs()
	assert.NoError(t, err)
	assert.Equal(t, []string{"n2:8091"}, peers)
}
