// Copyright 2025 Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the
// licenses/APL2.txt.

package service_impl

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

// getRemoteMemcachedTunables returns the cached remote memcached connection pool tunables.
func (b *BucketTopologyService) getRemoteMemcachedTunables() base.RemoteMemcachedTunables {
	b.cachedTunablesMtx.RLock()
	defer b.cachedTunablesMtx.RUnlock()
	return b.cachedRemoteMemcachedTunables
}

// UpdateAllStatsProvidersConnPoolTunables updates the cached tunables and propagates
// them to all existing stats providers. Called when global settings change.
func (b *BucketTopologyService) UpdateAllStatsProvidersConnPoolTunables(settings *metadata.GlobalSettings) {
	tunables := base.RemoteMemcachedTunables{
		MaxConnsPerServer: settings.GetRemoteMemcachedConnPoolMaxConns(),
		MinConnsPerServer: settings.GetRemoteMemcachedConnPoolMinConns(),
		GCInterval:        settings.GetRemoteMemcachedConnPoolGCInterval(),
	}

	b.cachedTunablesMtx.Lock()
	b.cachedRemoteMemcachedTunables = tunables
	b.cachedTunablesMtx.Unlock()

	b.tgtBucketStatsProvidersMtx.RLock()
	defer b.tgtBucketStatsProvidersMtx.RUnlock()

	providers := make([]service_def.BucketStatsProvider, 0, len(b.tgtBucketStatsProviders))
	for _, provider := range b.tgtBucketStatsProviders {
		providers = append(providers, provider)
	}

	b.logger.Infof("Updating connection pool tunables for %d stats providers", len(providers))

	for _, provider := range providers {
		if statsProvider, ok := provider.(*ClusterBucketStatsProvider); ok {
			statsProvider.UpdateConnPoolTunables(settings)
		} else if _, ok := provider.(*CngBucketStatsProvider); ok {
			continue
		}
	}
}
