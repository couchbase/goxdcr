//go:build dev

// Copyright 2025-Present Couchbase, Inc.
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
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type BackfillReplServiceDevInjector struct {
	specIdMtx      sync.Mutex
	backfillSpecId string
}

func NewBackfillReplServiceInjector() *BackfillReplServiceDevInjector {
	return &BackfillReplServiceDevInjector{}
}

func (b *BackfillReplServiceDevInjector) InjectBackfillReplDelay(spec *metadata.BackfillReplicationSpec) {
	parentSpec := spec.GetReplicationSpec()
	if parentSpec != nil && parentSpec.Settings != nil && parentSpec.Settings.GetIntSettingValue(metadata.DevBackfillReplUpdateDelay) > 0 {
		msToSleep := parentSpec.Settings.GetIntSettingValue(metadata.DevBackfillReplUpdateDelay)
		time.Sleep(time.Duration(msToSleep) * time.Millisecond)
	}
}

func (b *BackfillReplServiceDevInjector) InjectRaiseCompleteBackfillDelay(replSpecSvc service_def.ReplicationSpecSvc, backfillSpecId string, logger *log.CommonLogger, injected *bool, err *error) {
	var shouldInjAndDelay bool
	specToCheck, specCheckErr := replSpecSvc.ReplicationSpec(backfillSpecId)
	if specCheckErr == nil {
		shouldInjAndDelay = specToCheck.Settings.GetBoolSettingValue(metadata.DevBackfillUnrecoverableErrorInj)
		if shouldInjAndDelay {
			*injected = true
			logger.Warnf("Dev inj spec has specified error injection %v", backfillSpecId)
			*err = fmt.Errorf("DevBackfillUnrecoverableErrorInj")
			b.specIdMtx.Lock()
			b.backfillSpecId = backfillSpecId
			b.specIdMtx.Unlock()
		}
	}
}

// InjectRaiseCompleteBackfillDelaySleep depends on earlier call of InjectRaiseCompleteBackfillDelay
func (b *BackfillReplServiceDevInjector) InjectRaiseCompleteBackfillDelaySleep(replSpecSvc service_def.ReplicationSpecSvc, logger *log.CommonLogger) {
	var shouldInjAndDelay bool
	b.specIdMtx.Lock()
	backfillSpecId := b.backfillSpecId
	b.specIdMtx.Unlock()
	if backfillSpecId == "" {
		return
	}

	specToCheck, specCheckErr := replSpecSvc.ReplicationSpec(backfillSpecId)
	if specCheckErr == nil {
		shouldInjAndDelay = specToCheck.Settings.GetBoolSettingValue(metadata.DevBackfillUnrecoverableErrorInj)
		if shouldInjAndDelay {
			logger.Warnf("Dev inj sleeping for 90s for P2P push to kick in")
			time.Sleep(90 * time.Second)
		}
	}
}

type CollectionsManifestSvcDevInjector struct {
}

func NewCollectionsManifestSvcInjector() *CollectionsManifestSvcDevInjector {
	return &CollectionsManifestSvcDevInjector{}
}

func (i *CollectionsManifestSvcDevInjector) InjectDelay(newSpec *metadata.ReplicationSpecification, c *CollectionsManifestService) {
	delaySec := newSpec.Settings.GetIntSettingValue(metadata.DevColManifestSvcDelaySec)
	portSpecifier := newSpec.Settings.GetIntSettingValue(metadata.DevNsServerPortSpecifier)
	if delaySec > 0 {
		if portSpecifier > 0 {
			myhostAddr, _ := c.xdcrTopologySvc.MyHostAddr()
			myPortNumber, _ := base.GetPortNumber(myhostAddr)
			// If err, myPortNumber is 0 and won't match portSpecifier anyway
			if int(myPortNumber) == portSpecifier {
				c.logger.Infof("XDCR Dev Injection for this node %v set delay to %v seconds", portSpecifier, delaySec)
				atomic.StoreUint32(&c.xdcrInjDelaySec, uint32(delaySec))
			}
		} else {
			c.logger.Infof("XDCR Dev Injection set delay to %v seconds", delaySec)
		}
	} else if delaySec == 0 {
		atomic.StoreUint32(&c.xdcrInjDelaySec, 0)
	}
}
