// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

type BackfillMgr struct {
	collectionsManifestSvc service_def.CollectionsManifestSvc
	exitFunc               base.ParentExitFunc
	replSpecSvc            service_def.ReplicationSpecSvc

	logger *log.CommonLogger

	childrenWaitgrp sync.WaitGroup
	cancelChan      chan struct{}

	cleanupOnce sync.Once
}

func (c *BackfillMgr) backfillMgrExitFunc(force bool) {

	if !force {
		c.cleanup()
	}
}

func (c *BackfillMgr) cleanup() {
	c.cleanupOnce.Do(func() {
		c.logger.Infof("BackfillMgr exiting")
		c.Stop()
		c.cancelChan <- struct{}{}
		c.logger.Infof("BackfillMgr exited")
	})
}

func NewBackfillManager(collectionsManifestSvc service_def.CollectionsManifestSvc,
	parentExitFunc base.ParentExitFunc,
	replSpecSvc service_def.ReplicationSpecSvc) *BackfillMgr {

	backfillMgr := &BackfillMgr{
		collectionsManifestSvc: collectionsManifestSvc,
		replSpecSvc:            replSpecSvc,
		cancelChan:             make(chan struct{}, 1),
		logger:                 log.NewLogger("BackfillMgr", log.DefaultLoggerContext),
	}

	wrappedExitFunc := func(force bool) {
		backfillMgr.backfillMgrExitFunc(force)
		parentExitFunc(force)
	}

	// TODO - to be used by future metadata change monitors
	backfillMgr.exitFunc = wrappedExitFunc

	return backfillMgr
}

func (c *BackfillMgr) Start() {
	c.logger.Infof("BackfillMgr Starting...")
	c.initMetadataChangeMonitor()

	c.collectionsManifestSvc.SetMetadataChangeHandlerCallback(c.collectionsManifestChangeCb)
	c.replSpecSvc.SetMetadataChangeHandlerCallback(c.replicationSpecChangeHandlerCallback)

	c.logger.Infof("BackfillMgr Started")
}

func (c *BackfillMgr) Stop() {
	c.logger.Infof("BackfillMgr Stopping...")

	c.logger.Infof("BackfillMgr Stopped")
}

func (c *BackfillMgr) initMetadataChangeMonitor() {
	// TODO - future metadata change monitors
	//	mcm := base.NewMetadataChangeMonitor()
}

func (c *BackfillMgr) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.ReplicationSpecification)
	newSpec, ok2 := newSpecObj.(*metadata.ReplicationSpecification)

	if !ok || !ok2 {
		c.logger.Errorf("BackfillMgr error converting oldSpec: %v newSpec: %v", ok, ok2)
		return base.ErrorInvalidInput
	}

	c.logger.Infof("BackfillMgr detected specId %v old %v new %v", changedSpecId, oldSpec, newSpec)

	c.collectionsManifestSvc.ReplicationSpecChangeCallback(changedSpecId, oldSpecObj, newSpecObj)

	return nil
}

func (c *BackfillMgr) collectionsManifestChangeCb(replId string, oldVal, newVal interface{}) error {
	oldManifest, ok := oldVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}
	newManifest, ok := newVal.(*metadata.CollectionsManifestPair)
	if !ok {
		return base.ErrorInvalidInput
	}

	var addedOrModified metadata.ScopesMap
	var removed metadata.ScopesMap
	var srcErr error
	var tgtErr error

	// Handle source
	if oldManifest.Source == nil && newManifest.Source != nil {
		c.logger.Infof("Source manifest did not exist, now it has: %v\n", newManifest.Source.String())
	} else if oldManifest.Source != nil && newManifest.Source == nil {
		// Don't think it's possible...
		c.logger.Infof("Source manifest has been deleted")
		srcErr = base.ErrorInvalidInput
	} else {
		addedOrModified, removed, srcErr = newManifest.Source.Diff(oldManifest.Source)
		if srcErr != nil {
			c.logger.Errorf("Unable to diff between source manifests: %v", srcErr.Error())
		}
		c.logger.Infof(fmt.Sprintf("Source Added or removed:\n%v\nRemoved:\n%v\n", addedOrModified.String(), removed.String()))
	}

	// Handle target
	if oldManifest.Target == nil && newManifest.Target != nil {
		c.logger.Infof("Target manifest did not exist, now it has: %v\n", newManifest.Target.String())
	} else if oldManifest.Target != nil && newManifest.Target == nil {
		// Don't think it's possible...
		c.logger.Infof("Target manifest has been deleted")
		tgtErr = base.ErrorInvalidInput
	} else {
		addedOrModified, removed, tgtErr = newManifest.Target.Diff(oldManifest.Target)
		if tgtErr != nil {
			c.logger.Errorf("Unable to diff between target manifests: %v", tgtErr.Error())
		}
		c.logger.Infof(fmt.Sprintf("Target Added or removed:\n%v\nRemoved:\n%v\n", addedOrModified.String(), removed.String()))
	}

	if tgtErr != nil && srcErr != nil {
		if srcErr != nil {
			return srcErr
		} else {
			return tgtErr
		}
	}
	return nil
}
