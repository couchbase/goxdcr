// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package backfill_manager

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

type ReplicationSpecChangeListener struct {
	*base.MetakvChangeListener
}

func isCollectionsMgrRunning() bool {
	// TODO - dummy return
	return true
}

func NewReplicationSpecChangeListener(repl_spec_svc service_def.ReplicationSpecSvc,
	cancelChan chan struct{},
	childrenWaitGrp *sync.WaitGroup,
	logger_ctx *log.LoggerContext,
	exitProcess base.ParentExitFunc) *ReplicationSpecChangeListener {

	rscl := &ReplicationSpecChangeListener{
		base.NewMetakvChangeListener(base.ReplicationSpecChangeListenerCollections,
			metadata_svc.GetCatalogPathFromCatalogKey(metadata_svc.ReplicationSpecsCatalogKey),
			cancelChan,
			childrenWaitGrp,
			repl_spec_svc.ReplicationSpecServiceCallback,
			logger_ctx,
			base.ReplicationSpecChangeListenerCollections,
			isCollectionsMgrRunning,
			exitProcess),
	}
	return rscl
}

func (rscl *ReplicationSpecChangeListener) Id() string {
	return "CollectionsMgr ReplicationSpecChangeListener"
}

func (rscl *ReplicationSpecChangeListener) replicationSpecChangeHandlerCallback(changedSpecId string, oldSpecObj interface{}, newSpecObj interface{}) error {
	oldSpec, ok := oldSpecObj.(*metadata.ReplicationSpecification)
	newSpec, ok2 := newSpecObj.(*metadata.ReplicationSpecification)

	if !ok || !ok2 {
		rscl.Logger().Errorf("%v error converting oldSpec: %v newSpec: %v", rscl.Id(), ok, ok2)
		return base.ErrorInvalidInput
	}

	rscl.Logger().Infof("%v detected specId %v old %v new %v", rscl.Id(), changedSpecId, oldSpec, newSpec)

	return nil
}
