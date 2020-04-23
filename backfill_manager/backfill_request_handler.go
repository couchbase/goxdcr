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
	"sync/atomic"
)

// TODO - make this internal setting
var maxIncomingReqSize int = 50
var errorStopped error = fmt.Errorf("BackfillMgr is stopping")

// Provide a running request serializer that can handle incoming requests
// and backend operations for backfill mgr
type BackfillRequestHandler struct {
	id              string
	logger          *log.CommonLogger
	backfillReplSvc service_def.BackfillReplSvc

	childrenWaitgrp sync.WaitGroup
	finCh           chan bool
	stopRequested   uint32
	incomingReqCh   chan metadata.CollectionNamespaceMapping
	getThroughSeqno LatestSeqnoGetter

	spec    *metadata.ReplicationSpecification
	specMtx sync.RWMutex

	// TODO MB-38931 - once consistent metakv is in, this needs to be updated
	cachedBackfillSpec    *metadata.BackfillReplicationSpec
	cachedBackfillSpecMtx sync.RWMutex
}

type LatestSeqnoGetter func() (map[uint16]uint64, error)

func NewCollectionBackfillRequestHandler(logger *log.CommonLogger, replId string, backfillReplSvc service_def.BackfillReplSvc,
	spec *metadata.ReplicationSpecification, seqnoGetter LatestSeqnoGetter) *BackfillRequestHandler {
	return &BackfillRequestHandler{
		logger:          logger,
		id:              replId,
		finCh:           make(chan bool),
		incomingReqCh:   make(chan metadata.CollectionNamespaceMapping, maxIncomingReqSize),
		spec:            spec,
		getThroughSeqno: seqnoGetter,
	}
}

func (b *BackfillRequestHandler) Start() error {
	b.logger.Infof("BackfillRequestHandler %v starting...", b.id)
	atomic.StoreUint32(&b.stopRequested, 0)
	b.childrenWaitgrp.Add(1)
	go b.run()

	b.logger.Infof("BackfillRequestHandler %v started", b.id)
	return nil
}

func (b *BackfillRequestHandler) Stop(waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	b.logger.Infof("BackfillRequestHandler %v stopping...", b.id)
	defer waitGrp.Done()
	atomic.StoreUint32(&b.stopRequested, 1)
	close(b.finCh)
	close(b.incomingReqCh)

	b.childrenWaitgrp.Done()
	b.logger.Infof("BackfillRequestHandler %v stopped", b.id)

	var componentErr base.ComponentError
	componentErr.ComponentId = b.id
	errCh <- componentErr
}

// runs until stopped
func (b *BackfillRequestHandler) run() {
	for {
		select {
		case <-b.finCh:
			return
		case req := <-b.incomingReqCh:
			b.handleBackfillRequestInternal(req)
		default:
			// Do nothing
		}
	}
}

func (b *BackfillRequestHandler) IsStopped() bool {
	return atomic.LoadUint32(&(b.stopRequested)) == 1
}

func (b *BackfillRequestHandler) HandleBackfillRequest(req metadata.CollectionNamespaceMapping) error {
	if b.IsStopped() {
		return errorStopped
	}

	// Serialize the requests
	b.incomingReqCh <- req
	return nil
}

// TODO - MB-38931 - once consistent metakv is in place, need to have a listener, handle concurrent updates, etc
func (b *BackfillRequestHandler) handleBackfillRequestInternal(req metadata.CollectionNamespaceMapping) {
	// TODO - to be used later
	//	b.specMtx.RLock()
	//	internalId := b.spec.InternalId
	//	b.specMtx.RUnlock()

	seqnosMap, err := b.getThroughSeqno()
	if err != nil {
		// TODO handle error
		return
	}
	b.logger.Infof("Replication %v - These collections need to backfill %v for vb->seqnos %v", b.id, req, seqnosMap)

	// TODO - create or modify existing tasks
}

func (b *BackfillRequestHandler) GetSourceNucketName() string {
	return b.spec.SourceBucketName
}
