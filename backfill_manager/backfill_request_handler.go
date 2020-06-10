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
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pipeline_pkg "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_svc"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var errorStopped error = fmt.Errorf("BackfillReqHandler is stopping")
var errorSyncDel error = fmt.Errorf("Synchronous deletion took place")

type PersistType int

const (
	AddOp PersistType = iota
	SetOp PersistType = iota
	DelOp PersistType = iota
)

// Provide a running request serializer that can handle incoming requests
// and backend operations for backfill mgr
// Each backfill request handler is responsible for one replication
type BackfillRequestHandler struct {
	*component.AbstractComponent
	id              string
	logger          *log.CommonLogger
	backfillReplSvc service_def.BackfillReplSvc
	startOnce       sync.Once

	// Attached pipeline
	pipelines                    []common.Pipeline
	pipelinesMtx                 sync.RWMutex
	backfillPipelineVBsDone      map[uint16]bool
	backfillPipelineTotalVBsDone int
	raisePipelineErrors          []func(error)
	detachCbs                    []func()

	childrenWaitgrp      sync.WaitGroup
	finCh                chan bool
	stopRequested        uint32
	incomingReqCh        chan metadata.CollectionNamespaceMapping
	doneTaskCh           chan uint16
	handleResultCh       chan error
	doneTaskResultCh     chan error
	persistResultCh      chan error
	persistenceNeededCh  chan PersistType
	persistentOverflowCh chan bool
	persistInterval      time.Duration

	getThroughSeqno LatestSeqnoGetter
	vbsGetter       MyVBsGetter
	vbsDoneNotifier MyVBsTasksDoneNotifier

	spec *metadata.ReplicationSpecification

	// TODO MB-38931 - once consistent metakv is in, this needs to be updated
	cachedBackfillSpec *metadata.BackfillReplicationSpec
	delOpBackfillId    string
}

type LatestSeqnoGetter func() (map[uint16]uint64, error)

type MyVBsGetter func() ([]uint16, error)

type MyVBsTasksDoneNotifier func(startNewTask bool)

func NewCollectionBackfillRequestHandler(logger *log.CommonLogger, replId string, backfillReplSvc service_def.BackfillReplSvc,
	spec *metadata.ReplicationSpecification, seqnoGetter LatestSeqnoGetter, vbsGetter MyVBsGetter, maxConcurrentReqs int,
	persistInterval time.Duration, vbsTasksDoneNotifier MyVBsTasksDoneNotifier) *BackfillRequestHandler {
	return &BackfillRequestHandler{
		AbstractComponent:    component.NewAbstractComponentWithLogger(replId, logger),
		logger:               logger,
		id:                   replId,
		finCh:                make(chan bool),
		incomingReqCh:        make(chan metadata.CollectionNamespaceMapping),
		doneTaskCh:           make(chan uint16, base.NumberOfVbs),
		doneTaskResultCh:     make(chan error, base.NumberOfVbs),
		handleResultCh:       make(chan error, maxConcurrentReqs),
		persistResultCh:      make(chan error, maxConcurrentReqs+base.NumberOfVbs),
		persistenceNeededCh:  make(chan PersistType, 1),
		persistentOverflowCh: make(chan bool, maxConcurrentReqs+base.NumberOfVbs),
		spec:                 spec,
		getThroughSeqno:      seqnoGetter,
		vbsGetter:            vbsGetter,
		backfillReplSvc:      backfillReplSvc,
		persistInterval:      persistInterval,
		vbsDoneNotifier:      vbsTasksDoneNotifier,
	}
}

func (b *BackfillRequestHandler) Start() error {
	b.startOnce.Do(func() {
		atomic.StoreUint32(&b.stopRequested, 0)

		spec, err := b.backfillReplSvc.BackfillReplSpec(b.id)
		if err == nil && spec != nil {
			b.cachedBackfillSpec = spec
		}
		b.childrenWaitgrp.Add(1)
		go b.run()
	})
	return nil
}

func (b *BackfillRequestHandler) Stop(waitGrp *sync.WaitGroup, errCh chan base.ComponentError) {
	defer waitGrp.Done()
	atomic.StoreUint32(&b.stopRequested, 1)
	close(b.finCh)
	close(b.incomingReqCh)

	b.childrenWaitgrp.Done()

	var componentErr base.ComponentError
	componentErr.ComponentId = b.id
	errCh <- componentErr
}

// The backfill request handler's purpose is to handle burst traffic as quickly as possible, and then
// do a single persistence to metakv at the end of the burst, as it is possible that the requests
// could be non-overlapping
// Once the metakv persistence has finished, return the persistent result back to the caller(s)
// So if two callers asked to create 2 backfill requests, there will result in one single error
// returned to both callers
//
// There are 2 channels - handleResultCh and persistResultCh, to represent error code for
// handling the actual request, and subsequently, the error code for persisting
// Callers are expected to listen first to handleResultsCh, and if it is nil, then go listen on
// persistResultCh
//
// Another channel being serialized here is the doneTaskCh. It is used when a VB is finished with a task.
// When a VB is done with a task, the VB's task (top one) is removed from the cached backfill's VBTasksMap
// and the cached backfill is queued up for persistence. The persistence path is shared between handling
// the incoming req and handling the VBs being marked done
// Only cavaet here is that IF the VB being marked done is the LAST TASK in the whole backfill replication,
// the backfill replication MUST be deleted. This is necessary because when a the last VB task is marked done,
// PipelineManager will NOT restart a new backfill pipeline.
// So the backfill spec must be removed, so that when a new VB task comes in afterwards, it will be a brand new
// spec and that will trigger the callback for a brand new spec, which is to launch backfill pipeline.
// Because handling VB done and handling incoming requests are serialized here, it is safe to delete the spec
// (synchronously) and then re-create a new spec once the incomingReqCh is read next
func (b *BackfillRequestHandler) run() {
	var needToRunPersist bool
	var needCoolDown uint32
	for {
		select {
		case <-b.finCh:
			return
		case req := <-b.incomingReqCh:
			err := b.handleBackfillRequestInternal(req)
			b.handleResultCh <- err
			if err == nil {
				needToRunPersist = true
			}
		case vbno := <-b.doneTaskCh:
			err := b.handleVBDone(vbno)
			if err == nil {
				needToRunPersist = true
			} else if err == errorSyncDel {
				// Handling this VB has led to completion of the backfill spec
				// The spec has been synchronously deleted
				needToRunPersist = false
			}
			b.doneTaskResultCh <- err
		default:
			if needToRunPersist && atomic.LoadUint32(&needCoolDown) == 0 {
				needToRunPersist = false
				// No more incoming requests - done bursting handling, do a single metakv operation
				select {
				case persistType := <-b.persistenceNeededCh:
					err := b.metaKvOp(persistType)
					atomic.StoreUint32(&needCoolDown, 1)
					go func() {
						// Cool down period
						time.Sleep(b.persistInterval)
						atomic.StoreUint32(&needCoolDown, 0)
					}()
					b.persistResultCh <- err
					// For any overflow, return the results too
				OVERFLOWRETURN:
					for {
						select {
						case <-b.persistentOverflowCh:
							b.persistResultCh <- err
						default:
							break OVERFLOWRETURN
						}
					}
				}
			}
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

	// Serialize the requests - goes to handleBackfillRequestInternal()
	b.incomingReqCh <- req

	err := <-b.handleResultCh
	if err != nil {
		return err
	}

	return <-b.persistResultCh
}

func (b *BackfillRequestHandler) HandleVBTaskDone(vbno uint16) error {
	if b.IsStopped() {
		return errorStopped
	}

	// goes to handleVBDone()
	b.doneTaskCh <- vbno
	err := <-b.doneTaskResultCh
	if err != nil {
		if err == errorSyncDel {
			// The backfill replication was deleted because this vb being done meant the every task was finished
			err = nil
		}
		return err
	}

	return <-b.persistResultCh
}

// TODO - MB-38931 - once consistent metakv is in place, need to have a listener, handle concurrent updates, etc
func (b *BackfillRequestHandler) handleBackfillRequestInternal(req metadata.CollectionNamespaceMapping) error {
	clonedSpec := b.spec.Clone()

	seqnosMap, err := b.getThroughSeqno()
	if err != nil {
		// TODO handle error
		return err
	}
	myVBs, err := b.vbsGetter()
	if err != nil {
		return err
	}

	vbTasksMap, err := metadata.NewBackfillVBTasksMap(req, myVBs, seqnosMap)
	if err != nil {
		return err
	}

	exists := b.cachedBackfillSpec != nil
	if !exists {
		backfillSpec := metadata.NewBackfillReplicationSpec(clonedSpec.Id, clonedSpec.InternalId, vbTasksMap, clonedSpec)
		b.cachedBackfillSpec = backfillSpec
		b.logNewBackfillMsg(req, seqnosMap)
		b.requestPersistence(AddOp)
	} else {
		if b.cachedBackfillSpec.Contains(vbTasksMap) {
			// already handled - redundant request
			// Just request persistence to ensure synchronization
			b.requestPersistence(SetOp)
			return nil
		}

		var shouldSkipFirst bool = true
		if b.cachedBackfillSpec.VBTasksMap.ContainsAtLeastOneTask() {
			b.pipelinesMtx.RLock()
			pipeline, _ := b.getPipeline(common.BackfillPipeline)
			if pipeline != nil && (pipeline.State() == common.Pipeline_Initial || pipeline.State() == common.Pipeline_Stopped) {
				// See if there are checkpoints present that represent the backfill pipeline has started before
				// If any is present, it means that the first VBTask has made progress and the incoming task cannot be merged with it
				checkpointMgr, ok := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC).(pipeline_svc.CheckpointMgrSvc)
				if ok {
					ckptExists, err := checkpointMgr.CheckpointsExist(pipeline.FullTopic())
					if err == nil && !ckptExists {
						shouldSkipFirst = false
					}
				}
			}
			b.pipelinesMtx.RUnlock()

			skipFirstString := ""
			if !shouldSkipFirst {
				skipFirstString = " (complete merge) "
			}
			// Note, this message is used for integration testing script
			b.logger.Infof("Replication %v%v- These collections need to append backfill %v for vb->seqnos %v", b.id, skipFirstString, req, seqnosMap)
		} else {
			b.logNewBackfillMsg(req, seqnosMap)
		}

		b.cachedBackfillSpec.MergeNewTasks(vbTasksMap, shouldSkipFirst)
		b.requestPersistence(SetOp)
	}
	return nil
}

// Note, this message is used for integration testing script
func (b *BackfillRequestHandler) logNewBackfillMsg(req metadata.CollectionNamespaceMapping, seqnosMap map[uint16]uint64) {
	b.logger.Infof("Replication %v - These collections need to backfill %v for vb->seqnos %v", b.id, req, seqnosMap)
}

func (b *BackfillRequestHandler) requestPersistence(op PersistType) error {
	var err error
	if op == AddOp || op == SetOp {
		select {
		case b.persistenceNeededCh <- op:
			// Got op to persist
		default:
			// Piggy back off of previous above request
			b.persistentOverflowCh <- true
		}
	} else if op == DelOp {
		// DelOps are synchronous - overwrites any previous add or sets
		// all previous persist actions are considered successful, return them to sender
	OVERFLOWRETURN2:
		for {
			select {
			case <-b.persistenceNeededCh:
				b.persistResultCh <- nil
			case <-b.persistentOverflowCh:
				b.persistResultCh <- nil
			default:
				break OVERFLOWRETURN2
			}
		}
		// Then synchronously delete
		err = b.metaKvOp(op)
	}
	return err
}

// This is called per vb when ThroughSeqnoSvc notifies backfill request handler that a VB has finished a task
// Once a VB is finished, the VB task needs to be removed from the VBTaskMap, and then
// it'll wait for other VBs until all the responsible VBs are finished
// The idea is that DCP for each VB that producer (KV) says "StreamEnd", it means there's no more data
// ThroughSeqnoTrackerSvc then will ensure that the seqno last sent from the DCP is considered finished
// Eventually all the VBs will finish, and the pipeline will restart
// DCP has a check stuckness monitor. So if a VB is stuck, it'll restart the pipeline
// If XMEM is stuck, then this callback will not be called, checkpoints will not be deleted,
// and backfill pipeline will restart with error to restart from the last checkpoint.
func (b *BackfillRequestHandler) handleVBDone(vbno uint16) error {
	b.pipelinesMtx.Lock()
	pipeline, i := b.getPipeline(common.BackfillPipeline)
	if pipeline == nil {
		b.pipelinesMtx.Unlock()
		return fmt.Errorf("Fatal error: %v backfill pipeline cannot be found", b.Id())
	}
	vbIsAlreadyDone, exists := b.backfillPipelineVBsDone[vbno]
	if !exists {
		// odd coding error
		err := fmt.Errorf("BackfillReqHandler %v attached to pipeline %v with registered vbs %v, but received vb %v",
			b.Id(), pipeline.FullTopic(), b.backfillPipelineVBsDone, vbno)
		b.raisePipelineErrors[i](err)
		b.pipelinesMtx.Unlock()
		return err
	}
	if vbIsAlreadyDone {
		err := fmt.Errorf("BackfillReqHandler %v attached to pipeline %v already marked vb %v done",
			b.Id(), pipeline.FullTopic(), vbno)
		b.raisePipelineErrors[i](err)
		b.pipelinesMtx.Unlock()
		return err
	}
	b.backfillPipelineVBsDone[vbno] = true
	b.backfillPipelineTotalVBsDone++

	if b.cachedBackfillSpec == nil {
		panic("Cannot be nil")
	} else {
		b.cachedBackfillSpec.VBTasksMap.MarkOneVBTaskDone(vbno)

		// Need to also delete the checkpoints associated with this VB
		// So that when backfill pipeline restarts and if there's another task to be done
		// the old obsolete checkpoints will not be used
		checkpointMgr, ok := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC).(pipeline_svc.CheckpointMgrSvc)
		if !ok {
			panic("Unable to find ckptmgr")
		} else {
			err := checkpointMgr.DelSingleVBCheckpoint(pipeline.FullTopic(), vbno)
			if err != nil {
				b.logger.Errorf("Unable to delete checkpoint doc for %v vbno %v err %v", pipeline.FullTopic(), vbno, err)
			}
		}
	}

	var backfillDone bool
	if b.backfillPipelineTotalVBsDone == len(b.backfillPipelineVBsDone) {
		backfillDone = true
	}
	b.pipelinesMtx.Unlock()

	var hasMoreTasks bool
	if backfillDone {
		hasMoreTasks = b.cachedBackfillSpec.VBTasksMap.ContainsAtLeastOneTask()
		b.vbsDoneNotifier(hasMoreTasks)
	}

	var err error
	if backfillDone && !hasMoreTasks {
		// TODO - once metakv is in, this need to be revisited
		b.delOpBackfillId = b.cachedBackfillSpec.Id
		b.cachedBackfillSpec = nil
		// At this point, there is no more tasks in the backfill spec
		// This is only possible if all the tasks are done
		// We must delete the spec here before a new one can be added
		delErr := b.requestPersistence(DelOp)
		if delErr == nil {
			err = errorSyncDel
		} else {
			err = delErr
		}
	} else {
		b.requestPersistence(SetOp)
	}
	return err
}

func (b *BackfillRequestHandler) metaKvOp(op PersistType) error {
	switch op {
	case AddOp:
		return b.backfillReplSvc.AddBackfillReplSpec(b.cachedBackfillSpec)
	case SetOp:
		return b.backfillReplSvc.SetBackfillReplSpec(b.cachedBackfillSpec)
	default:
		_, err := b.backfillReplSvc.DelBackfillReplSpec(b.delOpBackfillId)
		return err
	}
}

func (b *BackfillRequestHandler) GetSourceNucketName() string {
	return b.spec.SourceBucketName
}

// Each backfill request handler is responsible for one specific replication
// Each replication will have one single active pipeline at one time
// Req Handlers are tied to the repl spec so
// When replications are deleted, the backfill request handler will also be removed
// and thus there is no need for "Detach"
func (b *BackfillRequestHandler) Attach(pipeline common.Pipeline) error {
	b.pipelinesMtx.Lock()
	defer b.pipelinesMtx.Unlock()

	if pipeline.Type() == common.BackfillPipeline {
		b.backfillPipelineVBsDone = make(map[uint16]bool)
		b.backfillPipelineTotalVBsDone = 0
		dcp_parts := pipeline.Sources()
		for _, dcp := range dcp_parts {
			vbs := dcp.ResponsibleVBs()
			for _, vb := range vbs {
				b.backfillPipelineVBsDone[vb] = false
			}
		}
	} else if pipeline.Type() == common.MainPipeline {
		asyncListenerMap := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
		pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.CollectionRoutingEventListener, b)
	}

	// Register supervisor for error handling
	supervisor := pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC).(pipeline_svc.PipelineSupervisorSvc)
	b.RegisterComponentEventListener(common.ErrorEncountered, supervisor)

	b.pipelines = append(b.pipelines, pipeline)
	errFunc := func(err error) {
		supervisor.OnEvent(common.NewEvent(common.ErrorEncountered, nil /*data*/, nil /*component*/, nil /*derivedData*/, err))
	}
	b.raisePipelineErrors = append(b.raisePipelineErrors, errFunc)

	detachCb := func() {
		err := b.UnRegisterComponentEventListener(common.ErrorEncountered, supervisor)
		b.logger.Infof("BackfillReqHandler for %v %v stopping by deregistering listener with err %v", pipeline.Type(), pipeline.Topic(), err)
	}
	b.detachCbs = append(b.detachCbs, detachCb)

	b.logger.Infof("BackfillRequestHandler %v attached to %v with %v total VBs", b.Id(), pipeline.FullTopic(), len(b.backfillPipelineVBsDone))
	return nil
}

func (b *BackfillRequestHandler) Detach(pipeline common.Pipeline) error {
	b.pipelinesMtx.Lock()
	defer b.pipelinesMtx.Unlock()

	var idxToDel int = -1
	for i, attachedP := range b.pipelines {
		if pipeline.FullTopic() == attachedP.FullTopic() {
			b.logger.Infof("Detaching %v %v", attachedP.Type(), attachedP.Topic())
			idxToDel = i
			break
		}
	}

	if idxToDel == -1 {
		return base.ErrorNotFound
	}

	if b.detachCbs[idxToDel] != nil {
		b.detachCbs[idxToDel]()
	}

	b.pipelines = append(b.pipelines[:idxToDel], b.pipelines[idxToDel+1:]...)
	b.detachCbs = append(b.detachCbs[:idxToDel], b.detachCbs[idxToDel+1:]...)
	b.raisePipelineErrors = append(b.raisePipelineErrors[:idxToDel], b.raisePipelineErrors[idxToDel+1:]...)
	return nil
}

// Need to hold lock
func (b *BackfillRequestHandler) getPipeline(ptype common.PipelineType) (common.Pipeline, int) {
	for i, pipeline := range b.pipelines {
		if pipeline.Type() == ptype {
			return pipeline, i
		}
	}
	return nil, -1
}

// Implement AsyncComponentEventHandler
func (b *BackfillRequestHandler) Id() string {
	return b.id
}

// Implement ComponentEventListener
func (b *BackfillRequestHandler) OnEvent(event *common.Event) {
	b.ProcessEvent(event)
}

func (b *BackfillRequestHandler) ProcessEvent(event *common.Event) error {
	switch event.EventType {
	case common.FixedRoutingUpdateEvent:
		routingInfo, ok := event.Data.(parts.CollectionsRoutingInfo)
		if !ok {
			b.logger.Errorf("Invalid routing info %v type", reflect.TypeOf(event.Data))
			// ProcessEvent doesn't care about return code
			return nil
		}
		syncCh, ok := event.OtherInfos.(chan error)
		if !ok {
			b.logger.Errorf("Invalid routing info response channel %v type", reflect.TypeOf(event.OtherInfos))
			// ProcessEvent doesn't care about return code
			return nil
		}
		err := b.HandleBackfillRequest(routingInfo.BackfillMap)
		if err != nil {
			b.logger.Errorf("Handler Process event received err %v for backfillmap %v", err, routingInfo.BackfillMap)
		}
		syncCh <- err
		close(syncCh)
	case common.LastSeenSeqnoDoneProcessed:
		vbno, ok := event.Data.(uint16)
		if !ok {
			err := fmt.Errorf("Invalid vbno data type raised for LastSeenSeqnoDoneProcessed. Type: %v", reflect.TypeOf(event.Data))
			// Only backfill pipeline could have raised it
			b.pipelinesMtx.RLock()
			defer b.pipelinesMtx.RUnlock()
			_, i := b.getPipeline(common.BackfillPipeline)
			b.logger.Errorf(err.Error())
			b.raisePipelineErrors[i](err)
			return err
		}
		err := b.HandleVBTaskDone(vbno)
		if err != nil {
			b.logger.Errorf("Process LastSeenSeqnoDoneProcessed for % vbno %v resulted with %v", b.Id(), err)
		}
	default:
		b.logger.Warnf("Incorrect event type, %v, received by %v", event.EventType, b.id)
	}
	return nil
}

// end Implement AsyncComponentEventHandler
