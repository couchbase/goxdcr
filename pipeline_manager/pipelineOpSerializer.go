/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_manager

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline"
	"sync"
)

type PipelineMgtOpType int

var MaxNonblockingQueueJobs = 1000
var ErrQueueMaxed error = errors.New("The requested action has been persisted in the metadata store, but was unabled to execute due to too many queued jobs.")
var SerializerStoppedErr error = errors.New("Pipeline Manager is shutting down. The requested action is unable to be processed.")

const (
	PipelineGetOrCreate            PipelineMgtOpType = iota
	PipelineInit                   PipelineMgtOpType = iota
	PipelineUpdate                 PipelineMgtOpType = iota
	PipelineUpdateWStoppedCb       PipelineMgtOpType = iota
	PipelineDeletion               PipelineMgtOpType = iota
	PipelineReinitStream           PipelineMgtOpType = iota
	PipelineAutoPause              PipelineMgtOpType = iota
	BackfillPipelineStart          PipelineMgtOpType = iota
	BackfillPipelineStop           PipelineMgtOpType = iota
	BackfillPipelineStopWStoppedCb PipelineMgtOpType = iota
	BackfillPipelineClean          PipelineMgtOpType = iota
	DismissEvent                   PipelineMgtOpType = iota
	BackfillMappingStatusUpdate    PipelineMgtOpType = iota
)

func (p PipelineMgtOpType) String() string {
	switch p {
	case PipelineGetOrCreate:
		return "PipelineGetOrCreate"
	case PipelineInit:
		return "PipelineInit"
	case PipelineUpdate:
		return "PipelineUpdate"
	case PipelineUpdateWStoppedCb:
		return "PipelineUpdateWStoppedCb"
	case PipelineDeletion:
		return "PipelineDeletion"
	case PipelineReinitStream:
		return "PipelineReinitStream"
	case PipelineAutoPause:
		return "PipelineAutoPause"
	case BackfillPipelineStart:
		return "BackfillPipelineStart"
	case BackfillPipelineStop:
		return "BackfillPipelineStop"
	case BackfillPipelineStopWStoppedCb:
		return "BackfillPipelineStopWStoppedCb"
	case BackfillPipelineClean:
		return "BackfillPipelineClean"
	case DismissEvent:
		return "DismissEvent"
	case BackfillMappingStatusUpdate:
		return "BackfillMappingStatusUpdate"
	default:
		return "?? PipelineMgtOpType"
	}
}

type PipelineOpSerializerIface interface {
	// Asynchronous User APIs
	Delete(topic string) error
	Update(topic string, err error) error
	UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error
	Init(topic string) error
	ReInit(topic string) error
	Pause(topic string) error
	StartBackfill(topic string) error

	// Synchronous Backfill APIs
	StopBackfill(topic string) error
	StopBackfillWithCb(pipelineName string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error
	CleanBackfill(topic string) error

	// Synchronous User APIs - call and get data from a channel
	GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error)

	// APIs for Pipeline Manager to drive
	Stop()

	// Event Framework
	DismissEvent(topic string, eventId int) error
	BackfillMappingStatusUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error
}

type SerializerRepStatusPair struct {
	repStatus *pipeline.ReplicationStatus
	errCode   error
}

// A job is consisted of a name and an operation to be serialized
type Job struct {
	// Main job types
	pipelineTopic string
	jobType       PipelineMgtOpType

	// Aux inputs for jobs
	errForUpdateOp error
	waitGrp        *sync.WaitGroup

	// Input for UpdateWithStoppedCallback
	callbackForWhenPipelineIsStopped base.StoppedPipelineCallback
	errorCbForFailedStoppedOp        base.StoppedPipelineErrCallback

	// Event and backfill related
	eventId           int
	diffPair          *metadata.CollectionNamespaceMappingsDiffPair
	srcManifestsDelta []*metadata.CollectionsManifest
	skipBackfillCkpt  bool

	// Optional outputs from jobs
	repStatusCh chan SerializerRepStatusPair
}

func (j *Job) String() string {
	return fmt.Sprintf("Job for %v Type: %v ", j.pipelineTopic, j.jobType.String())
}

type PipelineOpSerializer struct {
	// Pipeline Manager related stuff
	pipelineMgr PipelineMgrForSerializer
	childWGrp   sync.WaitGroup
	logger      *log.CommonLogger

	// For shutting down
	shutdownOnce sync.Once
	stopped      bool
	stoppedLk    sync.RWMutex

	// Synchronization stuff
	jobTopicMap map[string]chan Job
	mapMtx      sync.RWMutex
}

// The goal of the serializer is to provide a serialized front-end to end-users for any operations
// to the pipeline, and then distribute the operations into safe parallel tasks for pipeline manager
func NewPipelineOpSerializer(pipelineMgrIn PipelineMgrForSerializer, logger *log.CommonLogger) *PipelineOpSerializer {
	serializer := &PipelineOpSerializer{
		pipelineMgr: pipelineMgrIn,
		logger:      logger,
		jobTopicMap: make(map[string]chan Job),
	}
	return serializer
}

/****************************************
 * Implementations of external API calls
 ****************************************/
func (serializer *PipelineOpSerializer) Delete(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var delJob Job
	delJob.jobType = PipelineDeletion
	delJob.pipelineTopic = topic

	return serializer.distributeJob(delJob)
}

func (serializer *PipelineOpSerializer) Update(topic string, err error) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var updateJob Job
	updateJob.jobType = PipelineUpdate
	updateJob.pipelineTopic = topic
	updateJob.errForUpdateOp = err

	return serializer.distributeJob(updateJob)
}

func (serializer *PipelineOpSerializer) Init(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var initJob Job
	initJob.jobType = PipelineInit
	initJob.pipelineTopic = topic

	return serializer.distributeJob(initJob)
}

func (serializer *PipelineOpSerializer) ReInit(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var resetJob Job
	resetJob.jobType = PipelineReinitStream
	resetJob.pipelineTopic = topic

	return serializer.distributeJob(resetJob)
}

func (serializer *PipelineOpSerializer) Pause(topic string) error {
	var pauseJob Job
	pauseJob.jobType = PipelineAutoPause
	pauseJob.pipelineTopic = topic

	return serializer.distributeJob(pauseJob)
}

func (serializer *PipelineOpSerializer) StartBackfill(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var startBackfillJob Job
	startBackfillJob.jobType = BackfillPipelineStart
	startBackfillJob.pipelineTopic = topic

	return serializer.distributeJob(startBackfillJob)
}

func (serializer *PipelineOpSerializer) StopBackfill(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var stopBackfillJob Job
	stopBackfillJob.jobType = BackfillPipelineStop
	stopBackfillJob.pipelineTopic = topic
	stopBackfillJob.skipBackfillCkpt = true
	stopBackfillJob.waitGrp = &sync.WaitGroup{}
	stopBackfillJob.waitGrp.Add(1)

	distributeErr := serializer.distributeJob(stopBackfillJob)
	if distributeErr != nil {
		return distributeErr
	}

	stopBackfillJob.waitGrp.Wait()
	return nil
}

func (serializer *PipelineOpSerializer) StopBackfillWithCb(topic string, cb base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback, skipCkpt bool) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var stopBackfillJob Job
	stopBackfillJob.jobType = BackfillPipelineStopWStoppedCb
	stopBackfillJob.pipelineTopic = topic
	stopBackfillJob.skipBackfillCkpt = skipCkpt
	stopBackfillJob.callbackForWhenPipelineIsStopped = cb
	stopBackfillJob.errorCbForFailedStoppedOp = errCb
	stopBackfillJob.waitGrp = &sync.WaitGroup{}
	stopBackfillJob.waitGrp.Add(1)

	distributeErr := serializer.distributeJob(stopBackfillJob)
	if distributeErr != nil {
		return distributeErr
	}

	stopBackfillJob.waitGrp.Wait()
	return nil
}

func (serializer *PipelineOpSerializer) CleanBackfill(topic string) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var cleanBackfillJob Job
	cleanBackfillJob.jobType = BackfillPipelineClean
	cleanBackfillJob.pipelineTopic = topic

	return serializer.distributeJob(cleanBackfillJob)
}

// Synchronous call
func (serializer *PipelineOpSerializer) GetOrCreateReplicationStatus(topic string, err error) (*pipeline.ReplicationStatus, error) {
	if serializer.isStopped() {
		return nil, SerializerStoppedErr
	}

	var getOrCreateJob Job
	getOrCreateJob.jobType = PipelineGetOrCreate
	getOrCreateJob.pipelineTopic = topic
	getOrCreateJob.errForUpdateOp = err
	// Make it blocking
	getOrCreateJob.repStatusCh = make(chan SerializerRepStatusPair)

	queueErr := serializer.distributeJob(getOrCreateJob)
	if queueErr != nil {
		return nil, queueErr
	}

	retPair := <-getOrCreateJob.repStatusCh

	return retPair.repStatus, retPair.errCode
}

/*********************************************
 * Internal implementations
 ********************************************/
func (serializer *PipelineOpSerializer) isStopped() bool {
	serializer.stoppedLk.RLock()
	defer serializer.stoppedLk.RUnlock()
	return serializer.stopped
}

// distributeJob's main purpose is to ensure that jobs are executed parallely, but sequentially per pipelineTopic
func (serializer *PipelineOpSerializer) distributeJob(oneJob Job) (retErr error) {
	serializer.mapMtx.RLock()
	jobCh, ok := serializer.jobTopicMap[oneJob.pipelineTopic]
	if ok {
		select {
		case jobCh <- oneJob:
		default:
			retErr = ErrQueueMaxed
		}
		serializer.mapMtx.RUnlock()
	} else {
		serializer.mapMtx.RUnlock()
		serializer.mapMtx.Lock()
		defer serializer.mapMtx.Unlock()
		jobCh, ok = serializer.jobTopicMap[oneJob.pipelineTopic]
		if ok {
			// There's a chance someone raced in front... but we cannot downgrade the lock
			serializer.logger.Infof("Another job jumped ahead of creating a topic: %v", oneJob.pipelineTopic)
			select {
			case jobCh <- oneJob:
			default:
				retErr = ErrQueueMaxed
			}
		} else {
			serializer.stoppedLk.RLock()
			if !serializer.stopped {
				serializer.jobTopicMap[oneJob.pipelineTopic] = make(chan Job, MaxNonblockingQueueJobs)
				serializer.jobTopicMap[oneJob.pipelineTopic] <- oneJob
				serializer.childWGrp.Add(1)
				go serializer.handleJobs(oneJob.pipelineTopic)
			}
			serializer.stoppedLk.RUnlock()
		}
	}
	return
}

// One go instance of this method is running per pipelineTopic
// Its job is to execute the jobs serially, and once nothing else can be executed, launch a cleanup routine
func (serializer *PipelineOpSerializer) handleJobs(pipelineTopic string) {
	defer serializer.childWGrp.Done()
	var job Job
	var err error

	serializer.mapMtx.RLock()
	jobCh, ok := serializer.jobTopicMap[pipelineTopic]
	if !ok {
		serializer.logger.Errorf(fmt.Sprintf("Error: Job multiplex channel for %v does not exist", pipelineTopic))
		serializer.mapMtx.RUnlock()
		return
	}
	serializer.mapMtx.RUnlock()

forloop:
	for {
		select {
		case job = <-jobCh:
			if serializer.isStopped() {
				return
			}
			if job.pipelineTopic != pipelineTopic {
				serializer.logger.Errorf("Names %v <-> %v do not match... coding bug", job.pipelineTopic, pipelineTopic)
				continue forloop
			}
			serializer.logger.Infof("PipelineOpSerializer %v handling job: %v", pipelineTopic, job.String())
			switch job.jobType {
			case PipelineGetOrCreate:
				repStatus, repStatusErr := serializer.pipelineMgr.GetOrCreateReplicationStatus(job.pipelineTopic, job.errForUpdateOp)
				// Encapsulate response into a struct
				var repStatusPair SerializerRepStatusPair
				repStatusPair.repStatus = repStatus
				repStatusPair.errCode = repStatusErr
				// Return to caller who is waiting
				job.repStatusCh <- repStatusPair
			case PipelineInit:
				_, err = serializer.pipelineMgr.GetOrCreateReplicationStatus(job.pipelineTopic, nil)
				if err != nil {
					serializer.logger.Warnf("Error getting replication status for pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case PipelineDeletion:
				err = serializer.pipelineMgr.RemoveReplicationStatus(job.pipelineTopic)
				if err != nil {
					serializer.logger.Warnf("Error removing replication status for pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case PipelineUpdate:
				err = serializer.pipelineMgr.Update(job.pipelineTopic, job.errForUpdateOp)
				if err != nil {
					serializer.logger.Warnf("Error updating pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case PipelineUpdateWStoppedCb:
				err = serializer.pipelineMgr.UpdateWithStoppedCb(job.pipelineTopic, job.callbackForWhenPipelineIsStopped, job.errorCbForFailedStoppedOp)
				if err != nil {
					serializer.logger.Errorf("Error updating pipeline %v with callback. err=%v", job.pipelineTopic, err)
				}
			case PipelineReinitStream:
				// Any errors here would be considered critical, as filters in replication spec has already been
				// changed but replication is not reflecting the changes. Raise UI errors to get users' attention
				// as this would cause serious data inconsistencies
				err := serializer.pipelineMgr.CleanupPipeline(job.pipelineTopic)
				if err != nil {
					errMsg := fmt.Sprintf("Error during re-intializing XDCR replication: getting replication status for pipeline %v, err=%v", job.pipelineTopic, err)
					serializer.logger.Errorf(errMsg)
					serializer.pipelineMgr.GetLogSvc().Write(errMsg)
					continue forloop
				}

				err = serializer.pipelineMgr.Update(job.pipelineTopic, job.errForUpdateOp)
				if err != nil {
					serializer.logger.Warnf("Error updating pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case PipelineAutoPause:
				err := serializer.pipelineMgr.PauseReplication(job.pipelineTopic)
				if err != nil {
					serializer.logger.Warnf("Error auto-pausing pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case BackfillPipelineStart:
				err := serializer.pipelineMgr.StartBackfill(job.pipelineTopic)
				if err != nil {
					serializer.logger.Warnf("Error starting backfill pipeline %v. err=%v", job.pipelineTopic, err)
				}
			case BackfillPipelineStop:
				err := serializer.pipelineMgr.StopBackfill(job.pipelineTopic, job.skipBackfillCkpt)
				if err != nil {
					serializer.logger.Warnf("Error stopping backfill pipeline %v. err=%v", job.pipelineTopic, err)
				}
				job.waitGrp.Done()
			case BackfillPipelineStopWStoppedCb:
				err := serializer.pipelineMgr.StopBackfillWithStoppedCb(job.pipelineTopic, job.callbackForWhenPipelineIsStopped, job.errorCbForFailedStoppedOp, job.skipBackfillCkpt)
				if err != nil {
					serializer.logger.Warnf("Error stopping backfill pipeline with callback %v. err=%v", job.pipelineTopic, err)
				}
				job.waitGrp.Done()
			case BackfillPipelineClean:
				err := serializer.pipelineMgr.CleanupBackfillPipeline(job.pipelineTopic)
				if err != nil {
					serializer.logger.Warnf("Error cleaning backfill pipeline ckpts %v. err=%v", job.pipelineTopic, err)
				}
			case DismissEvent:
				err := serializer.pipelineMgr.DismissEvent(job.eventId)
				if err != nil {
					serializer.logger.Warnf("Error dismissing event %v for pipeline %v. err=%v", job.eventId, job.pipelineTopic, err)
				}
			case BackfillMappingStatusUpdate:
				err := serializer.pipelineMgr.BackfillMappingUpdate(job.pipelineTopic, job.diffPair, job.srcManifestsDelta)
				if err != nil {
					serializer.logger.Warnf("Error updating backfill mapping for pipeline %v. err=%v", job.pipelineTopic, err)
				}
			default:
				serializer.logger.Errorf(fmt.Sprintf("Unknown job type: %v -> %v", job.jobType, job.String()))
			}
			serializer.logger.Infof("PipelineOpSerializer %v done handling job: %v", pipelineTopic, job.String())
		default:
			// Cannot read from channel anymore
			serializer.stoppedLk.RLock()
			if !serializer.stopped {
				serializer.childWGrp.Add(1)
				go serializer.cleanupJob(pipelineTopic)
			}
			serializer.stoppedLk.RUnlock()
			// Without a break label, this break would have applied to the innermost select
			break forloop
		}
	}
}

// One instance of this is launched from handleJobs
func (serializer *PipelineOpSerializer) cleanupJob(pipelineTopic string) {
	serializer.mapMtx.Lock()
	defer serializer.mapMtx.Unlock()
	defer serializer.childWGrp.Done()

	_, ok := serializer.jobTopicMap[pipelineTopic]
	if ok {
		if len(serializer.jobTopicMap[pipelineTopic]) == 0 {
			delete(serializer.jobTopicMap, pipelineTopic)
		} else {
			// Someone else snuck in a job while we're supposed to clean up. Re-launch handler, which will finish
			// the job(s) and relaunch another cleanupJob() while this one returns
			serializer.stoppedLk.RLock()
			if !serializer.stopped {
				serializer.childWGrp.Add(1)
				go serializer.handleJobs(pipelineTopic)
			}
			serializer.stoppedLk.RUnlock()
		}
	}
}

func (serializer *PipelineOpSerializer) Stop() {
	serializer.shutdownOnce.Do(func() {
		serializer.stoppedLk.Lock()
		serializer.stopped = true
		serializer.stoppedLk.Unlock()
		serializer.childWGrp.Wait()
		serializer.pipelineMgr = nil
		serializer.logger.Infof("Pipeline Manager Serializer stopped")
	})
}

func (serializer *PipelineOpSerializer) UpdateWithStoppedCb(topic string, callback base.StoppedPipelineCallback, errCb base.StoppedPipelineErrCallback) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var updateJob Job
	updateJob.jobType = PipelineUpdateWStoppedCb
	updateJob.pipelineTopic = topic
	updateJob.callbackForWhenPipelineIsStopped = callback
	updateJob.errorCbForFailedStoppedOp = errCb

	return serializer.distributeJob(updateJob)
}

func (serializer *PipelineOpSerializer) DismissEvent(topic string, eventId int) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var dismissJob Job
	dismissJob.jobType = DismissEvent
	dismissJob.pipelineTopic = topic
	dismissJob.eventId = eventId

	return serializer.distributeJob(dismissJob)
}

func (serializer *PipelineOpSerializer) BackfillMappingStatusUpdate(topic string, diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error {
	if serializer.isStopped() {
		return SerializerStoppedErr
	}

	var statusUpdateJob Job
	statusUpdateJob.jobType = BackfillMappingStatusUpdate
	statusUpdateJob.pipelineTopic = topic
	statusUpdateJob.diffPair = diffPair
	statusUpdateJob.srcManifestsDelta = srcManifestsDelta

	return serializer.distributeJob(statusUpdateJob)
}
