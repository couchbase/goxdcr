package pipeline_manager

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/pipeline"
	"sync"
)

type PipelineMgtOpType int

var MaxNonblockingQueueJobs = 1000
var ErrQueueMaxed error = errors.New("The requested action has been persisted in the metadata store, but was unabled to execute due to too many queued jobs.")
var SerializerStoppedErr error = errors.New("Pipeline Manager is shutting down. The requested action is unable to be processed.")

const (
	PipelineGetOrCreate PipelineMgtOpType = iota
	PipelineInit        PipelineMgtOpType = iota
	PipelineUpdate      PipelineMgtOpType = iota
	PipelineDeletion    PipelineMgtOpType = iota
)

type PipelineOpSerializerIface interface {
	// Asynchronous User APIs
	Delete(topic string) error
	Update(topic string, err error) error
	Init(topic string) error

	// Synchronous User APIs - call and get data from a channel
	GetOrCreateReplicationStatus(topic string, cur_err error) (*pipeline.ReplicationStatus, error)

	// APIs for Pipeline Manager to drive
	Stop()
}

type SerializerRepStatusPair struct {
	repStatus (*pipeline.ReplicationStatus)
	errCode   error
}

// A job is consisted of a name and an operation to be serialized
type Job struct {
	// Main job types
	pipelineTopic string
	jobType       PipelineMgtOpType

	// Aux inputs for jobs
	errForUpdateOp error

	// Optional outputs from jobs
	repStatusCh chan SerializerRepStatusPair
}

type PipelineOpSerializer struct {
	// Pipeline Manager related stuff
	pipelineMgr Pipeline_mgr_iface
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
func NewPipelineOpSerializer(pipelineMgrIn Pipeline_mgr_iface, logger *log.CommonLogger) *PipelineOpSerializer {
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

forloop:
	for {
		serializer.mapMtx.RLock()

		_, ok := serializer.jobTopicMap[pipelineTopic]
		if !ok {
			serializer.logger.Errorf(fmt.Sprintf("Error: Job multiplex channel for %v does not exist", pipelineTopic))
			serializer.mapMtx.RUnlock()
			return
		}

		select {
		case job = <-serializer.jobTopicMap[pipelineTopic]:
			// Unlock Readlock here while the processing below takes place
			serializer.mapMtx.RUnlock()
			if serializer.isStopped() {
				return
			}
			if job.pipelineTopic != pipelineTopic {
				serializer.logger.Errorf("Names %v <-> %v do not match... coding bug", job.pipelineTopic, pipelineTopic)
				continue forloop
			}
			serializer.logger.Infof("PipelineOpSerializer %v handling job: %v", pipelineTopic, job)
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
				serializer.pipelineMgr.GetOrCreateReplicationStatus(job.pipelineTopic, nil)
			case PipelineDeletion:
				serializer.pipelineMgr.RemoveReplicationStatus(job.pipelineTopic)
			case PipelineUpdate:
				serializer.pipelineMgr.Update(job.pipelineTopic, job.errForUpdateOp)
			default:
				serializer.logger.Errorf(fmt.Sprintf("Unknown job type: %v -> %v", job.jobType, job))
			}
			serializer.logger.Infof("PipelineOpSerializer %v done handling job: %v", pipelineTopic, job)
		default:
			// Cannot read from channel anymore
			serializer.stoppedLk.RLock()
			if !serializer.stopped {
				serializer.childWGrp.Add(1)
				go serializer.cleanupJob(pipelineTopic)
			}
			serializer.stoppedLk.RUnlock()
			serializer.mapMtx.RUnlock()
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
