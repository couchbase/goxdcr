package conflictlog

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	"github.com/couchbase/goxdcr/v8/utils"
)

var _ baseclog.Logger = (*LoggerImpl)(nil)

const (
	ThrottlerTimeoutPercent = float64(0.20)
)

// LoggerImpl implements the Logger interface
type LoggerImpl struct {
	logger *log.CommonLogger

	// id uniquely identifies a logger instance
	id string

	// utils object for misc utilities
	utils utils.UtilsIface

	// replId is the unique replication ID
	replId string

	// rules describe the mapping to the target conflict bucket
	rulesLock sync.RWMutex

	// connPool is used to get the connection to the cluster with conflict bucket
	connPool iopool.ConnPool

	// opts are the logger options
	opts LoggerOptions

	// lock is the logger level RW lock
	lock sync.RWMutex

	// throttlerSvc is the IOPS throttler
	throttlerSvc throttlerSvc.ThroughputThrottlerSvc

	// number of attempts to try for throttler
	throttlerAttempts int

	// logReqCh is the work queue for all logging requests
	logReqCh chan logRequest

	// finch is intended to signal closure of the logger
	finch chan bool

	// shutdownWorkerCh is intended to shut a subset of workers
	// This is used when new worker count is lesser than the current one
	shutdownWorkerCh chan bool

	// wg is used to wait for outstanding log requests to be finished
	wg sync.WaitGroup

	// an abstract component has the ability to register async listeners,
	// as required for generating statistics events.
	*component.AbstractComponent

	// these stats will be printed periodically to help debug issues.
	stats clStats

	// used to produce UI events.
	eventsProducer common.PipelineEventsProducer

	// error counts that will decide hibernation.
	// The logger will be hibernated when: errCnt reaches X across workers
	// in a time period T, i.e. time.Now() - errStartTime <= T
	errorCnt       uint64
	errorStartTime *time.Time

	// hibernated meaning the logger is not accepting any requests.
	// This can happen when logger was in a error state for a long period of time.
	// 0 is logger is not in hibernation, unix epoch timestamp of hibernation start time otherwise.
	hibernated         atomic.Bool
	hibernationLock    sync.Mutex
	hibernationEventId int64
}

type logRequest struct {
	conflictRec *ConflictRecord
	//ackCh is the channel on which the result of the logging is reported back
	ackCh chan error
}

type clStats struct {
	recordCnt atomic.Uint64

	// conflict logging of a single conflict detected by xmem is mainly spent time
	// in the following four phases.
	waitTime          atomic.Uint64
	throttleLatency   atomic.Uint64
	preprocessLatency atomic.Uint64
	totalWriteLatency atomic.Uint64

	// miscellaneous stats
	poolGetLatency atomic.Uint64
	poolGetCnt     atomic.Uint64

	// the following will be used to decide to print the stats or not.
	lastRecordCnt atomic.Uint64
	tickCnt       atomic.Uint64
}

// calcThrottlerAttempts returns the number of attempts to get go ahead
// from the throttler
func calcThrottlerAttempts(setMetaTimeout time.Duration) int {

	slotMs := float64(time.Second.Milliseconds()) / float64(base.NumberOfSlotsForThroughputThrottling)
	tmp := (float64(setMetaTimeout.Milliseconds()) * ThrottlerTimeoutPercent) / slotMs

	attempts := int(tmp)
	if attempts == 0 {
		attempts = 1
	}

	return attempts
}

// newLoggerImpl creates a new logger impl instance.
// throttlerSvc is allowed to be nil, which means no throttling
func newLoggerImpl(logger *log.CommonLogger, replId string, utils utils.UtilsIface, throttlerSvc throttlerSvc.ThroughputThrottlerSvc, connPool iopool.ConnPool, eventsProducer common.PipelineEventsProducer, opts ...LoggerOpt) (l *LoggerImpl, err error) {
	// set the defaults
	options := LoggerOptions{
		rules:                nil,
		mapper:               NewConflictMapper(logger),
		logQueueCap:          base.DefaultCLogQueueCapacity,
		workerCount:          base.DefaultCLogWorkerCount,
		networkRetryCount:    base.DefaultCLogNetworkRetryCount,
		networkRetryInterval: time.Duration(base.DefaultCLogNetworkRetryIntervalMs) * time.Millisecond,
		poolGetTimeout:       time.Duration(base.DefaultCLogPoolGetTimeoutMs) * time.Millisecond,
		setMetaTimeout:       time.Duration(base.DefaultCLogSetMetaTimeoutMs) * time.Millisecond,
		maxErrorCount:        base.DefaultCLogMaxErrorCount,
		errorTimeWindow:      time.Duration(base.DefaultCLogErrorTimeWindowMs) * time.Millisecond,
		reattemptDuration:    time.Duration(base.DefaultCLogReattemptDurationMs) * time.Millisecond,
	}

	// override the defaults
	for _, opt := range opts {
		opt(&options)
	}

	throttlerAttempts := calcThrottlerAttempts(options.setMetaTimeout)
	if throttlerAttempts == 0 {
		throttlerAttempts = 1
	}

	loggerId := fmt.Sprintf("%s_%v", ConflictLoggerName, newLoggerId())
	logger.Infof("creating new conflict logger id=%s replId=%s loggerOptions=%s throttlerAttempts=%d", loggerId, replId, options.String(), throttlerAttempts)

	l = &LoggerImpl{
		logger:             logger,
		id:                 loggerId,
		utils:              utils,
		replId:             replId,
		rulesLock:          sync.RWMutex{},
		connPool:           connPool,
		opts:               options,
		lock:               sync.RWMutex{},
		throttlerSvc:       throttlerSvc,
		throttlerAttempts:  throttlerAttempts,
		logReqCh:           make(chan logRequest, options.logQueueCap),
		finch:              make(chan bool, 1),
		shutdownWorkerCh:   make(chan bool, LoggerShutdownChCap),
		wg:                 sync.WaitGroup{},
		AbstractComponent:  component.NewAbstractComponentWithLogger(loggerId, logger),
		eventsProducer:     eventsProducer,
		hibernationEventId: -1,
	}

	return
}

func (l *LoggerImpl) PrintStatusSummary() {
	if l.isClosed() {
		return
	}

	// We will print the stats only if one of the below condition is true:
	// 1. there were conflicts in the last base.StatsLogInterval
	// 2. it's been 30 minutes since we last printed the stats (by default).
	maxFrequency := uint64(base.CLogStatsLoggingMaxFreqInterval / base.StatsLogInterval)

	numRecords := l.stats.recordCnt.Load()
	defer l.stats.tickCnt.Add(1)

	if numRecords == l.stats.lastRecordCnt.Load() &&
		l.stats.tickCnt.Load()%maxFrequency != 0 {
		return
	}

	l.stats.lastRecordCnt.Store(numRecords)

	avgWaitTimeLatency := 0.0
	avgThrottleLatency := 0.0
	avgPreprocessLatency := 0.0
	avgTotalWriteLatency := 0.0
	avgPoolGetLatency := 0.0
	if numRecords > 0 {
		avgWaitTimeLatency = float64(l.stats.waitTime.Load() / numRecords)
		avgThrottleLatency = float64(l.stats.throttleLatency.Load() / numRecords)
		avgPreprocessLatency = float64(l.stats.preprocessLatency.Load() / numRecords)
		avgTotalWriteLatency = float64(l.stats.totalWriteLatency.Load() / numRecords)
	}

	poolGetCnt := l.stats.poolGetCnt.Load()
	if poolGetCnt > 0 {
		avgPoolGetLatency = float64(l.stats.poolGetLatency.Load() / poolGetCnt)
	}

	l.logger.Infof("%s stats: conflicts=%v, wait=%v, throttle=%v, preprocess=%v, totalWrite=%v, poolGet=%v, queueLen=%v, errCnt=%v, errStartTime=%v, hib=%v",
		l.Id(), numRecords, avgWaitTimeLatency, avgThrottleLatency, avgPreprocessLatency, avgTotalWriteLatency, avgPoolGetLatency, len(l.logReqCh), l.errorCnt, l.errorStartTime, l.hibernated.Load())
}

func (l *LoggerImpl) Id() string {
	return l.id
}

func (l *LoggerImpl) log(c *ConflictRecord) (ackCh chan error, err error) {
	l.stats.recordCnt.Add(1)

	if l.hibernated.Load() {
		err = baseclog.ErrLoggerHibernated
		return
	}

	ackCh = make(chan error, 1)
	req := logRequest{
		conflictRec: c,
		ackCh:       ackCh,
	}

	l.lock.RLock()
	defer l.lock.RUnlock()

	select {
	case <-l.finch:
		err = baseclog.ErrLoggerClosed
	case l.logReqCh <- req:
	default:
		err = baseclog.ErrQueueFull
	}

	return
}

func (l *LoggerImpl) QueueSize() int {
	return len(l.logReqCh)
}

func (l *LoggerImpl) Log(c baseclog.Conflict) (h base.ConflictLoggerHandle, err error) {
	if l.logger.GetLogLevel() >= log.LogLevelTrace {
		l.logger.Tracef("logging conflict record scope=%s collection=%s", c.Scope(), c.Collection())
	}

	if l.isClosed() {
		err = baseclog.ErrLoggerClosed
		return
	}

	record, ok := c.(*ConflictRecord)
	if !ok {
		err = fmt.Errorf("wrong type of conflict record %T", c)
		return
	}

	ackCh, err := l.log(record)
	if err != nil {
		return
	}

	h = logReqHandle{
		ackCh: ackCh,
	}

	return
}

func (l *LoggerImpl) isClosed() bool {
	select {
	case <-l.finch:
		return true
	default:
		return false
	}
}

func (l *LoggerImpl) close() (err error) {
	if l.isClosed() {
		return nil
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	// check for closed channel as multiple threads could have attempted it
	if l.isClosed() {
		return nil
	}

	l.logger.Infof("closing conflict logger id=%s replId=%s", l.id, l.replId)

	// The close is implemented on the same principle that some restaurants have:
	// "last entry at 10:00 PM". When translated here it means, we will disallow
	// new requests but existing requests will be served and results acknowledged.
	// We also will wait for all of these to be completed.
	close(l.finch)

	// We close the work channel as well i.e. logReqCh. This is because we want to
	// handle inflight log requests (both in the channel and in workers)
	close(l.logReqCh)

	l.wg.Wait()
	l.logger.Infof("closing of conflict logger done id=%s replId=%s", l.id, l.replId)

	return
}

func (l *LoggerImpl) startWorker() {
	l.wg.Add(1)
	go l.worker()
}

func (l *LoggerImpl) UpdateWorkerCount(newCount int) (err error) {
	l.logger.Debugf("going to change conflict logger worker count old=%d new=%d id=%s", l.opts.workerCount, newCount, l.id)

	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	if newCount <= 0 || newCount == l.opts.workerCount {
		err = baseclog.ErrNoChange
		return
	}

	l.logger.Infof("changing conflict logger worker count old=%d new=%d id=%s", l.opts.workerCount, newCount, l.id)
	defer l.logger.Infof("done changing conflict logger worker count old=%d new=%d id=%s", l.opts.workerCount, newCount, l.id)

	if newCount > l.opts.workerCount {
		for i := 0; i < (newCount - l.opts.workerCount); i++ {
			l.startWorker()
		}
	} else {
		for i := 0; i < (l.opts.workerCount - newCount); i++ {
			l.shutdownWorkerCh <- true
		}
	}

	l.opts.workerCount = newCount
	return
}

// r should be non-nil
func (l *LoggerImpl) UpdateRules(r *baseclog.Rules) (err error) {
	l.logger.Debugf("going to change conflict logger rules old=%d new=%d", l.opts.rules, r)
	if l.isClosed() {
		return
	}

	l.rulesLock.Lock()
	defer l.rulesLock.Unlock()

	if r == nil {
		err = baseclog.ErrEmptyRules
		return
	}

	err = r.Validate()
	if err != nil {
		return
	}

	if l.opts.rules.SameAs(r) {
		err = baseclog.ErrNoChange
		return
	}

	l.opts.rules = r

	l.logger.Infof("conflict logger rules=%v updated id=%s", r, l.id)
	return
}

func (l *LoggerImpl) worker() {
	defer l.wg.Done()

	for {
		select {
		case <-l.shutdownWorkerCh:
			l.logger.Infof("shutting down conflict log worker replId=%s, id=%s", l.replId, l.id)
			return
		case req := <-l.logReqCh:
			// nil implies that logReqCh might be closed
			if req.conflictRec == nil {
				return
			}

			queueTime := time.Since(req.conflictRec.StartTime).Microseconds()
			l.stats.waitTime.Add(uint64(queueTime))
			req.conflictRec.ResetStartTime()

			err := l.processReq(req)
			req.ackCh <- err
			if err != nil {
				l.hibernateIfNeeded(err)
			}

			totalWriteTime := time.Since(req.conflictRec.StartTime).Microseconds()
			l.stats.totalWriteLatency.Add(uint64(totalWriteTime))
		}
	}
}

func (l *LoggerImpl) getTarget(rec *ConflictRecord) (t baseclog.Target, err error) {
	l.rulesLock.RLock()
	defer l.rulesLock.RUnlock()

	t, err = l.opts.mapper.Map(l.opts.rules, rec)
	if err != nil {
		return
	}

	return
}

func (l *LoggerImpl) getFromPool(bucketName string) (conn Connection, err error) {
	now := time.Now()

	obj, err := l.connPool.Get(bucketName, l.opts.poolGetTimeout)
	if err != nil {
		return
	}

	conn, ok := obj.(Connection)
	if !ok {
		err = fmt.Errorf("pool object is of invalid type got=%T", obj)
		return
	}

	poolGetLatency := time.Since(now).Microseconds()
	l.stats.waitTime.Add(uint64(poolGetLatency))
	l.stats.poolGetCnt.Add(1)

	return
}

func (l *LoggerImpl) throttle() error {
	if l.throttlerSvc == nil {
		return nil
	}

	var ok bool

	// The attempt count is guaranteed to be atleast 1
	for i := 0; i < l.throttlerAttempts; i++ {
		ok = l.throttlerSvc.CanSend(throttlerSvc.ThrottlerReqCLog)
		if ok {
			return nil
		}

		// If its a last attempt then there is no point in waiting
		// so we bail
		if i == l.throttlerAttempts-1 {
			break
		}

		l.throttlerSvc.Wait()
	}

	return baseclog.ErrThrottle
}

func (l *LoggerImpl) writeDocsWithTimeout(req logRequest, target baseclog.Target, timeout time.Duration) (err error) {
	resultCh := make(chan error, 1)
	go func() {
		err := l.writeDocs(req, target)
		resultCh <- err
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()

	select {
	case err := <-resultCh:
		return err
	case <-t.C:
		return baseclog.ErrTimeout
	}
}

func (l *LoggerImpl) writeDocs(req logRequest, target baseclog.Target) (err error) {

	srcVbSeqnoPair := []interface{}{req.conflictRec.Source.VBNo, req.conflictRec.Source.Seqno}
	pipelineType := req.conflictRec.OriginatingPipeline

	// Write source document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err := conn.SetMeta(req.conflictRec.Source.Id, req.conflictRec.Source.Body, req.conflictRec.Source.Datatype, target)
		return err
	}, req.conflictRec.OriginatingPipeline)
	if err != nil {
		// errors are not logged here as it will spam it when we get errors like TMPFAIL
		// we let the callers deal with it
		return err
	}
	l.RaisePipelineSpecificEvent(common.NewEvent(common.CLogDocsWritten, SourceDoc, l, srcVbSeqnoPair, nil), pipelineType)

	// Write target document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = conn.SetMeta(req.conflictRec.Target.Id, req.conflictRec.Target.Body, req.conflictRec.Target.Datatype, target)
		return err
	}, req.conflictRec.OriginatingPipeline)
	if err != nil {
		return err
	}
	l.RaisePipelineSpecificEvent(common.NewEvent(common.CLogDocsWritten, TargetDoc, l, srcVbSeqnoPair, nil), pipelineType)

	// Write conflict record.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = conn.SetMeta(req.conflictRec.Id, req.conflictRec.Body, req.conflictRec.Datatype, target)
		return err
	}, req.conflictRec.OriginatingPipeline)
	if err != nil {
		return err
	}
	l.RaisePipelineSpecificEvent(common.NewEvent(common.CLogDocsWritten, CRD, l, srcVbSeqnoPair, nil), pipelineType)

	return
}

// Processes conflict bucket write errors by raising necessary events.
// Returns the fatality level of error passed in (needs retry, no retry etc)
func (l *LoggerImpl) processWriteError(err error, originatingPipelineType common.PipelineType) writeError {
	if err == nil {
		return noRetryErr
	}

	nwError := l.utils.IsSeriousNetError(err)
	l.RaisePipelineSpecificEvent(common.NewEvent(common.CLogWriteStatus, err, l, nil, nwError), originatingPipelineType)

	if nwError {
		return networkErr
	}

	switch err {
	case baseclog.ErrTMPFAIL:
		fallthrough
	case baseclog.ErrNotMyVBucket:
		fallthrough
	case baseclog.ErrUnknownCollection:
		return needRetryErr
	case baseclog.ErrImpossibleResp:
		fallthrough
	case baseclog.ErrFatalResp:
		fallthrough
	case baseclog.ErrEACCESS:
		fallthrough
	case baseclog.ErrGuardrail:
		fallthrough
	case baseclog.ErrUnknownResp:
		return noRetryErr
	default:
		return unknownErr
	}
}

// writeDocRetry arranges for a connection from the pool which the supplied function can use. The function
// wraps the supplied function to check for network errors and appropriately releases the connection back
// to the pool
func (l *LoggerImpl) writeDocRetry(bucketName string, fn func(conn Connection) error, originatingPipelineType common.PipelineType) (err error) {
	var conn Connection
	var nwError bool
	for i := 0; i < l.opts.networkRetryCount; i++ {
		conn, err = l.getFromPool(bucketName)
		if err != nil {
			if base.SelectBucketErrBucketDNE(err) {
				// the conflict bucket doesn't exist. No need to retry.
				err = baseclog.ErrRules
				return
			}

			nwError = l.utils.IsSeriousNetError(err)
			l.RaisePipelineSpecificEvent(common.NewEvent(common.CLogWriteStatus, err, l, nil, nwError), originatingPipelineType)
			// This is to account for nw errors while connecting
			if !nwError {
				return
			}
			time.Sleep(l.opts.networkRetryInterval)
			continue
		}

		if l.logger.GetLogLevel() >= log.LogLevelTrace {
			l.logger.Tracef("got connection from pool, id=%d lid=%s bucket=%s",
				conn.Id(), l.id, bucketName)
		}

		// The call to connPool.Put is not advised to be done in a defer here.
		// This is because calling defer in a loop accumulates multiple defers
		// across the iterations and calls all of them when function exits which
		// will be error prone in this case
		err = fn(conn)
		if err == baseclog.ErrScopeColNotFound {
			err = baseclog.ErrRules
			return
		}

		writeErr := l.processWriteError(err, originatingPipelineType)
		if err == nil || writeErr.noRetryNeeded() {
			// there was no network error and the write was a success or
			// the write returned a status code which cannot be fixed by retry
			if err != nil {
				l.logger.Debugf("releasing connection to pool after write with error, id=%d damaged=%v err=%v lid=%s bucket=%s",
					conn.Id(), false, err, l.id, bucketName)
			} else {
				if l.logger.GetLogLevel() >= log.LogLevelTrace {
					l.logger.Tracef("releasing connection to pool after write, id=%d damaged=%v err=%v lid=%s bucket=%s",
						conn.Id(), false, err, l.id, bucketName)
				}
			}
			l.connPool.Put(bucketName, conn, false)
			break
		}

		// there was either a network error or there needs to be a retry
		nwError = writeErr.isNWError()
		// Errors are not logged, but captured in the form of stats.
		// One has to turn on debug logging to capture the errors that are not captured by the stats
		l.logger.Debugf("error in writing doc to conflict bucket err=%v, id=%d lid=%s nwError=%v bucket=%s", err, conn.Id(), l.id, nwError, bucketName)
		l.connPool.Put(bucketName, conn, nwError)
		time.Sleep(l.opts.networkRetryInterval)
	}

	return err
}

func (l *LoggerImpl) processReq(req logRequest) error {
	var err error

	err = l.throttle()
	if err != nil {
		return err
	}

	throttleTime := time.Since(req.conflictRec.StartTime).Microseconds()
	l.stats.throttleLatency.Add(uint64(throttleTime))
	req.conflictRec.ResetStartTime()

	target, err := l.getTarget(req.conflictRec)
	if err != nil {
		l.logger.Errorf("%s error getting conflict log target, r=%v%+v%v, rules=%s, err=%v",
			l.id, base.UdTagBegin, req.conflictRec, base.UdTagEnd, l.opts.rules.String(), err,
		)
		return err
	}

	if target.IsBlacklistTarget() {
		// this conflict shouldn't be logged.
		return nil
	}

	err = req.conflictRec.PopulateData(l.replId, req.conflictRec.OriginatingPipeline)
	if err != nil {
		l.logger.Errorf("%s error populating clog data, r=%v%+v%v, err=%v",
			l.id, base.UdTagBegin, req.conflictRec, base.UdTagEnd, err,
		)
		return err
	}

	preprocessTime := time.Since(req.conflictRec.StartTime).Microseconds()
	l.stats.preprocessLatency.Add(uint64(preprocessTime))
	req.conflictRec.ResetStartTime()

	timeout := l.opts.setMetaTimeout
	err = l.writeDocsWithTimeout(req, target, timeout)
	if err != nil {
		return err
	}

	return nil
}

type logReqHandle struct {
	ackCh chan error
}

func (h logReqHandle) Wait(finch chan bool) (err error) {
	select {
	case <-finch:
		err = baseclog.ErrLogWaitAborted
	case err = <-h.ackCh:
	}

	return
}

// newCap should be greater than or equal to existing queue capacity
func (l *LoggerImpl) UpdateQueueCapcity(newCap int) (err error) {
	l.logger.Debugf("going to change conflict logger queue capacity old=%d new=%d id=%s", l.opts.logQueueCap, newCap, l.id)

	if l.isClosed() {
		return baseclog.ErrLoggerClosed
	}

	// acquire the lock so that no more writes to the channel happens.
	// read from channel by the workers can still happen, so we shut down the workers temporarily.
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.logReqCh == nil {
		err = fmt.Errorf("nil queue")
		return
	}

	if l.opts.logQueueCap == newCap {
		err = baseclog.ErrNoChange
		return
	}

	if newCap < l.opts.logQueueCap {
		// pipeline should have restarted, fatal error
		err = fmt.Errorf("newCap=%v < oldCap=%v, need to restart pipeline", newCap, l.opts.logQueueCap)
		return
	}

	l.logger.Infof("changing conflict logger queue capacity old=%d new=%d id=%s", l.opts.logQueueCap, newCap, l.id)
	defer l.logger.Infof("done changing conflict logger queue capacity cap=%d id=%s", l.opts.logQueueCap, l.id)

	// temporarily make sure there is no one reading from the queue.
	for i := 0; i < l.opts.workerCount; i++ {
		l.shutdownWorkerCh <- true
	}
	l.wg.Wait()

	defer func() {
		// bring back the workers
		for i := 0; i < l.opts.workerCount; i++ {
			l.startWorker()
		}
	}()

	newQueue := make(chan logRequest, newCap)
	oldQueue := l.logReqCh

	length := len(oldQueue)
	for i := 0; i < length; i++ {
		select {
		case req, ok := <-oldQueue:
			if !ok || req.conflictRec == nil {
				// someone else closed the channel.
				// unlikely that we will come here because we are under the lock.
				continue
			}
			newQueue <- req
		default:
			// the code should not reach here,
			// but have a bail out path just in case.
		}
	}
	close(oldQueue)
	l.logReqCh = newQueue
	l.opts.logQueueCap = newCap

	return
}

func (l *LoggerImpl) UpdateNWRetryCount(cnt int) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger network retry count old=%d new=%d id=%s", l.opts.networkRetryCount, cnt, l.id)

	l.opts.networkRetryCount = cnt
}

func (l *LoggerImpl) UpdateNWRetryInterval(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger network retry interval old=%v new=%v id=%s", l.opts.networkRetryInterval, t, l.id)

	l.opts.networkRetryInterval = t
}

func (l *LoggerImpl) UpdateSetMetaTimeout(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger set meta timeout old=%v new=%v id=%s", l.opts.setMetaTimeout, t, l.id)

	l.opts.setMetaTimeout = t
}

func (l *LoggerImpl) UpdateGetFromPoolTimeout(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger pool get timeout old=%v new=%v id=%s", l.opts.poolGetTimeout, t, l.id)

	l.opts.poolGetTimeout = t
}

func (l *LoggerImpl) UpdateMaxErrorCount(cnt int) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger max error count old=%v new=%v id=%s", l.opts.maxErrorCount, cnt, l.id)

	l.opts.maxErrorCount = cnt
}

func (l *LoggerImpl) UpdateErrorTimeWindow(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger error time window old=%v new=%v id=%s", l.opts.errorTimeWindow, t, l.id)

	l.opts.errorTimeWindow = t
}

func (l *LoggerImpl) UpdateReattemptDuration(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.lock.Lock()
	defer l.lock.Unlock()

	l.logger.Infof("changing conflict logger reattempt duration old=%v new=%v id=%s", l.opts.reattemptDuration, t, l.id)

	l.opts.reattemptDuration = t
}

func (l *LoggerImpl) Attach(_ common.Pipeline) error {
	// noop - conflict logger doesn't need to have access to the pipeline itself.
	return nil
}

func (l *LoggerImpl) Start(_ metadata.ReplicationSettingsMap) error {
	l.logger.Infof("spawning conflict logger workers replId=%s count=%d id=%s", l.replId, l.opts.workerCount, l.id)

	// send an event down to the statsMgr of both main and backfill (if exists) pipelines,
	// to mark the prometheus stat as unhibernated or running
	l.RaiseEvent(common.NewEvent(common.CLogWriteStatus, baseclog.ErrLoggerRunning, l, nil, false))

	for i := 0; i < l.opts.workerCount; i++ {
		l.startWorker()
	}

	return nil
}

func (l *LoggerImpl) Stop() error {
	err := l.close()
	if err != nil {
		return err
	}

	// unhibernate the logger to get rid of the UI error message
	l.unhibernate()

	return nil
}

// The following conflict logging settings update should result in a restart of pipeline,
// and which cannot be handled on a live basis by this function:
// 1. conflict logging mapping change (i.e. On/Enabled -> Off/Disabled change)
// 2. conflict logging mapping change (i.e Off/Disabled -> On/Enabled change)
// 3. conflict logging queue capacity decrease (i.e. newCap < oldCap).
func (l *LoggerImpl) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	conflictLoggingIsOn := l != nil

	// change of conflict logging rules if needed
	var conflictLoggingMap base.ConflictLoggingMappingInput
	conflictLoggingIn, conflictLoggingMapExists := settings[base.CLogKey]
	if !conflictLoggingMapExists {
		// no conflict logging update exists, which is fine.
		return nil
	}

	conflictLoggingMap, conflictLoggingMapExists = base.ParseConflictLoggingInputType(conflictLoggingIn)
	if !conflictLoggingMapExists {
		// wrong type
		err := fmt.Errorf("conflict map %v as input, is of invalid type %v. Ignoring the update",
			conflictLoggingIn, reflect.TypeOf(conflictLoggingIn))
		l.logger.Errorf(err.Error())
		return err
	}

	if conflictLoggingMap == nil {
		// nil is not an accepted value, should not reach here.
		err := fmt.Errorf("conflict map is nil as input, but is cannot be nil. Ignoring %v for the update",
			conflictLoggingIn)
		l.logger.Errorf(err.Error())
		return err
	}

	// validate if we can perform live pipeline update or not.
	if conflictLoggingIsOn {
		if conflictLoggingMap.Disabled() {
			// conflict logging is on in the pipeline right now
			// and the setting update was to turn it off. The pipeline should have restarted.
			err := fmt.Errorf("conflict map respresents Off as input. Ignoring %v for the update because pipeline should have restarted",
				conflictLoggingIn)
			l.logger.Errorf(err.Error())
			return err
		}
	} else {
		if !conflictLoggingMap.Disabled() {
			// conflict logging is off in the pipeline right now
			// and the setting update was to turn it on. The pipeline should have restarted.
			err := fmt.Errorf("conflict map respresents On as input. Ignoring %v for the update because pipeline should have restarted",
				conflictLoggingIn)
			log.NewLogger(ConflictLoggerName, log.DefaultLoggerContext).Errorf(err.Error())
			return err
		}

		// No need of settings update.
		// Conflict logging is Off and will remain off.
		return nil
	}

	// update logger queue capacity if needed
	loggerQueueCap, ok := settings[base.CLogQueueCapacity]
	if ok {
		cnt, ok := loggerQueueCap.(int)
		if ok && cnt != 0 {
			err := l.UpdateQueueCapcity(cnt)
			if err != nil && err != baseclog.ErrNoChange {
				l.logger.Errorf("error updating queue capacity to %v", cnt)
				return err
			}
		}
	}

	// update logger setMeta timeout if needed
	setMetaTimeoutMs, ok := settings[base.CLogSetMetaTimeout]
	if ok {
		t, ok := setMetaTimeoutMs.(int)
		if ok {
			l.UpdateSetMetaTimeout(time.Duration(t) * time.Millisecond)
		}
	}

	// update logger getPool timeout if needed
	getPoolTimeoutMs, ok := settings[base.CLogPoolGetTimeout]
	if ok {
		t, ok := getPoolTimeoutMs.(int)
		if ok {
			l.UpdateGetFromPoolTimeout(time.Duration(t) * time.Millisecond)
		}
	}

	// update network retry interval if needed
	nwRetryIntervalMs, ok := settings[base.CLogNetworkRetryInterval]
	if ok {
		t, ok := nwRetryIntervalMs.(int)
		if ok {
			l.UpdateNWRetryInterval(time.Duration(t) * time.Millisecond)
		}
	}

	// update network retry count if needed
	nwRetryCount, ok := settings[base.CLogNetworkRetryCount]
	if ok {
		cnt, ok := nwRetryCount.(int)
		if ok {
			l.UpdateNWRetryCount(cnt)
		}
	}

	// update logger worker count if needed
	workerCount, ok := settings[base.CLogWorkerCount]
	if ok {
		cnt, ok := workerCount.(int)
		if ok && cnt != 0 {
			err := l.UpdateWorkerCount(cnt)
			if err != nil && err != baseclog.ErrNoChange {
				l.logger.Errorf("error updating worker count to %v", cnt)
				return err
			}
		}
	}

	// update max error count if needed
	maxErrCnt, ok := settings[base.CLogMaxErrorCount]
	if ok {
		cnt, ok := maxErrCnt.(int)
		if ok && cnt != 0 {
			l.UpdateMaxErrorCount(cnt)
		}
	}

	// update error time window
	errTimeWindow, ok := settings[base.CLogErrorTimeWindow]
	if ok {
		t, ok := errTimeWindow.(int)
		if ok {
			l.UpdateErrorTimeWindow(time.Duration(t) * time.Millisecond)
		}
	}

	// update reattempt duration if needed
	reattemptDuration, ok := settings[base.CLogReattemptDuration]
	if ok {
		t, ok := reattemptDuration.(int)
		if ok {
			l.UpdateReattemptDuration(time.Duration(t) * time.Millisecond)
		}
	}

	// change of conflict logging rules if needed
	if conflictLoggingMapExists {
		// conflict logging is on and its settings needs to be updated.
		err := UpdateLoggerWithRules(conflictLoggingMap, l, l.replId, l.logger)
		if err != nil {
			err := fmt.Errorf("error updating existing conflict logging rules, ignoring %v and continuing with old rules. err=%v",
				conflictLoggingMap, err)
			l.logger.Errorf(err.Error())
			return err
		}
	}

	return nil
}

// If a service can be shared by more than one pipeline in the same replication.
// Yes - logger is shared between main and backfill pipeline.
func (l *LoggerImpl) IsSharable() bool {
	return true
}

// If sharable, allow service to be detached
func (l *LoggerImpl) Detach(pipeline common.Pipeline) error {
	// no-op
	return nil
}

// the function handles errors and hibernates the logger on continuous count of specific errors.
func (l *LoggerImpl) hibernateIfNeeded(err error) {
	if err != baseclog.ErrRules &&
		err != baseclog.ErrThrottle &&
		err != baseclog.ErrTimeout {
		return
	}

	l.hibernationLock.Lock()
	defer l.hibernationLock.Unlock()

	if l.isClosed() || l.hibernated.Load() {
		// already hibernated or logger closed.
		return
	}

	var needToHibernate bool

	timeNow := time.Now()
	if l.errorCnt == 0 {
		// first occurance of the error
		l.errorStartTime = &timeNow
		l.errorCnt = 1
	} else if l.errorStartTime != nil && timeNow.Sub(*l.errorStartTime) <= l.opts.errorTimeWindow {
		// error inside the timeframe of the first error
		l.errorCnt++
		if l.errorCnt == uint64(l.opts.maxErrorCount) {
			needToHibernate = true
		}
	} else {
		// error outside the timeframe of the first error. Reset the count.
		l.errorCnt = 0
		l.errorStartTime = nil
	}

	if !needToHibernate || l.opts.reattemptDuration == 0 {
		return
	}

	// raise an event for both main and backfill (if exists) pipelines.
	l.RaiseEvent(common.NewEvent(common.CLogWriteStatus, baseclog.ErrLoggerHibernated, l, nil, false))

	// hibernate the logger
	defer l.logger.Infof("Conflict logger hibernated rid=%s lid=%s", l.replId, l.id)

	var uiMessage string
	if err == baseclog.ErrRules {
		uiMessage = fmt.Sprintf(HibernationRulesErrUIStr, l.opts.reattemptDuration/time.Minute)
	} else {
		uiMessage = fmt.Sprintf(HibernationSystemErrUIStr, l.opts.maxErrorCount, l.opts.errorTimeWindow/time.Second, l.opts.reattemptDuration/time.Minute)
	}

	l.hibernated.Store(true)
	eventId := l.eventsProducer.AddEvent(base.LowPriorityMsg, uiMessage, base.EventsMap{}, nil)
	l.hibernationEventId = eventId

	// unhibernate the logger eventually
	time.AfterFunc(l.opts.reattemptDuration, func() {
		l.unhibernate()
	})
}

func (l *LoggerImpl) unhibernate() {
	l.hibernationLock.Lock()
	defer l.hibernationLock.Unlock()

	if l.isClosed() || !l.hibernated.Load() {
		// if logger is closed or logger is already unhibernated.
		return
	}

	// send an event down to the statsMgr of both main and backfill (if exists) pipelines,
	// to mark the prometheus stat as unhibernated
	l.RaiseEvent(common.NewEvent(common.CLogWriteStatus, baseclog.ErrLoggerRunning, l, nil, false))

	l.hibernated.Store(false)
	l.errorCnt = 0
	l.errorStartTime = nil

	uiEventId := l.hibernationEventId
	defer l.logger.Infof("Conflict logger unhibernated rid=%s lid=%s event=%v", l.replId, l.id, uiEventId)
	if uiEventId == -1 {
		return
	}

	err := l.eventsProducer.DismissEvent(int(l.hibernationEventId))
	if err != nil && err != base.ErrorNotFound {
		l.logger.Errorf("Error dismissing event=%v rid=%s lid=%s", l.hibernationEventId, l.replId, l.id)
	}
}
