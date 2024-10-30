package conflictlog

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
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

	// rwMtx is the logger level RW lock
	rwMtx sync.RWMutex

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
}

type logRequest struct {
	conflictRec *ConflictRecord
	//ackCh is the channel on which the result of the logging is reported back
	ackCh chan error
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
func newLoggerImpl(logger *log.CommonLogger, replId string, utils utils.UtilsIface, throttlerSvc throttlerSvc.ThroughputThrottlerSvc, connPool iopool.ConnPool, opts ...LoggerOpt) (l *LoggerImpl, err error) {
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
	logger.Infof("creating new conflict logger id=%d replId=%s loggerOptions=%s throttlerAttempts=%d ", loggerId, replId, options.String(), throttlerAttempts)

	l = &LoggerImpl{
		logger:            logger,
		id:                loggerId,
		utils:             utils,
		replId:            replId,
		rulesLock:         sync.RWMutex{},
		connPool:          connPool,
		opts:              options,
		rwMtx:             sync.RWMutex{},
		throttlerSvc:      throttlerSvc,
		throttlerAttempts: throttlerAttempts,
		logReqCh:          make(chan logRequest, options.logQueueCap),
		finch:             make(chan bool, 1),
		shutdownWorkerCh:  make(chan bool, LoggerShutdownChCap),
		wg:                sync.WaitGroup{},
		AbstractComponent: component.NewAbstractComponentWithLogger(loggerId, logger),
	}

	logger.Infof("spawning conflict logger workers replId=%s count=%d", l.replId, l.opts.workerCount)
	for i := 0; i < l.opts.workerCount; i++ {
		l.startWorker()
	}

	return
}

func (l *LoggerImpl) Id() string {
	return l.id
}

func (l *LoggerImpl) log(c *ConflictRecord) (ackCh chan error, err error) {
	ackCh = make(chan error, 1)
	req := logRequest{
		conflictRec: c,
		ackCh:       ackCh,
	}

	l.rwMtx.RLock()
	defer l.rwMtx.RUnlock()

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
	l.logger.Debugf("logging conflict record replId=%s scope=%s collection=%s", l.replId, c.Scope(), c.Collection())

	if l.isClosed() {
		err = baseclog.ErrLoggerClosed
		return
	}

	record, ok := c.(*ConflictRecord)
	if !ok {
		err = fmt.Errorf("wrong type of conflict record %T", c)
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

func (l *LoggerImpl) Close() (err error) {
	// check for closed channel as multiple threads could have attempted it
	if l.isClosed() {
		return nil
	}

	l.rwMtx.Lock()

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

	defer l.rwMtx.Unlock()

	l.wg.Wait()
	l.logger.Infof("closing of conflict logger done id=%s replId=%s", l.id, l.replId)

	return
}

func (l *LoggerImpl) startWorker() {
	l.wg.Add(1)
	go l.worker()
}

func (l *LoggerImpl) UpdateWorkerCount(newCount int) (err error) {
	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	if l.isClosed() {
		return
	}

	l.logger.Debugf("going to change conflict logger worker count replId=%s old=%d new=%d id=%s", l.replId, l.opts.workerCount, newCount, l.id)

	if newCount <= 0 || newCount == l.opts.workerCount {
		err = baseclog.ErrNoChange
		return
	}

	l.logger.Infof("changing conflict logger worker count replId=%s old=%d new=%d id=%s", l.replId, l.opts.workerCount, newCount, l.id)
	defer l.logger.Infof("done changing conflict logger worker count replId=%s old=%d new=%d id=%s", l.replId, l.opts.workerCount, newCount, l.id)

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
	l.rulesLock.Lock()
	defer l.rulesLock.Unlock()

	if l.isClosed() {
		return
	}

	l.logger.Debugf("going to change conflict logger worker count replId=%s old=%d new=%d", l.replId, l.opts.rules, r)
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

	l.logger.Infof("conflict logger rules=%v updated id=%s replId=%s", r, l.id, l.replId)
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

			err := l.processReq(req)
			req.ackCh <- err
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
	obj, err := l.connPool.Get(bucketName, l.opts.poolGetTimeout)
	if err != nil {
		return
	}

	conn, ok := obj.(Connection)
	if !ok {
		err = fmt.Errorf("pool object is of invalid type got=%T", obj)
		return
	}

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
	// Write source document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err := conn.SetMeta(req.conflictRec.Source.Id, req.conflictRec.Source.Body, req.conflictRec.Source.Datatype, target)
		return err
	})
	if err != nil {
		// errors are not logged here as it will spam it when we get errors like TMPFAIL
		// we let the callers deal with it
		return err
	}
	go l.RaiseEvent(common.NewEvent(common.ConflictDocsWritten, SourceDoc, l, nil, nil))

	// Write target document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = conn.SetMeta(req.conflictRec.Target.Id, req.conflictRec.Target.Body, req.conflictRec.Target.Datatype, target)
		return err
	})
	if err != nil {
		return err
	}
	go l.RaiseEvent(common.NewEvent(common.ConflictDocsWritten, TargetDoc, l, nil, nil))

	// Write conflict record.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = conn.SetMeta(req.conflictRec.Id, req.conflictRec.Body, req.conflictRec.Datatype, target)
		return err
	})
	if err != nil {
		return err
	}
	go l.RaiseEvent(common.NewEvent(common.ConflictDocsWritten, CRD, l, nil, nil))

	return
}

// writeDocRetry arranges for a connection from the pool which the supplied function can use. The function
// wraps the supplied function to check for network errors and appropriately releases the connection back
// to the pool
func (l *LoggerImpl) writeDocRetry(bucketName string, fn func(conn Connection) error) (err error) {
	var conn Connection

	for i := 0; i < l.opts.networkRetryCount; i++ {
		conn, err = l.getFromPool(bucketName)
		if err != nil {
			// This is to account for nw errors while connecting
			if !l.utils.IsSeriousNetError(err) {
				return
			}
			time.Sleep(l.opts.networkRetryInterval)
			continue
		}

		l.logger.Debugf("got connection from pool, id=%d rid=%s lid=%s", conn.Id(), l.replId, l.id)

		// The call to connPool.Put is not advised to be done in a defer here.
		// This is because calling defer in a loop accumulates multiple defers
		// across the iterations and calls all of them when function exits which
		// will be error prone in this case
		err = fn(conn)
		if err == nil {
			l.logger.Debugf("releasing connection to pool after success, id=%d damaged=%v rid=%s lid=%s", conn.Id(), false, l.replId, l.id)
			l.connPool.Put(bucketName, conn, false)
			break
		}

		l.logger.Errorf("error in writing doc to conflict bucket err=%v, id=%d rid=%s lid=%s", err, conn.Id(), l.replId, l.id)
		nwError := l.utils.IsSeriousNetError(err)
		l.logger.Debugf("releasing connection to pool after failure, id=%d damaged=%v rid=%s lid=%s", conn.Id(), nwError, l.replId, l.id)
		l.connPool.Put(bucketName, conn, nwError)
		if !nwError {
			break
		}
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

	target, err := l.getTarget(req.conflictRec)
	if err != nil {
		return err
	}

	if target.IsBlacklistTarget() {
		// this conflict shouldn't be logged.
		return nil
	}

	err = req.conflictRec.PopulateData(l.replId)
	if err != nil {
		return err
	}

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
	if l.isClosed() {
		return baseclog.ErrLoggerClosed
	}

	// acquire the lock so that no more writes to the channel happens.
	// read from channel by the workers can still happen, so we shut down the workers temporarily.
	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	l.logger.Debugf("going to change conflict logger queue capacity replId=%s old=%d new=%d id=%s", l.replId, l.opts.workerCount, newCap, l.id)

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

	l.logger.Infof("changing conflict logger queue capacity replId=%s old=%d new=%d id=%s", l.replId, l.opts.logQueueCap, newCap, l.id)
	defer l.logger.Infof("done changing conflict logger queue capacity replId=%s cap=%d id=%s", l.replId, l.opts.logQueueCap, l.id)

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

	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	l.opts.networkRetryCount = cnt
}

func (l *LoggerImpl) UpdateNWRetryInterval(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	l.opts.networkRetryInterval = t
}

func (l *LoggerImpl) UpdateSetMetaTimeout(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	l.opts.setMetaTimeout = t
}

func (l *LoggerImpl) UpdateGetFromPoolTimeout(t time.Duration) {
	if l.isClosed() {
		return
	}

	l.rwMtx.Lock()
	defer l.rwMtx.Unlock()

	l.opts.poolGetTimeout = t
}
