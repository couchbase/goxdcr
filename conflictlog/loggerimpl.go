package conflictlog

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	"github.com/couchbase/goxdcr/v8/utils"
)

var _ Logger = (*loggerImpl)(nil)

// loggerImpl implements the Logger interface
type loggerImpl struct {
	logger *log.CommonLogger

	// id uniquely identifies a logger instance
	id int64

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

	// mu is the logger level lock
	mu sync.Mutex

	// throttlerSvc is the IOPS throttler
	throttlerSvc throttlerSvc.ThroughputThrottlerSvc

	// logReqCh is the work queue for all logging requests
	logReqCh chan logRequest

	// finch is intended to signal closure of the logger
	finch chan bool

	// shutdownWorkerCh is intended to shut a subset of workers
	// This is used when new worker count is lesser than the current one
	shutdownWorkerCh chan bool

	// wg is used to wait for outstanding log requests to be finished
	wg sync.WaitGroup
}

type logRequest struct {
	conflictRec *ConflictRecord
	//ackCh is the channel on which the result of the logging is reported back
	ackCh chan error
}

// newLoggerImpl creates a new logger impl instance.
// throttlerSvc is allowed to be nil, which means no throttling
func newLoggerImpl(logger *log.CommonLogger, replId string, utils utils.UtilsIface, throttlerSvc throttlerSvc.ThroughputThrottlerSvc, connPool iopool.ConnPool, opts ...LoggerOpt) (l *loggerImpl, err error) {
	// set the defaults
	options := LoggerOptions{
		rules:                nil,
		logQueueCap:          DefaultLogCapacity,
		workerCount:          DefaultLoggerWorkerCount,
		mapper:               NewConflictMapper(logger),
		networkRetryCount:    DefaultNetworkRetryCount,
		networkRetryInterval: DefaultNetworkRetryInterval,
		poolGetTimeout:       DefaultPoolGetTimeout,
		setMetaTimeout:       DefaultSetMetaTimeout,
	}

	// override the defaults
	for _, opt := range opts {
		opt(&options)
	}

	loggerId := newLoggerId()
	logger.Infof("creating new conflict logger id=%d replId=%s loggerOptions=%s", loggerId, replId, options.String())

	l = &loggerImpl{
		logger:           logger,
		id:               loggerId,
		utils:            utils,
		replId:           replId,
		rulesLock:        sync.RWMutex{},
		connPool:         connPool,
		opts:             options,
		mu:               sync.Mutex{},
		throttlerSvc:     throttlerSvc,
		logReqCh:         make(chan logRequest, options.logQueueCap),
		finch:            make(chan bool, 1),
		shutdownWorkerCh: make(chan bool, LoggerShutdownChCap),
		wg:               sync.WaitGroup{},
	}

	logger.Infof("spawning conflict logger workers replId=%s count=%d", l.replId, l.opts.workerCount)
	for i := 0; i < l.opts.workerCount; i++ {
		l.startWorker()
	}

	return
}

func (l *loggerImpl) Id() int64 {
	return l.id
}

func (l *loggerImpl) log(c *ConflictRecord) (ackCh chan error, err error) {
	ackCh = make(chan error, 1)
	req := logRequest{
		conflictRec: c,
		ackCh:       ackCh,
	}

	select {
	case <-l.finch:
		err = ErrLoggerClosed
	case l.logReqCh <- req:
	default:
		err = ErrQueueFull
	}

	return
}

func (l *loggerImpl) Log(c *ConflictRecord) (h base.ConflictLoggerHandle, err error) {
	l.logger.Debugf("logging conflict record replId=%s sourceKey=%s", l.replId, c.Source.Id)

	if l.isClosed() {
		err = ErrLoggerClosed
		return
	}

	ackCh, err := l.log(c)
	if err != nil {
		return
	}

	h = logReqHandle{
		ackCh: ackCh,
	}

	return
}

func (l *loggerImpl) isClosed() bool {
	select {
	case <-l.finch:
		return true
	default:
		return false
	}
}

func (l *loggerImpl) Close() (err error) {
	// check for closed channel as multiple threads could have attempted it
	if l.isClosed() {
		return nil
	}

	l.mu.Lock()

	// check for closed channel as multiple threads could have attempted it
	if l.isClosed() {
		return nil
	}

	l.logger.Infof("closing conflict logger id=%d replId=%s", l.id, l.replId)

	// The close is implemented on the same principle that some restaurants have:
	// "last entry at 10:00 PM". When translated here it means, we will disallow
	// new requests but existing requests will be served and results acknowledged.
	// We also will wait for all of these to be completed.
	close(l.finch)

	// We close the work channel as well i.e. logReqCh. This is because we want to
	// handle inflight log requests (both in the channel and in workers)
	close(l.logReqCh)

	defer l.mu.Unlock()

	l.wg.Wait()
	l.logger.Infof("closing of conflict logger done id=%d replId=%s", l.id, l.replId)

	return
}

func (l *loggerImpl) startWorker() {
	l.wg.Add(1)
	go l.worker()
}

func (l *loggerImpl) UpdateWorkerCount(newCount int) {
	l.logger.Infof("changing conflict logger worker count replId=%s old=%d new=%d", l.replId, l.opts.workerCount, newCount)

	l.mu.Lock()
	defer l.mu.Unlock()

	if newCount <= 0 || newCount == l.opts.workerCount {
		return
	}

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
	l.logger.Infof("changing conflict logger worker count done replId=%s", l.replId)
}

// r should be non-nil
func (l *loggerImpl) UpdateRules(r *base.ConflictLogRules) (err error) {
	if r == nil {
		err = ErrEmptyRules
		return
	}

	l.logger.Infof("conflict logger got request to update rules id=%d replId=%s %v", l.id, l.replId, r)

	err = r.Validate()
	if err != nil {
		return
	}

	l.rulesLock.Lock()
	l.opts.rules = r
	l.rulesLock.Unlock()

	l.logger.Infof("conflict logger rules updated id=%d replId=%s", l.id, l.replId)
	return
}

func (l *loggerImpl) worker() {
	defer l.wg.Done()

	for {
		select {
		case <-l.shutdownWorkerCh:
			l.logger.Infof("shutting down conflict log worker replId=%s", l.replId)
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

func (l *loggerImpl) getTarget(rec *ConflictRecord) (t base.ConflictLogTarget, err error) {
	l.rulesLock.RLock()
	defer l.rulesLock.RUnlock()

	t, err = l.opts.mapper.Map(l.opts.rules, rec)
	if err != nil {
		return
	}

	return
}

func (l *loggerImpl) getFromPool(bucketName string) (conn Connection, err error) {
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

func (l *loggerImpl) throttle() {
	if l.throttlerSvc == nil {
		return
	}

	ok := l.throttlerSvc.CanSend(false)
	for !ok {
		l.throttlerSvc.Wait()
		ok = l.throttlerSvc.CanSend(false)
	}
}

// setMetaTimeout is a wrapper on Connection's SetMeta using the timeout configured with the logger
func (l *loggerImpl) setMetaTimeout(conn Connection, key string, body []byte, dataType uint8, target base.ConflictLogTarget) error {
	resultCh := make(chan error, 1)
	go func() {
		l.throttle()
		err := conn.SetMeta(key, body, dataType, target)
		resultCh <- err
	}()

	t := time.NewTimer(l.opts.setMetaTimeout)
	defer t.Stop()

	select {
	case err := <-resultCh:
		return err
	case <-t.C:
		return ErrSetMetaTimeout
	}
}

func (l *loggerImpl) writeDocs(req logRequest, target base.ConflictLogTarget) (err error) {

	// Write source document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err := l.setMetaTimeout(conn, req.conflictRec.Source.Id, req.conflictRec.Source.Body, req.conflictRec.Source.Datatype, target)
		return err
	})
	if err != nil {
		// errors are not logged here as it will spam it when we get errors like TMPFAIL
		// we let the callers deal with it
		return err
	}

	// Write target document.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = l.setMetaTimeout(conn, req.conflictRec.Target.Id, req.conflictRec.Target.Body, req.conflictRec.Target.Datatype, target)
		return err
	})
	if err != nil {
		return err
	}

	// Write conflict record.
	err = l.writeDocRetry(target.Bucket, func(conn Connection) error {
		err = l.setMetaTimeout(conn, req.conflictRec.Id, req.conflictRec.Body, req.conflictRec.Datatype, target)
		return err
	})
	if err != nil {
		return err
	}

	return
}

// writeDocRetry arranges for a connection from the pool which the supplied function can use. The function
// wraps the supplied function to check for network errors and appropriately releases the connection back
// to the pool
func (l *loggerImpl) writeDocRetry(bucketName string, fn func(conn Connection) error) (err error) {
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

		l.logger.Debugf("got connection from pool, id=%d rid=%s lid=%v", conn.Id(), l.replId, l.id)

		// The call to connPool.Put is not advised to be done in a defer here.
		// This is because calling defer in a loop accumulates multiple defers
		// across the iterations and calls all of them when function exits which
		// will be error prone in this case
		err = fn(conn)
		if err == nil {
			l.logger.Debugf("releasing connection to pool after success, id=%d damaged=%v rid=%s lid=%v", conn.Id(), false, l.replId, l.id)
			l.connPool.Put(bucketName, conn, false)
			break
		}

		l.logger.Errorf("error in writing doc to conflict bucket err=%v, id=%d rid=%s lid=%v", err, conn.Id(), l.replId, l.id)
		nwError := l.utils.IsSeriousNetError(err)
		l.logger.Debugf("releasing connection to pool after failure, id=%d damaged=%v rid=%s lid=%v", conn.Id(), nwError, l.replId, l.id)
		l.connPool.Put(bucketName, conn, nwError)
		if !nwError {
			break
		}
		time.Sleep(l.opts.networkRetryInterval)
	}

	return err
}

func (l *loggerImpl) processReq(req logRequest) error {
	var err error

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

	err = l.writeDocs(req, target)
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
		err = ErrLogWaitAborted
	case err = <-h.ackCh:
	}

	return
}
