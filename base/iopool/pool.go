package iopool

import (
	"container/list"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
)

var _ ConnPool = (*ConnPoolImpl)(nil)

const (
	// ConnPool consts
	// DefaultPoolGCInterval is the GC frequency for connection pool
	DefaultPoolGCInterval = 60 * time.Second
	// DefaultPoolReapInterval is the last used threshold for reaping unused connections
	DefaultPoolReapInterval = 120 * time.Second
)

var (
	ErrClosedConnPool     error = errors.New("use of closed connection pool")
	ErrConnPoolGetTimeout error = errors.New("conflict logging pool get timedout")
)

// ConnPool defines the behaviour of a connection pool for objects/resources
// which implements io.Closer interface. The pool should reap the unused resources by
// calling io.Closer.Close() governed by the GC & reap interval
//
// Sample retry loop usage by user using the pool
//
//	 for i:=0; i<5; i++ {
//			conn, err = pool.Get(bucket)
//			... use the conn
//			err = conn.Write()
//			pool.Put(bucket, conn, err)
//			if err != nil && IsErrorTransient(err) {
//				continue
//			}
//			break
//	}
type ConnPool interface {
	// Get returns an object from the pool. If there is none then it creates
	// one by calling newConnFn() and returns it. It is guaranteed that either
	// an error or a non-nil connection object will be returned
	Get(bucketName string, timeout time.Duration) (conn io.Closer, err error)

	// Put releases the connection back to the pool for reuse. It is caller's job
	// to ensure that right bucket name is passed here. The damaged == true tells the pool
	// that the conn is damaged. The pool appropriately should manage its internal state
	// in response this (e.g. active Connection Count)
	Put(bucketName string, conn io.Closer, damaged bool)

	// UpdateGCInterval updates the new GC frequency
	// Duration <= 0 has no effect and its ignored
	UpdateGCInterval(d time.Duration)

	// UpdateReapInterval updates the reap interval for unused connections
	// Duration <= 0 has no effect and its ignored
	UpdateReapInterval(d time.Duration)

	// SetLimit sets the upper limit of number of active connections in the pool and
	// created but not released back to the pool. On reaching the max connections the Get()
	// will block. Value <= has no effect and its ignored
	SetLimit(n int)

	// Count returns the number of objects/connections being held by the pool
	Count() int

	// Close reaps all the connections which are in the pool. It does not deal with connections
	// which are not yet released back to the pool. The caller should ensure this.
	Close() error
}

// ConnPoolImpl is a connection pool for any object which implements io.Closer interface
// This is generic enough to support pooling of wide array of resources like files,
// sockets, etc
// The pool has connections per bucket but the user can use empty string as bucket if
// there is no notion of a bucket.
type ConnPoolImpl struct {
	logger *log.CommonLogger

	// buckets is the map of buckets to its connection list
	buckets map[string]*connList

	// mu is a pool level lock
	mu sync.Mutex

	// function to create new pool objects. This is called when
	// there are no objects to return
	newConnFn func(bucketName string) (io.Closer, error)

	// gcTicker controls the periodicity with which reaping of idle connections is attempted
	gcTicker *time.Ticker

	// reapInterval determines how last used threshold beyond which the
	// connection should be reaped
	reapInterval time.Duration

	// Limiter is a semaphore to limit the connection count
	limiter *base.Semaphore

	finch chan bool

	// closed when set to true indicates that pool is closed
	closed bool
}

// connList is the list of actual objects which are pooled
type connList struct {
	// lastUsed is the timestamp when the connection list was used (either pop or push)
	lastUsed time.Time

	// mu is list level lock
	mu sync.Mutex

	//list is the actual linked list to hold the pooled objects
	list *list.List
}

func (l *connList) len() int {
	l.mu.Lock()
	n := l.list.Len()
	l.mu.Unlock()
	return n
}

func (l *connList) pop() io.Closer {
	l.mu.Lock()
	defer l.mu.Unlock()

	ele := l.list.Front()
	if ele == nil {
		return nil
	}
	w, _ := ele.Value.(io.Closer)
	l.list.Remove(ele)

	l.lastUsed = time.Now()

	return w
}

func (l *connList) push(w io.Closer) {
	l.mu.Lock()
	l.list.PushBack(w)
	l.lastUsed = time.Now()
	l.mu.Unlock()
}

func (l *connList) closeAll() {
	l.mu.Lock()
	defer l.mu.Unlock()

	e := l.list.Front()
	for e != nil {
		closer := e.Value.(io.Closer)
		_ = closer.Close()
		l.list.Remove(e)
		e = l.list.Front()
	}
}

// NewConnPool creates a new connection pool
func NewConnPool(logger *log.CommonLogger, limit int, newConnFn func(bucketName string) (io.Closer, error)) *ConnPoolImpl {
	p := &ConnPoolImpl{
		logger:       logger,
		buckets:      map[string]*connList{},
		mu:           sync.Mutex{},
		newConnFn:    newConnFn,
		gcTicker:     time.NewTicker(DefaultPoolGCInterval),
		reapInterval: DefaultPoolReapInterval,
		finch:        make(chan bool, 1),
		limiter:      base.NewSemaphore(limit),
	}

	go p.gc()

	return p
}

// getOrCreateListNoLock gets the connection list for the bucket. If the bucket does not
// exist then it creates one before returning. The function assumes that caller will acquire
// the lock before calling.
func (pool *ConnPoolImpl) getOrCreateListNoLock(bucketName string) *connList {
	clist, ok := pool.buckets[bucketName]
	if !ok {
		clist = &connList{
			mu:       sync.Mutex{},
			list:     list.New(),
			lastUsed: time.Now(),
		}
		pool.buckets[bucketName] = clist
	}

	return clist
}

func (pool *ConnPoolImpl) get(bucketName string) (conn io.Closer) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	clist := pool.getOrCreateListNoLock(bucketName)

	conn = clist.pop()
	return
}

// UpdateGCInterval updates the new GC frequency
func (pool *ConnPoolImpl) UpdateGCInterval(d time.Duration) {
	// Negative duration will cause the timer to panic.
	if d <= 0 {
		return
	}

	pool.mu.Lock()
	pool.gcTicker = time.NewTicker(d)
	pool.mu.Unlock()
}

func (pool *ConnPoolImpl) getGCTicker() (t *time.Ticker) {
	pool.mu.Lock()
	t = pool.gcTicker
	pool.mu.Unlock()

	return
}

// UpdateReapInterval updates the reap interval for unused connections
func (pool *ConnPoolImpl) UpdateReapInterval(d time.Duration) {
	// Negative duration does not make any sense
	if d <= 0 {
		return
	}

	pool.mu.Lock()
	pool.reapInterval = d
	pool.mu.Unlock()
}

// Get returns an object from the pool. If there is none then it creates
// one by calling newConnFn() and returns it. It is guaranteed that either
// an error or a non-nil connection object will be returned
func (pool *ConnPoolImpl) Get(bucketName string, timeout time.Duration) (conn io.Closer, err error) {
	if pool.closed {
		err = ErrClosedConnPool
		return
	}

	if !pool.limiter.AcquireWithTimeout(timeout) {
		err = ErrConnPoolGetTimeout
		return
	}

	conn = pool.get(bucketName)
	if conn != nil {
		return
	}

	conn, err = pool.newConnFn(bucketName)
	if err != nil {
		pool.limiter.Release()
	}

	return
}

// Put releases the connection back to the pool for reuse. It is caller's job
// to ensure that right bucket name is passed here.
func (pool *ConnPoolImpl) Put(bucketName string, conn io.Closer, damaged bool) {
	defer pool.limiter.Release()

	if damaged {
		// We do nothing here except to close the connection & release back to the limiter (see defer)
		// We have to release back to make way for a new connection to replace the damaged one.
		conn.Close()
		return
	}

	if pool.closed {
		conn.Close()
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	l := pool.getOrCreateListNoLock(bucketName)
	l.push(conn)
}

// Count returns the number of objects being held
func (pool *ConnPoolImpl) Count() (n int) {

	pool.mu.Lock()
	for _, connList := range pool.buckets {
		n += connList.len()
	}
	pool.mu.Unlock()

	return
}

// Close shutsdown the GC worker and initiates a final gc with force=true
func (pool *ConnPoolImpl) Close() error {
	if pool.closed {
		return nil
	}

	pool.closed = true
	pool.logger.Infof("closing all connections of conflict connection pool")
	close(pool.finch)

	// use force=true to ensure all remaining connections are reaped.
	pool.gcOnce(true)
	return nil
}

// SetLimit sets the upper limit of number of active connections in the pool and
// created but not released back to the pool. On reaching the max connections the Get()
// will block
func (pool *ConnPoolImpl) SetLimit(n int) {
	if n <= 0 {
		return
	}

	// There is no need for a pool level lock as SetLimit is thread safe
	pool.limiter.SetLimit(n)
}

// reapConnList collects all the connection lists which have not been used for the reapInterval time
// This does not close the connection itself
func (pool *ConnPoolImpl) reapConnList(force bool) []*connList {
	connListList := []*connList{}
	reapedBuckets := []string{}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	now := time.Now()
	// Collect all expired buckets and its lists
	for bucketName, connList := range pool.buckets {
		elapsed := now.Sub(connList.lastUsed)
		if force || elapsed >= pool.reapInterval {
			reapedBuckets = append(reapedBuckets, bucketName)
			connListList = append(connListList, connList)
		}
	}

	// Delete expired buckets
	// It is possible that a connection for a deleted bucket will be requested
	// after this. This is fine since it will treated like a new bucket being requested
	// for the first time
	for _, bucketName := range reapedBuckets {
		pool.logger.Debugf("reaping connections for bucket=%s", bucketName)
		delete(pool.buckets, bucketName)
	}

	return connListList
}

// gcOnce runs one single iteration of reaping the connections
func (pool *ConnPoolImpl) gcOnce(force bool) {
	connListList := pool.reapConnList(force)

	pool.logger.Debugf("conflict connection pool reaping lists. force=%v, count=%d", force, len(connListList))

	// Note: the closing of the connections happen outside the pool lock.
	// From this point, a parallel request to create a connection is safe
	// and it will land in a new connList.
	for _, connList := range connListList {
		connList.closeAll()
	}
}

// gc reaps the unused connections by checking at a regular interval
// This is how it works
//  1. Every time connList is used (pop or push) it updates its lastUsed = time.Now()
//  2. Each iteration of GC checks all buckets and its list to see which ones have exceeded the reapInterval
//  3. It collects such lists and for each of the lists it closes all the connections
//
// It is certainly possible to get a access pattern such that a connList is being closed and
// there is a parallel request connection for the same bucket. This request can be catered to in parallel safely
// as it will land in a completely new list. This cannot be avoided and it is expected that
// such cases will be much fewer.
func (pool *ConnPoolImpl) gc() {
	for {
		// we copy the ticker so that it can be modified in parallel
		gcTicker := pool.getGCTicker()

		select {
		case <-pool.finch:
			pool.logger.Info("conflict log conn pool gc worker exiting")
			return
		case <-gcTicker.C:
			pool.logger.Debug("conflict log conn pool gc run starts")
			pool.gcOnce(false)
		}
	}
}
