package cng

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type wrapperConn struct {
	conn       *base.CngConn
	rw         sync.RWMutex
	generation atomic.Int64
}

func (w *wrapperConn) Close() error {
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

// ConnPool manages a pool of gRPC connections to CNG
// There are already different connection pools in goxdcr, e.g., for memcached
// In existing pools, a connection is checked out, used, and then returned.
// So a connection is to be used by one user at a time. In contrast, gRPC connections
// are designed to be shared by multiple go routines. So the connection pool here
// is a thin wrapper around a list of gRPC connections, and each time a connection
// is needed, one from the list is returned in a round-robin fashion.
type ConnPool struct {
	cfg     *PoolConfig
	clients []*wrapperConn
	logger  *log.CommonLogger

	// Thread-safe fields
	poolLock    sync.RWMutex
	counter     uint64 // atomic counter for round-robin selection
	initialized bool
	shutdown    chan struct{}
	isClosed    atomic.Bool
}

type PoolConfig struct {
	ConnCount     int
	ConnFn        func() (*base.CngConn, error)
	RetryInterval int // in milliseconds
}

func (c *PoolConfig) Validate() error {
	if c.ConnCount <= 0 {
		return fmt.Errorf("connection count must be > 0")
	}
	if c.ConnFn == nil {
		return fmt.Errorf("connection creation function cannot be nil")
	}
	if c.RetryInterval <= 0 {
		return fmt.Errorf("retry interval must be > 0")
	}
	return nil
}

// NewConnPool creates a new connection pool with the specified connection count and creation function
func NewConnPool(logger *log.CommonLogger, cfg *PoolConfig) (pool *ConnPool, err error) {
	if err = cfg.Validate(); err != nil {
		return nil, err
	}

	clients := make([]*wrapperConn, cfg.ConnCount)
	for i := 0; i < cfg.ConnCount; i++ {
		clients[i] = &wrapperConn{}
	}

	pool = &ConnPool{
		cfg:      cfg,
		logger:   logger,
		clients:  clients,
		shutdown: make(chan struct{}),
		isClosed: atomic.Bool{},
	}

	err = pool.init()
	if err != nil {
		pool.Close()
		pool = nil
	}

	return
}

// Initialize creates all connections in the pool
func (p *ConnPool) init() error {
	p.poolLock.Lock()
	defer p.poolLock.Unlock()

	p.logger.Infof("initializing CNG connection pool, conntCount=%d", p.cfg.ConnCount)
	for i := 0; i < p.cfg.ConnCount; i++ {
		err := p.connect(i)
		if err != nil {
			return fmt.Errorf("failed to create connection %d: %w", i, err)
		}
	}

	return nil
}

// connect recreates a connection at the specified index in a thread-safe manner
// This method assumes the pool is already initialized
func (p *ConnPool) connect(index int) error {
	if index < 0 || index >= len(p.clients) {
		return fmt.Errorf("connection index %d out of range", index)
	}

	wrapper := p.clients[index]

	gen := wrapper.generation.Load()
	// Use the connection-specific mutex to ensure only one goroutine recreates this connection
	p.clients[index].rw.Lock()
	defer p.clients[index].rw.Unlock()
	if !wrapper.generation.CompareAndSwap(gen, gen+1) {
		// Another goroutine has already recreated the connection
		return nil
	}

	wrapper.Close()

	p.logger.Infof("creating connection at index=%d, generation=%d", index, gen)
	// Create new connection
	newConn, err := p.cfg.ConnFn()
	if err != nil {
		return fmt.Errorf("failed to recreate connection at index %d: %w", index, err)
	}

	wrapper.conn = newConn

	return nil
}

func (p *ConnPool) callFn(w *wrapperConn, fn func(client XDCRClient) error) error {
	w.rw.RLock()
	defer w.rw.RUnlock()
	if w.conn == nil {
		return fmt.Errorf("connection is nil")
	}
	return fn(w.conn.Client())
}

func (p *ConnPool) Close() {
	if p == nil {
		return
	}

	p.isClosed.Store(true)
	close(p.shutdown)
	p.poolLock.Lock()
	defer p.poolLock.Unlock()
	p.closeAllConnUnsafe()
}

// closeAllConnUnsafe closes all connections in the pool without acquiring the pool lock
// This method should be called only when the pool lock is already held
func (p *ConnPool) closeAllConnUnsafe() (err error) {
	for _, w := range p.clients {
		w.rw.Lock()
		if w.conn != nil {
			w.conn.Close()
		}
		w.rw.Unlock()
	}
	return
}

// ChangeConnCount changes the number of connections in the pool
// If the new count is greater than the current count, new connections are created
// If the new count is less than the current count, excess connections are closed
// This method is thread-safe
func (p *ConnPool) ChangeConnCount(newCount int) (err error) {
	if newCount <= 0 {
		newCount = 1
	}

	p.poolLock.Lock()
	defer p.poolLock.Unlock()

	if newCount == len(p.clients) {
		// no change
		return nil
	}

	if newCount > len(p.clients) {
		// increase pool size
		for i := len(p.clients); i < newCount; i++ {
			wrapper := &wrapperConn{}
			p.clients = append(p.clients, wrapper)
			err = p.connect(i)
			if err != nil {
				return fmt.Errorf("failed to create connection %d: %v", i, err)
			}
		}
	} else {
		// decrease pool size
		for i := newCount; i < len(p.clients); i++ {
			wrapper := p.clients[i]
			wrapper.rw.Lock()
			if wrapper.conn != nil {
				wrapper.conn.Close()
			}
			wrapper.rw.Unlock()
		}
		p.clients = p.clients[:newCount]
	}

	p.cfg.ConnCount = newCount
	return
}

// WithConn executes the provided function with a connection from the pool using round-robin selection
// If the error returned by fn is a network error, the connection is recreated and fn is retried.
// The callback function fn should be thread-safe and idempotent since it may be called multiple times
// in case of network errors.
func (p *ConnPool) WithConn(fn func(client XDCRClient) error) error {
	p.poolLock.RLock()
	defer p.poolLock.RUnlock()

	// Use atomic counter for thread-safe round-robin selection
	index := atomic.AddUint64(&p.counter, 1) % uint64(len(p.clients))
	wrapper := p.clients[index]

	for {
		select {
		case <-p.shutdown:
			return fmt.Errorf("connection pool is shutting down or already closed")
		default:
			// First attempt
			err := p.callFn(wrapper, fn)
			if err == nil {
				return nil
			}

			// Check if it's a network error that requires connection recreation
			if !isNetworkError(err) {
				err = fmt.Errorf("non-network error, connIndex=%d, err: %v", index, err)
				return err
			}

			p.logger.Infof("network error, connIndex=%d, err: %v", index, err)
			time.Sleep(time.Duration(p.cfg.RetryInterval) * time.Millisecond) // brief pause before reconnecting
			// Attempt to recreate the connection
			err = p.connect(int(index))
			if err != nil {
				// If recreation fails, return the original error
				return fmt.Errorf("network error: %v", err)
			}
		}
	}
}

// isNetworkError checks if an error is a network-related error that requires connection recreation
// CNG TODO: this function is similar to the one in utils/utils.go. Consider refactoring to avoid code duplication
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common network errors (similar to utils/utils.go)
	errStr := err.Error()
	if err == syscall.EPIPE ||
		err == io.EOF ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") {
		return true
	}

	// Check for net.OpError
	if netError, ok := err.(*net.OpError); ok {
		return !netError.Temporary() && !netError.Timeout()
	}

	// Check for gRPC status errors that indicate connection issues
	if grpcStatus, ok := status.FromError(err); ok {
		switch grpcStatus.Code() {
		case codes.Unavailable, codes.DeadlineExceeded:
			return true
		}
	}

	return false
}
