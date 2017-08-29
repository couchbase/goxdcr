// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"net"
	"sync"
)

type TCPConnPool struct {
	clients  chan *net.TCPConn
	hostName string
	maxConn  int
	logger   *log.CommonLogger
}

type tcpConnPoolMgr struct {
	conn_pools_map map[string]*TCPConnPool
	token          sync.RWMutex
	once           sync.Once
	logger         *log.CommonLogger
}

var _tcpConnPoolMgr tcpConnPoolMgr

var NetTCP = "tcp"

/******************************************************************
 *
 *  Connection management
 *  These set of functions will not lock.
 *  With the exception of release(), this set of functions
 *  should only be called by a single thread at a gvien time.
 *
 ******************************************************************/

func (p *TCPConnPool) IsClosed() bool {
	return p.clients == nil
}

func (p *TCPConnPool) GetNew() (*net.TCPConn, error) {
	return NewTCPConn(p.hostName)
}

func (p *TCPConnPool) Get() (*net.TCPConn, error) {
	p.logger.Debugf("There are %d connections in the pool\n", len(p.clients))
	select {
	case client, ok := <-p.clients:
		if ok {
			return client, nil
		}
	default:
		//no more connection, create more
		client, err := NewTCPConn(p.hostName)
		return client, err
	}

	return nil, errors.New("connection pool is closed")
}

//
// Release connection back to the pool
//
func (p *TCPConnPool) Release(client *net.TCPConn) {
	// This would panic if p.clients is closed.  This
	// is intentional.
	select {
	case p.clients <- client:
		return
	default:
		//the pool reaches its capacity, drop the client on the floor
		client.Close()
		return
	}
}

//
// Release all connections in the connection pool.
//
func (p *TCPConnPool) ReleaseConnections() {

	if p.clients == nil {
		return
	}

	done := false
	for !done {
		select {
		case client, ok := <-p.clients:
			{
				if ok {
					if client != nil {
						client.Close()
					}
				} else {
					done = true
				}
			}
		default:
			{
				// if there is no more client in the channel
				done = true
			}
		}
	}

	close(p.clients)
	p.clients = nil
}

func (tcpConnPoolMgr *tcpConnPoolMgr) GetOrCreatePool(poolNameToCreate string, hostname string, connsize int) (*TCPConnPool, error) {
	pool := tcpConnPoolMgr.GetPool(poolNameToCreate)
	var err error
	size := connsize
	if size == 0 {
		size = DefaultCAPIConnectionSize
	}
	if pool == nil {
		pool, err = tcpConnPoolMgr.CreatePool(poolNameToCreate, hostname, size)
	}
	return pool, err
}

func (tcpConnPoolMgr *tcpConnPoolMgr) GetPool(poolName string) *TCPConnPool {
	tcpConnPoolMgr.token.RLock()
	defer tcpConnPoolMgr.token.RUnlock()
	pool := tcpConnPoolMgr.conn_pools_map[poolName]

	return pool
}

func (tcpConnPoolMgr *tcpConnPoolMgr) CreatePool(poolName string, hostName string, connectionSize int) (p *TCPConnPool, err error) {
	tcpConnPoolMgr.logger.Infof("Create TCP Pool - poolName=%v,", poolName)
	tcpConnPoolMgr.logger.Infof("connectionSize=%d", connectionSize)
	p = &TCPConnPool{clients: make(chan *net.TCPConn, connectionSize),
		hostName: hostName,
		logger:   log.NewLogger("TCPConnPool", tcpConnPoolMgr.logger.LoggerContext())}

	tcpConnPoolMgr.setPool(poolName, p)

	tcpConnPoolMgr.logger.Infof("Connection pool %s has been created\n", poolName)
	return p, nil
}

//
// This function creates a single connection to the vbucket master node.
//
func NewTCPConn(hostName string) (conn *net.TCPConn, err error) {
	con, err := DialTCPWithTimeout(NetTCP, hostName)
	if err != nil {
		return nil, err
	}
	if con == nil {
		return nil, fmt.Errorf("Failed to set up connection to %v", hostName)
	}
	conn, ok := con.(*net.TCPConn)
	if !ok {
		con.Close()
		return nil, fmt.Errorf("The connection to %v returned is not TCP type", hostName)
	}
	return conn, nil
}

//return the singleton TCPConnPoolMgr
func TCPConnPoolMgr() *tcpConnPoolMgr {
	_tcpConnPoolMgr.once.Do(func() {
		_tcpConnPoolMgr.conn_pools_map = make(map[string]*TCPConnPool)
		_tcpConnPoolMgr.logger = log.NewLogger("TCPConnPoolMgr", log.DefaultLoggerContext)

	})
	return &_tcpConnPoolMgr
}

func SetLoggerContexForTCPConnPoolMgr(logger_context *log.LoggerContext) *tcpConnPoolMgr {
	tcpConnPoolMgr := TCPConnPoolMgr()
	tcpConnPoolMgr.logger = log.NewLogger("TCPConnPoolMgr", logger_context)
	return tcpConnPoolMgr
}

func (tcpConnPoolMgr *tcpConnPoolMgr) Close() {
	tcpConnPoolMgr.token.Lock()
	defer tcpConnPoolMgr.token.Unlock()

	for key, pool := range tcpConnPoolMgr.conn_pools_map {
		tcpConnPoolMgr.logger.Infof("Closing pool %s", key)
		pool.ReleaseConnections()
	}

	tcpConnPoolMgr.conn_pools_map = make(map[string]*TCPConnPool)
}

func (tcpConnPoolMgr *tcpConnPoolMgr) setPool(poolName string, p *TCPConnPool) {
	tcpConnPoolMgr.token.Lock()
	tcpConnPoolMgr.conn_pools_map[poolName] = p
	tcpConnPoolMgr.token.Unlock()
}
