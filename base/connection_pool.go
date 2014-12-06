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
	"net/url"
	//	"log"
	"github.com/couchbase/goxdcr/log"
	mcc "github.com/couchbase/gomemcached/client"
	"sync"
)

type ConnPool struct {
	clients  chan *mcc.Client
	hostName string
	userName string
	password string
	maxConn  int
	logger   *log.CommonLogger
}

type connPoolMgr struct {
	conn_pools_map map[string]*ConnPool
	token          sync.Mutex
	once           sync.Once
	logger         *log.CommonLogger
}

var _connPoolMgr connPoolMgr

/******************************************************************
 *
 *  Connection management
 *  These set of functions will not lock.
 *  With the exception of release(), this set of functions
 *  should only be called by a single thread at a gvien time.
 *
 ******************************************************************/

func parseUsernamePassword(u string) (username string, password string, err error) {
	username = ""
	password = ""

	var url *url.URL
	url, err = url.Parse(u)
	if err != nil {
		return "", "", err
	}

	user := url.User
	if user != nil {
		username = user.Username()
		var isSet bool
		password, isSet = user.Password()
		if !isSet {
			password = ""
		}
	}

	return username, password, nil
}

func (p *ConnPool) IsClosed() bool {
	return p.clients == nil
}

func (p *ConnPool) Get() (*mcc.Client, error) {
	p.logger.Debugf("There are %d connections in the pool\n", len(p.clients))
	select {
	case client, ok := <-p.clients:
		if ok {
			return client, nil
		}
	default:
		//no more connection, create more
		mcClient, err := NewConn(p.hostName, p.userName, p.password)
		return mcClient, err
	}	
		
	return nil, errors.New("connection pool is closed")
}

//
// Release connection back to the pool
//
func (p *ConnPool) Release(client *mcc.Client) {
	// This would panic if p.clients is closed.  This
	// is intentional.
	select {
	case p.clients <- client:
		return
	default:
		//the pool reaches its capacity, drop the client on the floor
		return
	}
}

//
// Release all connections in the connection pool.
//
func (p *ConnPool) ReleaseConnections() {

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

func (connPoolMgr *connPoolMgr) GetOrCreatePool(poolNameToCreate string, hostname string, username string, password string, connsize int) (*ConnPool, error) {
	pool := connPoolMgr.GetPool(poolNameToCreate)
	var err error
	size := connsize
	if size == 0 {
		size = DefaultConnectionSize
	}
	if pool == nil {
		pool, err = connPoolMgr.CreatePool(poolNameToCreate, hostname, username, password, size)
	}
	return pool, err
}

func (connPoolMgr *connPoolMgr) GetPool(poolName string) *ConnPool {
	connPoolMgr.token.Lock()
	defer connPoolMgr.token.Unlock()
	pool := connPoolMgr.conn_pools_map[poolName]

	return pool
}

func (connPoolMgr *connPoolMgr) CreatePool(poolName string, hostName string, username string, password string, connectionSize int) (p *ConnPool, err error) {
	connPoolMgr.logger.Infof("Create Pool - poolName=%v,", poolName)
	connPoolMgr.logger.Infof("connectionSize=%d", connectionSize)
	p = &ConnPool{clients: make(chan *mcc.Client, connectionSize),
		hostName: hostName,
		userName: username,
		password: password,
		logger: log.NewLogger("ConnPool", connPoolMgr.logger.LoggerContext())}

	// make sure we release resource upon unexpected error
	defer func() {
		if r := recover(); r != nil {
			p.ReleaseConnections()
			panic(r)
		}
	}()

	//	 initialize the connection pool
	for i := 0; i < connectionSize; i++ {
		mcClient, err := NewConn(hostName, username, password)
		if err == nil {
			connPoolMgr.logger.Debug("A client connection is established")
			p.clients <- mcClient
		} else {
			connPoolMgr.logger.Debugf("error establishing connection with hostname=%s, username=%s, password=%s - %s", hostName, username, password, err)
		}

	}

	connPoolMgr.token.Lock()
	connPoolMgr.conn_pools_map[poolName] = p
	connPoolMgr.token.Unlock()

	connPoolMgr.logger.Infof("Connection pool %s is created with %d clients\n", poolName, len(p.clients))
	return p, nil
}

//
// This function creates a single connection to the vbucket master node.
//
func NewConn(hostName string, username string, password string) (conn *mcc.Client, err error) {
	// connect to host
	conn, err = mcc.Connect("tcp", hostName)
	if err != nil {
		return nil, err
	}

	// authentic using user/pass
	if len(username) != 0 && username != "default" {
		_connPoolMgr.logger.Debug("Authenticate...")
		_, err = conn.Auth(username, password)
		if err != nil {
			_connPoolMgr.logger.Errorf("err=%v\n", err)
			conn.Close()
			return nil, err
		}
	}

	return conn, nil
}

//return the singleton ConnPoolMgr
func ConnPoolMgr() *connPoolMgr {
	_connPoolMgr.once.Do(func() {
		_connPoolMgr.conn_pools_map = make(map[string]*ConnPool)
		_connPoolMgr.logger = log.NewLogger("ConnPoolMgr", log.DefaultLoggerContext)

	})
	return &_connPoolMgr
}

func SetLoggerContexForConnPoolMgr(logger_context *log.LoggerContext) *connPoolMgr {
	connPoolMgr := ConnPoolMgr()
	connPoolMgr.logger = log.NewLogger("ConnPoolMgr", logger_context)
	return connPoolMgr
}

func (connPoolMgr *connPoolMgr) Close() {
	connPoolMgr.token.Lock()
	defer connPoolMgr.token.Unlock()

	for key, pool := range connPoolMgr.conn_pools_map {
		connPoolMgr.logger.Infof("close pool %s", key)
		pool.ReleaseConnections()
	}
}

