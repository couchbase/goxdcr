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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/log"
	"io"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MAX_PAYLOAD_SIZE uint32 = 1000
)

type ConnPool interface {
	IsClosed() bool
	Get() (*mcc.Client, error)
	Release(client *mcc.Client)
	ReleaseConnections()
	Name() string
	IsFull() bool
}

type connPool struct {
	name       string
	clients    chan *mcc.Client
	hostName   string
	userName   string
	bucketName string
	password   string
	maxConn    int
	logger     *log.CommonLogger
}

type sslConnPool struct {
	connPool
	local_proxy_port      int
	remote_proxy_port     int
	remote_memcached_port int
	certificate           []byte
}

type connPoolMgr struct {
	conn_pools_map map[string]ConnPool
	map_lock       sync.RWMutex
	once           sync.Once
	logger         *log.CommonLogger
}

var _connPoolMgr connPoolMgr

// ensure that _connPoolMgr is initialized
func init() {
	ConnPoolMgr()
}

var WrongConnTypeError = errors.New("There is an exiting pool with the same name but with different connection type")
/******************************************************************
 *
 *  Connection management
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

func (p *connPool) IsClosed() bool {
	return p.clients == nil
}

func (p *connPool) Name() string {
	return p.name
}

func (p *connPool) IsFull() bool {
	return len(p.clients) >= p.maxConn
}

func (p *connPool) Get() (*mcc.Client, error) {
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

func (p *sslConnPool) Get() (*mcc.Client, error) {
	p.logger.Debugf("There are %d connections in the pool\n", len(p.clients))
	select {
	case client, ok := <-p.clients:
		if ok {
			return client, nil
		}
	default:
		//no more connection, create more
		mcClient, err := NewSSLConn(p.hostName, p.userName, p.password, p.remote_memcached_port, p.local_proxy_port, p.remote_proxy_port, p.certificate)
		return mcClient, err
	}

	return nil, errors.New("connection pool is closed")
}

//
// Release connection back to the pool
//
func (p *connPool) Release(client *mcc.Client) {
	// This would panic if p.clients is closed.  This
	// is intentional.

	//reset connection deadlines
	conn := client.Hijack()

	conn.(*net.TCPConn).SetReadDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))
	conn.(*net.TCPConn).SetWriteDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))

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
func (p *connPool) ReleaseConnections() {

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

func (connPoolMgr *connPoolMgr) GetOrCreatePool(poolNameToCreate string, hostname string, bucketname string, username string, password string, connsize int) (ConnPool, error) {
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()

	pool, ok := connPoolMgr.conn_pools_map[poolNameToCreate]
	if ok {
		_, ok = pool.(*connPool)
		if ok {
			return pool, nil
		} else {
			return nil, WrongConnTypeError
		}
	}

	var err error
	size := connsize
	if size == 0 {
		size = DefaultConnectionSize
	}
	pool = &connPool{clients: make(chan *mcc.Client, connsize),
		hostName:   hostname,
		userName:   username,
		password:   password,
		bucketName: bucketname,
		name:       poolNameToCreate,
		logger:     log.NewLogger("ConnPool", connPoolMgr.logger.LoggerContext())}
	connPoolMgr.conn_pools_map[poolNameToCreate] = pool

	go connPoolMgr.fillPool(pool.(*connPool), hostname, bucketname, username, password, connsize)
	return pool, err
}

func (connPoolMgr *connPoolMgr) GetOrCreateSSLPool(poolNameToCreate string, hostname string, bucketname string, username string, password string, connsize int,
	remote_mem_port int, local_proxy_port int, remote_proxy_port int, cert []byte) (ConnPool, error) {
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()

	pool, ok := connPoolMgr.conn_pools_map[poolNameToCreate]
	if ok {
		_, ok = pool.(*sslConnPool)
		if ok {
			return pool, nil
		} else {
			return nil, WrongConnTypeError
		}
	}

	var err error
	size := connsize
	if size == 0 {
		size = DefaultConnectionSize
	}
	p := &sslConnPool{
		connPool: connPool{clients: make(chan *mcc.Client, connsize),
			hostName:   hostname,
			userName:   username,
			password:   password,
			bucketName: bucketname,
			name:       poolNameToCreate,
			logger:     log.NewLogger("sslConnPool", connPoolMgr.logger.LoggerContext())},
		remote_memcached_port: remote_mem_port,
		local_proxy_port:      local_proxy_port,
		remote_proxy_port:     remote_proxy_port,
		certificate:           cert}
	connPoolMgr.conn_pools_map[poolNameToCreate] = p

	go connPoolMgr.fillPoolWithSSLConn(p, hostname, bucketname, username, password, connsize, remote_mem_port, local_proxy_port, remote_proxy_port, cert)
	return p, err

}

func (connPoolMgr *connPoolMgr) fillPoolWithSSLConn(p *sslConnPool, hostname, bucketname, username, password string,
	connsize int, remote_memcached_port int, local_proxy_port int, remote_proxy_port int, certificate []byte) error {
	connPoolMgr.logger.Infof("Fill Pool with ssl connection - poolName=%v", p.Name())
	connPoolMgr.logger.Infof("connectionSize=%d", connsize)

	//	 initialize the connection pool
	work_load := 10
	num_of_workers := int(math.Ceil(float64(connsize) / float64(work_load)))
	index := 0
	waitGrp := &sync.WaitGroup{}
	for i := 0; i < num_of_workers; i++ {
		var connectionsToCreate int
		if index+work_load < connsize {
			connectionsToCreate = work_load
		} else {
			connectionsToCreate = connsize - index
		}
		waitGrp.Add(1)
		go func(hostname, bucketname, username, password string, remote_memcached_port int, local_proxy_port int, remote_proxy_port int, certificate []byte, connectionsToCreate int, waitGrp *sync.WaitGroup) {
			defer waitGrp.Done()
			for i := 0; i < connectionsToCreate; i++ {
				mcClient, err := NewSSLConn(hostname, bucketname, password, remote_memcached_port, local_proxy_port, remote_proxy_port, certificate)
				if err == nil {
					connPoolMgr.logger.Info("A client connection is established")
					p.clients <- mcClient
				} else {
					connPoolMgr.logger.Errorf("error establishing connection with hostname=%s, username=%s, password=%s - %s", hostname, username, password, err)
				}
			}
		}(hostname, bucketname, username, password, remote_memcached_port, local_proxy_port, remote_proxy_port, certificate, connectionsToCreate, waitGrp)

		index = index + connectionsToCreate
	}

	waitGrp.Wait()

	if len(p.clients) == 0 {
		return fmt.Errorf("Failed to create connection pool of size %v for hostname=%v, bucketname=%v\n", p.Name(), hostname, bucketname)
	}

	connPoolMgr.logger.Infof("Connection pool %s is created with %d clients\n", p.Name(), len(p.clients))
	return nil
}

func (connPoolMgr *connPoolMgr) GetPool(poolName string) ConnPool {
	connPoolMgr.map_lock.RLock()
	defer connPoolMgr.map_lock.RUnlock()
	pool := connPoolMgr.conn_pools_map[poolName]

	return pool
}

func (connPoolMgr *connPoolMgr) FindPoolNamesByPrefix(poolNamePrefix string) []string {
	poolNames := []string{}
	connPoolMgr.map_lock.RLock()
	defer connPoolMgr.map_lock.RUnlock()
	for poolName, _ := range connPoolMgr.conn_pools_map {
		if strings.HasPrefix(poolName, poolNamePrefix) {
			poolNames = append(poolNames, poolName)
		}
	}

	return poolNames
}

func (connPoolMgr *connPoolMgr) RemovePool(poolName string) {
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()
	pool := connPoolMgr.conn_pools_map[poolName]
	pool.ReleaseConnections()
	delete(connPoolMgr.conn_pools_map, poolName)
	connPoolMgr.logger.Infof("Pool %v is removed, all connections are released", poolName)
}

func (connPoolMgr *connPoolMgr) fillPool(p *connPool, hostName string, bucketname string, username string, password string, connectionSize int) error {
	connPoolMgr.logger.Infof("Fill Pool - poolName=%v,", p.Name())
	connPoolMgr.logger.Infof("connectionSize=%d", connectionSize)

	//	 initialize the connection pool
	for i := 0; i < connectionSize; i++ {
		mcClient, err := NewConn(hostName, username, password)
		if err != nil {
			connPoolMgr.logger.Errorf("error establishing connection with hostname=%s, username=%s, password=%s - %s", hostName, username, password, err)
		}
		if err == nil {
			connPoolMgr.logger.Debug("A client connection is established")
			p.clients <- mcClient
		}
	}

	connPoolMgr.logger.Infof("Connection pool %s is filled with %d clients\n", p.Name(), len(p.clients))
	return nil
}

//
// This function creates a single connection to the vbucket master node.
//
func NewConn(hostName string, username string, password string) (conn *mcc.Client, err error) {
	// connect to host
	start_time := time.Now()
	conn, err = mcc.Connect("tcp", hostName)
	if err != nil {
		return nil, err
	}

	ConnPoolMgr().logger.Debugf("%vs spent on establish a connection to %v", time.Since(start_time).Seconds(), hostName)

	// authentic using user/pass
	if username != "" {
		ConnPoolMgr().logger.Debug("Authenticate...")
		_, err = conn.Auth(username, password)
		if err != nil {
			ConnPoolMgr().logger.Errorf("err=%v\n", err)
			conn.Close()
			return nil, err
		}
	}

	ConnPoolMgr().logger.Debugf("%vs spent on authenticate to %v", time.Since(start_time).Seconds(), hostName)
	return conn, nil
}

func NewSSLConn(hostname string, bucketname string, password string, remote_memcached_port int, local_proxy_port int, remote_proxy_port int, certificate []byte) (*mcc.Client, error) {
	//connect to local proxy port
	if len(certificate) == 0 {
		return nil, errors.New("No certificate is provided, can't establish ssl connection")
	}

	ssl_con_str := LocalHostName + UrlPortNumberDelimiter + strconv.FormatInt(int64(local_proxy_port), ParseIntBase)
	conn, err := net.Dial("tcp", ssl_con_str)
	if err != nil {
		ConnPoolMgr().logger.Errorf("Failed to establish ssl connection. err=%v\n", err)
		return nil, err
	}

	//establish ssl proxy connection
	handshake_msg := make(map[string]interface{})
	handshake_msg["proxyHost"] = hostname
	handshake_msg["proxyPort"] = remote_proxy_port
	handshake_msg["port"] = remote_memcached_port
	handshake_msg["bucket"] = bucketname
	handshake_msg["password"] = password

	//encode json
	msg, err := json.Marshal(handshake_msg)
	if err != nil {
		return nil, err
	}
	msgBytes := encodeSSLHandShakeMsg(msg)

	cert := encodeSSLHandShakeMsg(certificate)
	//send certificate
	conn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	conn.Write(msgBytes)
	conn.Write(cert)

	//receive response
	sizeBytes := make([]byte, 4)
	_, err = io.ReadFull(conn, sizeBytes)
	if err != nil {
		ConnPoolMgr().logger.Errorf("Failed to read. err=%v\n", err)
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBytes)
	if size > MAX_PAYLOAD_SIZE {
		return nil, errors.New("Failed to establish ssl connection - reply is invalid")
	}
	ConnPoolMgr().logger.Infof("payload size = %v\n", size)
	ackBytes := make([]byte, size)
	_, err = io.ReadFull(conn, ackBytes)
	if err != nil {
		ConnPoolMgr().logger.Errorf("Failed to read. err=%v\n", err)
		return nil, err
	}

	ack_map := make(map[string]interface{})
	err = json.Unmarshal(ackBytes, &ack_map)
	if err != nil {
		return nil, err
	}

	ConnPoolMgr().logger.Infof("ack = %v\n", ack_map)

	type_str, ok := ack_map["type"].(string)
	if !ok || type_str != "ok" {
		return nil, errors.New("Failed to establish ssl connection")
	}

	client, err := mcc.Wrap(conn)
	if err != nil {
		ConnPoolMgr().logger.Errorf("err=%v\n", err)
		conn.Close()
		return nil, err
	}

	ConnPoolMgr().logger.Info("memcached client on ssl connection is created")
	return client, nil
}

func encodeSSLHandShakeMsg(bytes []byte) []byte {
	ret := make([]byte, 4+len(bytes))
	binary.BigEndian.PutUint32(ret[0:4], uint32(len(bytes)))
	copy(ret[4:4+len(bytes)], bytes)
	return ret
}

//return the singleton ConnPoolMgr
func ConnPoolMgr() *connPoolMgr {
	_connPoolMgr.once.Do(func() {
		_connPoolMgr.conn_pools_map = make(map[string]ConnPool)
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
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()

	for key, pool := range connPoolMgr.conn_pools_map {
		connPoolMgr.logger.Infof("close pool %s", key)
		pool.ReleaseConnections()
	}

	connPoolMgr.conn_pools_map = make(map[string]ConnPool)
}
