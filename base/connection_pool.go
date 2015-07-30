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
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
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
	MAX_PAYLOAD_SIZE    uint32        = 1000
	DialTimeoutDuration time.Duration = 3 * time.Second
	ReadWriteDeadline   time.Duration = 1 * time.Second
)

type ConnType int

const (
	MemConn      ConnType = iota
	SSLOverProxy ConnType = iota
	SSLOverMem   ConnType = iota
)

var (
	dialer *net.Dialer = &net.Dialer{Timeout: DialTimeoutDuration}
)

func (connType ConnType) String() string {
	if connType == MemConn {
		return "MemConn"
	} else if connType == SSLOverProxy {
		return "SSLOverProxy"
	} else if connType == SSLOverMem {
		return "SSLOverMem"
	} else {
		return "InvalidConnType"
	}
}

type NewConnFunc func() (*mcc.Client, error)

type ConnPool interface {
	Get() (*mcc.Client, error)
	GetCAS() uint32
	Release(client *mcc.Client)
	ReleaseConnections(cas uint32)
	NewConnFunc() NewConnFunc
	Name() string
	Size() int
	ConnType() ConnType
	Hostname() string
	Close()
}

type SSLConnPool interface {
	ConnPool
	Certificate() []byte
}

type connPool struct {
	name        string
	clients     chan *mcc.Client
	hostName    string
	userName    string
	bucketName  string
	password    string
	maxConn     int
	newConnFunc NewConnFunc
	logger      *log.CommonLogger
	lock        *sync.RWMutex
	cas         uint32
	cas_lock    *sync.RWMutex
}

type sslOverProxyConnPool struct {
	connPool
	local_proxy_port      int
	remote_proxy_port     int
	remote_memcached_port int
	certificate           []byte
}

type sslOverMemConnPool struct {
	connPool
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
	mcc.DefaultDialTimeout = DialTimeoutDuration
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

func (p *connPool) init() {
	p.newConnFunc = p.newConn
}

func (p *connPool) Name() string {
	return p.name
}

func (p *connPool) Hostname() string {
	return p.hostName
}

func (p *connPool) Size() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.clients != nil {
		return len(p.clients)
	} else {
		return 0
	}
}

func (p *connPool) Get() (*mcc.Client, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.clients != nil {
		p.logger.Debugf("There are %d connections in the pool\n", len(p.clients))
		select {
		case client, ok := <-p.clients:
			if ok {
				return client, nil
			}
		default:
			//no more connection, create more
			mcClient, err := p.newConnFunc()
			return mcClient, err
		}
	}
	return nil, errors.New("connection pool is closed")
}

func (p *connPool) newConn() (*mcc.Client, error) {
	return NewConn(p.hostName, p.userName, p.password)
}
func (p *connPool) NewConnFunc() NewConnFunc {
	return p.newConnFunc
}

func (p *connPool) ConnType() ConnType {
	return MemConn
}

func (p *sslOverProxyConnPool) init() {
	p.newConnFunc = p.newConn
}

func (p *sslOverProxyConnPool) Certificate() []byte {
	return p.certificate
}

func (p *sslOverProxyConnPool) newConn() (*mcc.Client, error) {
	//connect to local proxy port
	ssl_con_str := LocalHostName + UrlPortNumberDelimiter + strconv.FormatInt(int64(p.local_proxy_port), ParseIntBase)
	conn, err := DialTCPWithTimeout("tcp", ssl_con_str)
	if err != nil {
		ConnPoolMgr().logger.Errorf("Failed to establish ssl over proxy connection. err=%v\n", err)
		return nil, err
	}

	//establish ssl proxy connection
	handshake_msg := make(map[string]interface{})
	handshake_msg["proxyHost"] = p.hostName
	handshake_msg["proxyPort"] = p.remote_proxy_port
	handshake_msg["port"] = p.remote_memcached_port
	handshake_msg["bucket"] = p.bucketName
	handshake_msg["password"] = p.password

	//encode json
	msg, err := json.Marshal(handshake_msg)
	if err != nil {
		return nil, err
	}
	msgBytes := encodeSSLHandShakeMsg(msg)

	cert := encodeSSLHandShakeMsg(p.certificate)
	//send certificate
	conn.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	conn.Write(msgBytes)
	conn.Write(cert)

	//receive response
	sizeBytes := make([]byte, 4)
	_, err = io.ReadFull(conn, sizeBytes)
	if err != nil {
		ConnPoolMgr().logger.Errorf("Failed to read response to handshake message. err=%v\n", err)
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
		ConnPoolMgr().logger.Errorf("Failed to read ack. err=%v\n", err)
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

func (p *sslOverProxyConnPool) ConnType() ConnType {
	return SSLOverProxy
}

func (p *sslOverMemConnPool) init() {
	p.newConnFunc = p.newConn
}

func (p *sslOverMemConnPool) Certificate() []byte {
	return p.certificate
}

func (p *sslOverMemConnPool) newConn() (*mcc.Client, error) {

	//connect to local proxy port
	if len(p.certificate) == 0 {
		return nil, errors.New("No certificate is provided, can't establish ssl connection")
	}

	ssl_con_str := p.hostName + UrlPortNumberDelimiter + strconv.FormatInt(int64(p.remote_memcached_port), ParseIntBase)

	ConnPoolMgr().logger.Infof("Try to create a ssl over memcached connection on %v", ssl_con_str)
	conn, _, err := MakeTLSConn(ssl_con_str, p.certificate, p.logger)
	if err != nil {
		return nil, err
	}

	client, err := mcc.Wrap(conn)
	if err != nil {
		p.logger.Errorf("Failed to wrap connection. err=%v\n", err)
		conn.Close()
		return nil, err
	}

	// authentic using user/pass
	if p.bucketName != "" {
		ConnPoolMgr().logger.Info("Authenticate...")
		_, err = client.Auth(p.bucketName, p.password)
		if err != nil {
			ConnPoolMgr().logger.Errorf("err=%v\n", err)
			conn.Close()
			return nil, err
		}
	}

	ConnPoolMgr().logger.Info("memcached client on ssl connection is created")
	return client, nil
}

func (p *sslOverMemConnPool) ConnType() ConnType {
	return SSLOverMem
}

//
// Release connection back to the pool
//
func (p *connPool) Release(client *mcc.Client) {
	//reset connection deadlines
	conn := client.Hijack()

	conn.(net.Conn).SetReadDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))
	conn.(net.Conn).SetWriteDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))

	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.clients != nil {
		select {
		case p.clients <- client:
			return
		default:
			//the pool reaches its capacity, drop the client on the floor
			return
		}
	}
}

func (p *connPool) GetCAS() uint32 {
	p.cas_lock.RLock()
	defer p.cas_lock.RUnlock()
	return p.cas
}

func (p *connPool) incrementCAS() {
	p.cas_lock.Lock()
	defer p.cas_lock.Unlock()
	p.cas++
}

func (p *connPool) doesCASMatch(cas uint32) bool {
	p.cas_lock.RLock()
	defer p.cas_lock.RUnlock()
	if p.cas == cas {
		return true
	}
	return false
}

//
// Release all connections in the connection pool.
//
func (p *connPool) ReleaseConnections(cas uint32) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if !p.doesCASMatch(cas) {
		// no op if cas value does not match
		return
	}

	defer p.incrementCAS()

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
}

func (p *connPool) Close() {
	p.ReleaseConnections(p.GetCAS())

	p.lock.Lock()
	defer p.lock.Unlock()
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
		lock:       &sync.RWMutex{},
		cas_lock:   &sync.RWMutex{},
		logger:     log.NewLogger("ConnPool", connPoolMgr.logger.LoggerContext())}
	connPoolMgr.conn_pools_map[poolNameToCreate] = pool

	pool.(*connPool).init()
	go connPoolMgr.fillPool(pool.(*connPool), connsize)
	return pool, err
}

func (connPoolMgr *connPoolMgr) GetOrCreateSSLOverMemPool(poolNameToCreate string, hostname string, bucketname string, username string, password string, connsize int, remote_mem_port int, cert []byte) (ConnPool, error) {
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()

	pool, ok := connPoolMgr.conn_pools_map[poolNameToCreate]
	if ok {
		_, ok = pool.(*sslOverMemConnPool)
		if ok {
			return pool, nil
		} else {
			connPoolMgr.logger.Errorf("Found existing pool with name=%v, connType=%v\n", pool.Name(), pool.ConnType())
			return nil, WrongConnTypeError
		}
	}

	var err error
	size := connsize
	if size == 0 {
		size = DefaultConnectionSize
	}
	p := &sslOverMemConnPool{
		connPool: connPool{clients: make(chan *mcc.Client, connsize),
			hostName:   hostname,
			userName:   username,
			password:   password,
			bucketName: bucketname,
			name:       poolNameToCreate,
			lock:       &sync.RWMutex{},
			cas_lock:   &sync.RWMutex{},
			logger:     log.NewLogger("sslConnPool", connPoolMgr.logger.LoggerContext())},
		remote_memcached_port: remote_mem_port,
		certificate:           cert}
	p.init()

	connPoolMgr.conn_pools_map[poolNameToCreate] = p

	go connPoolMgr.fillPool(p, connsize)
	return p, err

}

func (connPoolMgr *connPoolMgr) GetOrCreateSSLOverProxyPool(poolNameToCreate string, hostname string, bucketname string, username string, password string, connsize int,
	remote_mem_port int, local_proxy_port int, remote_proxy_port int, cert []byte) (ConnPool, error) {
	connPoolMgr.map_lock.Lock()
	defer connPoolMgr.map_lock.Unlock()

	pool, ok := connPoolMgr.conn_pools_map[poolNameToCreate]
	if ok {
		_, ok = pool.(*sslOverProxyConnPool)
		if ok {
			return pool, nil
		} else {
			return nil, errors.New("There is an existing non-ssl over proxy pool with the same name")
		}
	}

	var err error
	size := connsize
	if size == 0 {
		size = DefaultConnectionSize
	}
	p := &sslOverProxyConnPool{
		connPool: connPool{clients: make(chan *mcc.Client, connsize),
			hostName:   hostname,
			userName:   username,
			password:   password,
			bucketName: bucketname,
			name:       poolNameToCreate,
			lock:       &sync.RWMutex{},
			cas_lock:   &sync.RWMutex{},
			logger:     log.NewLogger("sslConnPool", connPoolMgr.logger.LoggerContext())},
		remote_memcached_port: remote_mem_port,
		local_proxy_port:      local_proxy_port,
		remote_proxy_port:     remote_proxy_port,
		certificate:           cert}

	p.init()
	connPoolMgr.conn_pools_map[poolNameToCreate] = p

	go connPoolMgr.fillPool(p, connsize)
	return p, err

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
	pool.Close()
	delete(connPoolMgr.conn_pools_map, poolName)
	connPoolMgr.logger.Infof("Pool %v is removed, all connections are released", poolName)
}

func (connPoolMgr *connPoolMgr) fillPool(p ConnPool, connectionSize int) error {
	connPoolMgr.logger.Infof("Fill Pool - poolName=%v,connType=%v, connectionSize=%d\n", p.Name(), p.ConnType, connectionSize)

	//	 initialize the connection pool
	work_load := 10
	num_of_workers := int(math.Ceil(float64(connectionSize) / float64(work_load)))
	index := 0
	waitGrp := &sync.WaitGroup{}
	for i := 0; i < num_of_workers; i++ {
		var connectionsToCreate int
		if index+work_load < connectionSize {
			connectionsToCreate = work_load
		} else {
			connectionsToCreate = connectionSize - index
		}
		f := p.NewConnFunc()
		if f == nil {
			return fmt.Errorf("Pool %v is not properly initialized, no NewConnFunc is set")
		}
		waitGrp.Add(1)
		go func(connectionsToCreate int, waitGrp *sync.WaitGroup, f NewConnFunc) {
			defer waitGrp.Done()
			for i := 0; i < connectionsToCreate; i++ {
				mcClient, err := f()
				if err == nil {
					p.Release(mcClient)
					connPoolMgr.logger.Info("A client connection is established")
				} else {
					connPoolMgr.logger.Errorf("error establishing new connection for pool %v, connectionsToCreate=%v, err=%v", p.Name(), connectionsToCreate, err)
				}
			}
		}(connectionsToCreate, waitGrp, f)

		index = index + connectionsToCreate
	}

	waitGrp.Wait()

	if p.Size() == 0 {
		return fmt.Errorf("Failed to fill connection pool of size %v for %v\n", connectionSize, p.Name())
	}

	connPoolMgr.logger.Infof("Connection pool %s is created with %d clients\n", p.Name(), p.Size())
	return nil

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
		pool.ReleaseConnections(pool.GetCAS())
	}

	connPoolMgr.conn_pools_map = make(map[string]ConnPool)
}

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

func MakeTLSConn(ssl_con_str string, certificate []byte, logger *log.CommonLogger) (*tls.Conn, *tls.Config, error) {
	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(certificate)
	if !ok {
		return nil, nil, InvalidCerfiticateError
	}

	block, _ := pem.Decode([]byte(certificate))
	if block == nil {
		return nil, nil, InvalidCerfiticateError
	}
	cert_remote, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, InvalidCerfiticateError
	}

	tlsConfig := &tls.Config{RootCAs: caPool}
	tlsConfig.BuildNameToCertificate()
	tlsConfig.InsecureSkipVerify = true

	// Connect to tls
	conn, err := tls.DialWithDialer(dialer, "tcp", ssl_con_str, tlsConfig)

	if err != nil {
		logger.Errorf("Failed to connect to %v, err=%v\n", ssl_con_str, err)
		return nil, nil, err
	}

	// Handshake with TLS to get cert
	err = conn.Handshake()

	if err != nil {
		logger.Errorf("TLS handshake failed when connecting to %v, err=%v\n", ssl_con_str, err)
		return nil, nil, err
	}

	if cert_remote.IsCA {
		connState := conn.ConnectionState()
		peer_certs := connState.PeerCertificates

		opts := x509.VerifyOptions{
			Roots:         tlsConfig.RootCAs,
			CurrentTime:   time.Now(),
			Intermediates: x509.NewCertPool(),
		}

		if len(peer_certs[0].IPAddresses) > 0 {
			opts.DNSName = connState.ServerName
		} else {
			logger.Debug("remote peer has a certificate which doesn't have IP SANs, skip verifying ServerName")
		}

		for i, cert := range peer_certs {
			if i == 0 {
				continue
			}
			opts.Intermediates.AddCert(cert)
		}
		_, err = peer_certs[0].Verify(opts)

		if err != nil {
			//close the conn
			conn.Close()
			return nil, nil, err
		}
	}
	return conn, tlsConfig, nil

}

func DialTCPWithTimeout(network, address string) (net.Conn, error) {
	return dialer.Dial(network, address)
}
