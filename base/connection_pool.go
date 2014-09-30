package base

import (
	"errors"
	"net/url"
	//	"log"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	mcc "github.com/couchbase/gomemcached/client"
	cb "github.com/couchbaselabs/go-couchbase"
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
	client, ok := <-p.clients
	if ok {
		return client, nil
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
		mcClient, err := newConn(hostName, username, password)
		if err == nil {
			connPoolMgr.logger.Infof("A client connection is established")
			p.clients <- mcClient
		} else {
			connPoolMgr.logger.Infof("error establishing connection with hostname=%s, username=%s, password=%s - %s", hostName, username, password, err)
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
func newConnection(bucket *cb.Bucket, vbid uint16, username string, password string) (conn *mcc.Client, err error) {
	//	log.Println("start new connection")

	// make sure we release resource upon unexpected error
	defer func() {
		if r := recover(); r != nil {
			if conn != nil {
				conn.Close()
			}
			panic(r)
		}
	}()

	// Assertion
	// ***TODO: Better error message
	if bucket == nil {
		return nil, errors.New("Illegal Arguments")
	}

	// Through the vbucket map, get the host which is the vbucket master
	hostStr := GetHostStr(bucket, vbid)

	return newConn(hostStr, username, password)
}

func newConn(hostName string, username string, password string) (conn *mcc.Client, err error) {
	// connect to host
	conn, err = mcc.Connect("tcp", hostName)
	if err != nil {
		return nil, err
	}

	// authentic using user/pass
	if len(username) != 0 && username != "default" {
		_connPoolMgr.logger.Info("Authenticate...")
		_, err = conn.Auth(username, password)
		if err != nil {
			_connPoolMgr.logger.Infof("err=%v\n", err)
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

func GetHostStr(bucket *cb.Bucket, vbid uint16) string {
	vbmap := bucket.VBServerMap()
	serverIdx := vbmap.VBucketMap[vbid][0]
	hostStr := bucket.VBSMJson.ServerList[serverIdx]
	return hostStr
}
