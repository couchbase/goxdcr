package Component

import (
	"errors"
	"sync/atomic"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilities "github.com/couchbase/goxdcr/v8/utils"

	"fmt"
	"sync"
)

var ErrClientNotFound = errors.New("client not found")
var ErrPoolClosed = errors.New("pool is closed")

// RemoteMemcachedComponent is an object to be composed with another object to provide the ability
// to contact target memcached servers.
//
// Locking Strategy:
// This component uses a two-level locking mechanism to protect concurrent access:
//
// 1. poolConfigMtx (outer lock): Coordinates pool configuration with client operations.
//    - RLock: Used during client acquire/release, where we only read or use existing channels.
//    - Lock: Used when modifying the pool’s channels or structure (reconfiguration, closing,
//            topology updates).
//    Using poolConfigMtx.RLock ensures that channels cannot be modified or closed while clients
//    are being acquired or released.
//    Using poolConfigMtx.Lock prevents all client operations while the pool is being reconfigured
//    or shut down.

//  2. KvMemClientsMtx (inner lock): Protects the KvMemClients map. Always acquire this
//     lock AFTER poolConfigMtx if you need both locks to avoid deadlock.
//
// Lock Ordering Rule: poolConfigMtx -> KvMemClientsMtx (*never reverse*)
type RemoteMemcachedComponent struct {
	InitConnOnce sync.Once
	InitConnDone chan bool
	InitConnErr  error
	FinishCh     chan bool

	// key = kv host name, value = ssl connection str
	SslConStrMap  map[string]string
	TargetKvVbMap func() (base.KvVBMapType, error)

	LoggerImpl *log.CommonLogger

	// these fields are used for xmem replication only
	// memcached clients for retrieval of target bucket stats
	// kvMemClients is a map of server addresses to channels of clients
	KvMemClients    map[string]chan mcc.ClientIface
	KvMemClientsMtx sync.RWMutex

	// Maximum connections per server
	MaxConnsPerServer int

	TargetBucketname string

	RefGetter func() *metadata.RemoteClusterReference

	Utils utilities.UtilsIface

	UserAgent string

	AlternateAddressCheck func(ref *metadata.RemoteClusterReference) (bool, error)

	// poolConfigMtx serializes pool configuration with client acquire/release operations.
	poolConfigMtx sync.RWMutex
	// closed is used to denote if the component is closed
	closed atomic.Bool
}

func NewRemoteMemcachedComponent(logger *log.CommonLogger, finCh chan bool, utils utilities.UtilsIface, bucketName string, userAgent string, maxConnsPerServer int) *RemoteMemcachedComponent {
	return &RemoteMemcachedComponent{
		InitConnOnce:      sync.Once{},
		InitConnDone:      make(chan bool),
		FinishCh:          finCh,
		LoggerImpl:        logger,
		KvMemClients:      make(map[string]chan mcc.ClientIface),
		KvMemClientsMtx:   sync.RWMutex{},
		Utils:             utils,
		TargetBucketname:  bucketName,
		UserAgent:         userAgent,
		MaxConnsPerServer: maxConnsPerServer,
	}
}

// reconfigureConnectionPoolNoLock updates the pool to use the new maximum number of connections per server.
// If the new limit is lower than the current number of active connections,
// the excess connections are closed and removed.
// If the new limit is higher, additional connections will be created on-demand during AcquireClient calls.
// NOTE:
//   - Caller MUST hold a write lock on poolConfigMtx before calling
//   - This method acquires a write lock on KvMemClientsMtx internally
func (r *RemoteMemcachedComponent) reconfigureConnectionPoolNoLock(newMaxConnsPerServer int) {
	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()

	errMap := make(base.ErrorMap)

	for serverAddr, oldClientChan := range r.KvMemClients {
		// Create a new channel with the updated capacity
		newClientChan := make(chan mcc.ClientIface, newMaxConnsPerServer)
		r.KvMemClients[serverAddr] = newClientChan

		// Close the old channel to signal no more clients will be added
		close(oldClientChan)

		// Migrate existing clients to the new channel
		// If the new channel reaches its capacity, close the excess connections
		for client := range oldClientChan {
			select {
			case newClientChan <- client:
				// Success
			default:
				// New pool is full, close the excess connection
				if err := client.Close(); err != nil {
					errMap[serverAddr] = fmt.Errorf("error closing excess connection for %v during reconfiguration: %v", serverAddr, err)
				}
			}
		}
	}
	if len(errMap) > 0 {
		r.LoggerImpl.Warnf("Failed to close some excess connections during reconfiguration: %v", base.FlattenErrorMap(errMap))
	}
}

// SetMaxConnectionsPerServer sets the maximum number of connections per server.
func (r *RemoteMemcachedComponent) SetMaxConnectionsPerServer(max int) {
	if max < 1 {
		max = 1
	}

	// Acquire poolConfigMtx to serialize pool configuration changes
	r.poolConfigMtx.Lock()
	defer r.poolConfigMtx.Unlock()

	// Only reconfigure if the value actually changed
	if r.MaxConnsPerServer == max {
		return
	}

	// if the pool is closed, return
	if r.IsClosed() {
		r.LoggerImpl.Warnf("SetMaxConnectionsPerServer called on closed pool for bucket %v", r.TargetBucketname)
		return
	}

	r.MaxConnsPerServer = max

	// Honor the new max connections per server
	r.reconfigureConnectionPoolNoLock(max)
}

func (r *RemoteMemcachedComponent) SetTargetKvVbMapGetter(getter func() (base.KvVBMapType, error)) *RemoteMemcachedComponent {
	r.TargetKvVbMap = getter
	return r
}

func (r *RemoteMemcachedComponent) SetRefGetter(getter func() *metadata.RemoteClusterReference) *RemoteMemcachedComponent {
	r.RefGetter = getter
	return r
}

func (r *RemoteMemcachedComponent) SetAlternateAddressChecker(checker func(ref *metadata.RemoteClusterReference) (bool, error)) *RemoteMemcachedComponent {
	r.AlternateAddressCheck = checker
	return r
}

// Since this is remote target related connection, it is quite possible that the method here
// may fail and return a non-nil error
// It is not a guarantee that the memcached clients in kv_mem_clients map is usable or valid
// and thus callers should manually check and re-initate if necessary
func (r *RemoteMemcachedComponent) InitConnections() error {
	// CNG TODO: remove this RemoteMemcachedComponent
	if r.RefGetter().IsCNG() {
		return nil
	}

	r.InitConnOnce.Do(func() {
		defer close(r.InitConnDone)
		if r.RefGetter().IsFullEncryption() {
			err := r.InitSSLConStrMap()
			if err != nil {
				r.LoggerImpl.Errorf("failed to initialize ssl connection string map, err=%v", err)
				r.InitConnErr = err
				return
			}
		}

		targetKvVbMap, err := r.TargetKvVbMap()
		if err != nil {
			r.InitConnErr = err
			return
		}

		// Acquire poolConfigMtx to ensure no concurrent configuration changes
		r.poolConfigMtx.RLock()
		defer r.poolConfigMtx.RUnlock()

		r.KvMemClientsMtx.Lock()
		defer r.KvMemClientsMtx.Unlock()
		for server_addr := range targetKvVbMap {
			clientChan := make(chan mcc.ClientIface, r.MaxConnsPerServer)
			r.KvMemClients[server_addr] = clientChan

			client, err := r.GetNewMemcachedClient(server_addr)
			if err != nil {
				r.LoggerImpl.Warnf("failed to construct memcached client for %v, err=%v", server_addr, err)
				// Continue despite failure - AcquireClient will retry creating connections
				// on-demand, which handles any transient network errors gracefully
				continue
			}

			// Put the initial client into the channel pool
			clientChan <- client
		}
	})

	r.WaitForInitConnDone()
	return r.InitConnErr
}

func (r *RemoteMemcachedComponent) InitSSLConStrMap() error {
	ref := r.RefGetter()
	connStr, err := ref.MyConnectionStr()
	if err != nil {
		return err
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := ref.MyCredentials()
	if err != nil {
		return err
	}

	useExternal, err := r.AlternateAddressCheck(ref)
	if err != nil {
		return err
	}

	ssl_port_map, err := r.Utils.GetMemcachedSSLPortMap(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey,
		r.TargetBucketname, r.LoggerImpl, useExternal)
	if err != nil {
		return err
	}

	r.SslConStrMap = make(map[string]string)

	targetKvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return err
	}

	for server_addr := range targetKvVbMap {
		ssl_port, ok := ssl_port_map[server_addr]
		if !ok {
			return fmt.Errorf("can't get remote memcached ssl port for %v", server_addr)
		}
		host_name := base.GetHostName(server_addr)
		ssl_con_str := base.GetHostAddr(host_name, uint16(ssl_port))
		r.SslConStrMap[server_addr] = ssl_con_str
	}

	return nil
}

func (r *RemoteMemcachedComponent) GetNewMemcachedClient(server_addr string) (mcc.ClientIface, error) {
	ref := r.RefGetter()
	username, password, _, certificate, san_in_certificate, client_certificate, client_key, err := ref.MyCredentials()
	if err != nil {
		return nil, err
	}
	if ref.IsFullEncryption() {
		ssl_con_str := r.SslConStrMap[server_addr]

		// getTlsConnFunc is a function that creates a tls connection to the given server address
		getTlsConnFunc := func(_ interface{}) (interface{}, error) {
			tlsConn, err := base.NewTLSConn(ssl_con_str, username, password, certificate, san_in_certificate, client_certificate, client_key, r.TargetBucketname, r.LoggerImpl)
			if err != nil {
				return nil, err
			}
			return tlsConn, nil
		}

		// use exponential backoff to create a tls connection to the given server address
		tlsConn, err := r.Utils.ExponentialBackoffExecutorWithFinishSignal("RemoteMemcachedComponent: GetNewMemcachedClient", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
			base.RemoteMcRetryFactor, getTlsConnFunc, nil, r.FinishCh)
		if err != nil {
			return nil, err
		}

		return tlsConn.(mcc.ClientIface), nil
	}
	return r.Utils.GetRemoteMemcachedConnection(server_addr, username, password,
		r.TargetBucketname, r.UserAgent, !ref.IsEncryptionEnabled(), /*plain_auth*/
		base.KeepAlivePeriod, r.LoggerImpl)
}

func (r *RemoteMemcachedComponent) WaitForInitConnDone() {
	select {
	case <-r.FinishCh:
		return
	case <-r.InitConnDone:
		return
	}
}

func (r *RemoteMemcachedComponent) Close() {
	// Fast path: check if the component is already closed
	if r.IsClosed() {
		return
	}

	// Acquire poolConfigMtx to serialize the closing of the component
	r.poolConfigMtx.Lock()
	defer r.poolConfigMtx.Unlock()

	if !r.closed.CompareAndSwap(false, true) {
		// Another goroutine has already closed the component
		// so we just return
		return
	}

	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()

	// Drain and close all connections for each server
	for server_addr := range r.KvMemClients {
		r.DeleteMemClientsNoLock(server_addr)
	}
}

func (r *RemoteMemcachedComponent) IsClosed() bool {
	return r.closed.Load()
}

func (r *RemoteMemcachedComponent) GetVbMaxCasMap() (base.HighSeqnosMapType, error) {
	err := r.InitConnections()
	if err != nil {
		return nil, err
	}

	kvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return nil, err
	}

	nodeMaxCasMap := make(base.HighSeqnosMapType)
	var nodeMaxCasMtx sync.Mutex

	errMap := make(base.ErrorMap)
	var errMapMtx sync.Mutex

	var waitGrp sync.WaitGroup
	for serverAddr, vbnos := range kvVbMap {
		waitGrp.Add(1)
		go r.getMaxCasWithRetry(serverAddr, vbnos, r.FinishCh, nodeMaxCasMap, &nodeMaxCasMtx, errMap, &errMapMtx, &waitGrp)
	}
	waitGrp.Wait()

	if len(errMap) > 0 {
		return nil, errors.New(base.FlattenErrorMap(errMap))
	}

	return nodeMaxCasMap, nil
}

func (r *RemoteMemcachedComponent) getMaxCasWithRetry(serverAddr string, vbnos []uint16, finCh chan bool, casMap base.HighSeqnosMapType, casMapMtx *sync.Mutex, errMap base.ErrorMap, errMapMtx *sync.Mutex, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	vbSeqnoMap := make(map[uint16]uint64)
	var currentClient mcc.ClientIface

	maxCasOp := func(param interface{}) (interface{}, error) {
		var err error

		// Acquire a client from the pool
		currentClient, err = r.AcquireClient(serverAddr)
		if err != nil {
			r.LoggerImpl.Warnf("Retrieval of maxCas stats failed. serverAddr=%v, vbnos=%v", serverAddr, vbnos)
			return nil, err
		}

		vbSeqnoMapReturned, vbsUnableToParse, err := r.Utils.GetMaxCasStatsForVBs(vbnos, currentClient, nil, &vbSeqnoMap)
		if err != nil {
			if err == base.ErrorNoVbSpecified {
				err = fmt.Errorf("KV node %v has no vbucket assigned to it", serverAddr)
			}
			// Connection is broken, delete it instead of returning to pool
			if err := currentClient.Close(); err != nil {
				r.LoggerImpl.Warnf("error closing connection for %v: %v", serverAddr, err)
			}
			return nil, err
		}

		// release client to pool
		r.ReleaseClient(serverAddr, currentClient)

		vbSeqnoMap = vbSeqnoMapReturned
		if len(vbsUnableToParse) > 0 {
			r.LoggerImpl.Warnf("parsing VbMaxCas had vbs unable to parse %v", vbsUnableToParse)
		}

		return nil, err
	}

	_, opErr := r.Utils.ExponentialBackoffExecutorWithFinishSignal("StatsMapOnVBMaxCas", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, maxCasOp, nil, finCh)

	if opErr != nil {
		r.LoggerImpl.Errorf("Retrieval of maxCas stats failed after %v retries. serverAddr=%v, vbnos=%v",
			base.MaxRemoteMcRetry, serverAddr, vbnos)
		errMapMtx.Lock()
		errMap[serverAddr] = opErr
		errMapMtx.Unlock()
	} else {
		casMapMtx.Lock()
		casMap[serverAddr] = &vbSeqnoMap
		casMapMtx.Unlock()
	}
}

// getClientChannel gets the channel pool for a server
func (r *RemoteMemcachedComponent) getClientChannel(serverAddr string) (chan mcc.ClientIface, bool) {
	r.KvMemClientsMtx.RLock()
	defer r.KvMemClientsMtx.RUnlock()

	clientChan, exists := r.KvMemClients[serverAddr]
	return clientChan, exists
}

// createClientChannel creates a new channel pool for a server
// Returns the existing channel if one already exists
func (r *RemoteMemcachedComponent) createClientChannel(serverAddr string) chan mcc.ClientIface {
	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()

	// Check if it already exists
	if clientChan, exists := r.KvMemClients[serverAddr]; exists {
		return clientChan
	}

	// Create new buffered channel
	clientChan := make(chan mcc.ClientIface, r.MaxConnsPerServer)
	r.KvMemClients[serverAddr] = clientChan
	return clientChan
}

// AcquireClient acquires a client from the pool for the given server.
// It tries to draw from the channel pool first.
// If none available, it creates a new connection.
// Caller MUST call ReleaseClient to return the client to the pool when done.
func (r *RemoteMemcachedComponent) AcquireClient(serverAddr string) (mcc.ClientIface, error) {
	// Hold poolConfigMtx.RLock to prevent pool reconfiguration during client operations
	r.poolConfigMtx.RLock()
	defer r.poolConfigMtx.RUnlock()

	// if the pool is closed, return an error
	if r.IsClosed() {
		return nil, fmt.Errorf("failed to acquire client from the pool for bucket %v: %w", r.TargetBucketname, ErrPoolClosed)
	}

	// First, try to get existing channel
	clientChan, exists := r.getClientChannel(serverAddr)

	// If channel doesn't exist, create it
	if !exists {
		clientChan = r.createClientChannel(serverAddr)
	}

	// Try to get a client from the pool (non-blocking)
	select {
	case client := <-clientChan:
		return client, nil
	default:
		// No client available in pool, create a new one
		client, err := r.GetNewMemcachedClient(serverAddr)
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}

// ReleaseClient returns a client to the pool for the given server.
// If the channel buffer is full, the connection is closed and discarded.
// If the client is nil, this is a no-op.
func (r *RemoteMemcachedComponent) ReleaseClient(serverAddr string, client mcc.ClientIface) {
	if client == nil {
		return
	}

	// Hold poolConfigMtx.RLock to prevent pool reconfiguration during client operations
	r.poolConfigMtx.RLock()
	defer r.poolConfigMtx.RUnlock()

	// if the pool is closed, just close the client and return
	if r.IsClosed() {
		if err := client.Close(); err != nil {
			r.LoggerImpl.Warnf("error closing client for %v on bucket %v: %v", serverAddr, r.TargetBucketname, err)
		}
		return
	}

	clientChan, exists := r.getClientChannel(serverAddr)
	if !exists {
		// server is no longer in the target kvVbMap, close the client
		r.LoggerImpl.Infof("releasing client for removed server %v, closing connection", serverAddr)
		if err := client.Close(); err != nil {
			r.LoggerImpl.Warnf("error closing client for removed server %v: %v", serverAddr, err)
		}
		return
	}

	// Try to return the client to the pool (non-blocking)
	select {
	case clientChan <- client:
		// Successfully returned to pool
	default:
		// Pool is at capacity, close the excess connection
		if err := client.Close(); err != nil {
			r.LoggerImpl.Warnf("error closing excess connection to %v: %v", serverAddr, err)
		}
	}
}

// DeleteMemClientsNoLock removes all connections for a server.
// NOTE:
//   - Caller MUST hold a write lock on KvMemClientsMtx before calling
func (r *RemoteMemcachedComponent) DeleteMemClientsNoLock(serverAddr string) {
	clientChan, exists := r.KvMemClients[serverAddr]
	if !exists {
		return
	}

	// Drain and close all connections for the server
	close(clientChan)

	for client := range clientChan {
		err := client.Close()
		if err != nil {
			r.LoggerImpl.Warnf("remoteMemComponent: error closing connection for %v: %v", serverAddr, err)
		}
	}
	delete(r.KvMemClients, serverAddr)

	r.LoggerImpl.Infof("removed connection for server %v", serverAddr)
}

// MonitorTopology reconciles the connection pool with the current target topology.
//
// The kvVbMap is treated as the source of truth for target topology.
// This routine only removes connections for servers that are no longer present
// in the kvVbMap. Connections to newly added servers(if any) are created lazily on demand
func (r *RemoteMemcachedComponent) MonitorTopology() {
	ticker := time.NewTicker(base.TopologyChangeCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.FinishCh:
			return
		case <-ticker.C:
			serversToDelete, err := r.collectObsoleteServers()
			if err != nil {
				r.LoggerImpl.Warnf("failed to identify obsolete servers: %v", err)
				continue
			}
			// if there are servers to delete, delete them
			if len(serversToDelete) > 0 {
				// Acquire poolConfigMtx.Lock to safely remove servers while blocking
				// all client operations, ensuring no one is using channels we're closing
				r.poolConfigMtx.Lock()
				r.removeServers(serversToDelete)
				r.poolConfigMtx.Unlock()
			}
		}
	}
}

// collectObsoleteServers returns a list of servers that are no longer in the target kvVbMap.
func (r *RemoteMemcachedComponent) collectObsoleteServers() ([]string, error) {
	// fetch the target kvVbMap
	targetKvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch target kvVbMap: %w", err)
	}

	// collect the servers that are no longer in the target kvVbMap
	serversToDelete := make([]string, 0)

	r.KvMemClientsMtx.RLock()
	defer r.KvMemClientsMtx.RUnlock()

	for serverAddr := range r.KvMemClients {
		if _, exists := targetKvVbMap[serverAddr]; !exists {
			serversToDelete = append(serversToDelete, serverAddr)
		}
	}
	return serversToDelete, nil
}

// removeServers deletes a list of servers from the pool
func (r *RemoteMemcachedComponent) removeServers(servers []string) {
	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()

	for _, serverAddr := range servers {
		r.DeleteMemClientsNoLock(serverAddr)
	}
}
