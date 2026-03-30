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
var ErrSslConStrNotFound = errors.New("SSL connection string not found")

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

//  2. KvMemClientsMtx (inner lock): Protects the KvMemClients map.
//     Always acquire this lock AFTER poolConfigMtx if you need both locks to avoid deadlock.
//
//  3. sslConStrMtx: Protects SslConStrMap
//
//  4. inflightMtx: Protects serverInflight and serverPeakInflight maps.
//     Always acquire AFTER KvMemClientsMtx when both are needed.
//
// Lock Ordering Rule: poolConfigMtx -> KvMemClientsMtx -> inflightMtx (*never reverse*)
type RemoteMemcachedComponent struct {
	InitConnOnce sync.Once
	InitConnDone chan bool
	InitConnErr  error
	FinishCh     chan bool

	// key = kv host name, value = ssl connection str
	SslConStrMap  map[string]string
	sslConStrMtx  sync.RWMutex
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
	// knownServers denotes the current set of servers in the target topology.
	knownServers map[string]bool

	// GC-related fields for trimming idle connections.
	// serverInflight tracks the current number of inflight (checked-out) connections per server.
	serverInflight map[string]*atomic.Int64
	// serverPeakInflight tracks the peak inflight count per server within the current GC window.
	serverPeakInflight map[string]*atomic.Int64
	// inflightMtx protects serverInflight and serverPeakInflight maps.
	inflightMtx sync.RWMutex
	// minConnsPerServer is the minimum number of connections to keep per server after GC trimming.
	minConnsPerServer int
	// gcInterval is the interval at which the idle connection GC runs.
	gcInterval time.Duration
}

func NewRemoteMemcachedComponent(logger *log.CommonLogger, finCh chan bool, utils utilities.UtilsIface, bucketName string, userAgent string, tunables base.RemoteMemcachedTunables) *RemoteMemcachedComponent {
	return &RemoteMemcachedComponent{
		InitConnOnce:       sync.Once{},
		InitConnDone:       make(chan bool),
		FinishCh:           finCh,
		LoggerImpl:         logger,
		KvMemClients:       make(map[string]chan mcc.ClientIface),
		KvMemClientsMtx:    sync.RWMutex{},
		Utils:              utils,
		TargetBucketname:   bucketName,
		UserAgent:          userAgent,
		MaxConnsPerServer:  tunables.MaxConnsPerServer,
		knownServers:       map[string]bool{},
		serverInflight:     make(map[string]*atomic.Int64),
		serverPeakInflight: make(map[string]*atomic.Int64),
		minConnsPerServer:  tunables.MinConnsPerServer,
		gcInterval:         tunables.GCInterval,
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

// SetTunables updates the tunable parameters for the connection pool.
// It can be used to update maxConnsPerServer, minConnsPerServer, and gcInterval.
// Only the non-zero fields in the tunables struct are applied.
func (r *RemoteMemcachedComponent) SetTunables(tunables base.RemoteMemcachedTunables) {
	// Acquire poolConfigMtx to serialize pool configuration changes
	r.poolConfigMtx.Lock()
	defer r.poolConfigMtx.Unlock()

	// if the pool is closed, return
	if r.IsClosed() {
		r.LoggerImpl.Warnf("SetTunables called on closed pool for bucket %v", r.TargetBucketname)
		return
	}

	// Update max connections if specified
	if tunables.MaxConnsPerServer > 0 && tunables.MaxConnsPerServer != r.MaxConnsPerServer {
		r.MaxConnsPerServer = tunables.MaxConnsPerServer
		r.reconfigureConnectionPoolNoLock(r.MaxConnsPerServer)
		r.LoggerImpl.Infof("Updated max connections per server to %d for bucket %v", r.MaxConnsPerServer, r.TargetBucketname)
	}

	// Update min connections if specified
	if tunables.MinConnsPerServer >= 0 && tunables.MinConnsPerServer != r.minConnsPerServer {
		r.minConnsPerServer = tunables.MinConnsPerServer
		r.LoggerImpl.Infof("Updated min connections per server to %d for bucket %v", r.minConnsPerServer, r.TargetBucketname)
	}

	// Update GC interval if specified
	if tunables.GCInterval > 0 && tunables.GCInterval != r.gcInterval {
		r.gcInterval = tunables.GCInterval
		r.LoggerImpl.Infof("Updated GC interval to %v for bucket %v", r.gcInterval, r.TargetBucketname)
	}
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
			r.knownServers[server_addr] = true

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

	targetKvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return err
	}

	newSslConStrMap := make(map[string]string)
	for server_addr := range targetKvVbMap {
		ssl_port, ok := ssl_port_map[server_addr]
		if !ok {
			return fmt.Errorf("can't get remote memcached ssl port for %v", server_addr)
		}
		host_name := base.GetHostName(server_addr)
		ssl_con_str := base.GetHostAddr(host_name, uint16(ssl_port))
		newSslConStrMap[server_addr] = ssl_con_str
	}

	r.sslConStrMtx.Lock()
	r.SslConStrMap = newSslConStrMap
	r.sslConStrMtx.Unlock()

	return nil
}

func (r *RemoteMemcachedComponent) GetNewMemcachedClient(server_addr string) (mcc.ClientIface, error) {
	ref := r.RefGetter()
	username, password, _, certificate, san_in_certificate, client_certificate, client_key, err := ref.MyCredentials()
	if err != nil {
		return nil, err
	}
	if ref.IsFullEncryption() {
		r.sslConStrMtx.RLock()
		ssl_con_str, ok := r.SslConStrMap[server_addr]
		r.sslConStrMtx.RUnlock()
		if !ok {
			// This could happen if the server_addr is newly added and the SSL connection string map has not been refreshed yet.
			// Callers should retry incase of this error
			return nil, fmt.Errorf("%w for server %v", ErrSslConStrNotFound, server_addr)
		}

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
	var client mcc.ClientIface
	select {
	case client = <-clientChan:
	default:
		// No client available in pool, create a new one
		var err error
		client, err = r.GetNewMemcachedClient(serverAddr)
		if err != nil {
			return nil, err
		}
	}

	r.incrementInflight(serverAddr)
	return client, nil
}

// DiscardClient closes a broken client connection and decrements the inflight counter.
func (r *RemoteMemcachedComponent) DiscardClient(serverAddr string, client mcc.ClientIface) error {
	if client == nil {
		return nil
	}
	r.decrementInflight(serverAddr)
	return client.Close()
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

	r.decrementInflight(serverAddr)

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
			serversToDelete, serversToAdd, err := r.diffTopology()
			if err != nil {
				r.LoggerImpl.Warnf("failed to diff topology: %v", err)
				continue
			}

			if len(serversToDelete) == 0 && len(serversToAdd) == 0 {
				continue
			}

			// For new servers under full encryption, refresh the SSL connection string
			if len(serversToAdd) > 0 && r.RefGetter().IsFullEncryption() {
				if err := r.Utils.ExponentialBackoffExecutor("GetNewMemcachedClient: RefreshSslConStrMap", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
					base.RemoteMcRetryFactor, r.InitSSLConStrMap); err != nil {
					// This is a critical failure since without the SSL connection string map, we won't be able to create connections to the new servers at all.
					// Log the error and continue - Do not update the list of known servers, so that we can retry refreshing the SSL connection string map
					// in the next iteration of MonitorTopology.
					r.LoggerImpl.Errorf("failed to refresh SSL connection string map for new servers %v after %v retries: %v", serversToAdd, base.MaxRemoteMcRetry, err)
					continue
				}
			}

			// Acquire poolConfigMtx.Lock to safely modify the pool while blocking
			// all client operations, ensuring no one is using channels we're closing.
			r.poolConfigMtx.Lock()
			if len(serversToDelete) > 0 {
				r.removeServers(serversToDelete)
			}
			if len(serversToAdd) > 0 {
				r.addServers(serversToAdd)
			}
			r.poolConfigMtx.Unlock()
		}
	}
}

// diffTopology compares the cached known-server set against the target kvVbMap
// and returns servers that need to be removed and servers that need to be added.
func (r *RemoteMemcachedComponent) diffTopology() (serversToDelete, serversToAdd []string, err error) {
	// fetch the target kvVbMap
	targetKvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch target kvVbMap: %w", err)
	}

	r.KvMemClientsMtx.RLock()
	defer r.KvMemClientsMtx.RUnlock()

	// Servers present in the known set but absent from the topology should be removed.
	for serverAddr := range r.knownServers {
		if _, exists := targetKvVbMap[serverAddr]; !exists {
			serversToDelete = append(serversToDelete, serverAddr)
		}
	}

	// Servers present in the topology but absent from the known set should be added.
	for serverAddr := range targetKvVbMap {
		if !r.knownServers[serverAddr] {
			serversToAdd = append(serversToAdd, serverAddr)
		}
	}

	return serversToDelete, serversToAdd, nil
}

// removeServers deletes a list of servers from the pool.
// NOTE: Caller MUST hold a write lock on poolConfigMtx before calling.
func (r *RemoteMemcachedComponent) removeServers(servers []string) {
	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()

	for _, serverAddr := range servers {
		r.DeleteMemClientsNoLock(serverAddr)
		delete(r.knownServers, serverAddr)
	}

	r.inflightMtx.Lock()
	for _, serverAddr := range servers {
		delete(r.serverInflight, serverAddr)
		delete(r.serverPeakInflight, serverAddr)
	}
	r.inflightMtx.Unlock()
}

// addServers adds a list of servers to the known set.
func (r *RemoteMemcachedComponent) addServers(servers []string) {
	for _, serverAddr := range servers {
		r.knownServers[serverAddr] = true
	}
}

// getOrCreateInflightCounters returns the inflight and peak inflight counters for a server,
// creating them if they don't exist.
func (r *RemoteMemcachedComponent) getOrCreateInflightCounters(serverAddr string) (inflight, peak *atomic.Int64) {
	r.inflightMtx.RLock()
	inflight, ok1 := r.serverInflight[serverAddr]
	peak, ok2 := r.serverPeakInflight[serverAddr]
	r.inflightMtx.RUnlock()

	if ok1 && ok2 {
		return inflight, peak
	}

	r.inflightMtx.Lock()
	defer r.inflightMtx.Unlock()

	if _, ok := r.serverInflight[serverAddr]; !ok {
		r.serverInflight[serverAddr] = &atomic.Int64{}
	}
	if _, ok := r.serverPeakInflight[serverAddr]; !ok {
		r.serverPeakInflight[serverAddr] = &atomic.Int64{}
	}

	return r.serverInflight[serverAddr], r.serverPeakInflight[serverAddr]
}

// incrementInflight atomically increments the inflight count for a server
// and updates the peak if the new count exceeds it.
func (r *RemoteMemcachedComponent) incrementInflight(serverAddr string) {
	inflight, peak := r.getOrCreateInflightCounters(serverAddr)
	current := inflight.Add(1)
	for {
		p := peak.Load()
		if current <= p || peak.CompareAndSwap(p, current) {
			break
		}
	}
}

// decrementInflight atomically decrements the inflight count for a server.
func (r *RemoteMemcachedComponent) decrementInflight(serverAddr string) {
	r.inflightMtx.RLock()
	if counter, ok := r.serverInflight[serverAddr]; ok {
		counter.Add(-1)
	}
	r.inflightMtx.RUnlock()
}

// ResetPeak sets the peak inflight count for a server to the current inflight count.
// It returns the previous peak value before the reset.
func (r *RemoteMemcachedComponent) resetPeak(serverAddr string) int64 {
	inflight, peak := r.getOrCreateInflightCounters(serverAddr)

	var currentPeak int64
	var currentInflight int64
	for {
		currentPeak = peak.Load()
		currentInflight = inflight.Load()
		if peak.CompareAndSwap(currentPeak, currentInflight) {
			break
		}
	}
	return currentPeak
}

// RunIdleConnectionsGC periodically trims excess idle connections from the pool.
// It monitors the peak inflight (concurrent) connection usage per server over each GC window.
// Connections beyond the observed peak demand (with a floor of minConnsPerServer) are closed.
func (r *RemoteMemcachedComponent) RunIdleConnectionsGC() {
	ticker := time.NewTicker(r.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.FinishCh:
			return
		case <-ticker.C:
			r.gcIdleConnectionsOnce()
		}
	}
}

// gcIdleConnectionsOnce performs a single GC pass. For each server, it reads the peak inflight
// count observed during the current window, computes a target pool size, drains any excess
// connections from the channel, and resets the peak for the next window.
func (r *RemoteMemcachedComponent) gcIdleConnectionsOnce() {
	r.poolConfigMtx.RLock()
	defer r.poolConfigMtx.RUnlock()

	if r.IsClosed() {
		return
	}

	var clientsToClose []mcc.ClientIface

	r.KvMemClientsMtx.RLock()
	for serverAddr, clientChan := range r.KvMemClients {
		r.inflightMtx.RLock()
		currentInFlight, hasInflight := r.serverInflight[serverAddr]
		r.inflightMtx.RUnlock()

		if !hasInflight {
			continue
		}

		trimmedPoolSize := int64(r.minConnsPerServer)
		peak := r.resetPeak(serverAddr)
		if peak > trimmedPoolSize {
			trimmedPoolSize = peak
		}

		currentInUseConns := currentInFlight.Load()
		idleConns := int64(len(clientChan))
		totalConns := currentInUseConns + idleConns

		if totalConns > int64(r.MaxConnsPerServer) {
			totalConns = int64(r.MaxConnsPerServer)
		}

		excess := totalConns - trimmedPoolSize
		for i := int64(0); i < excess; i++ {
			select {
			case client := <-clientChan:
				clientsToClose = append(clientsToClose, client)
			default:
			}
		}
	}
	r.KvMemClientsMtx.RUnlock()

	errMap := make(base.ErrorMap)
	for _, client := range clientsToClose {
		if err := client.Close(); err != nil {
			errMap[fmt.Sprintf("idle_conn_%p", client)] = err
		}
	}
	if len(errMap) > 0 {
		r.LoggerImpl.Warnf("idle connections GC: errors closing idle connections: %v", base.FlattenErrorMap(errMap))
	}

	if len(clientsToClose) > 0 {
		r.LoggerImpl.Infof("idle connections GC: closed %d idle connection(s) for bucket %v on target cluster %s", len(clientsToClose), r.TargetBucketname, r.RefGetter().Name())
	}
}
