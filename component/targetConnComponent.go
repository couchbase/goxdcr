package Component

import (
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	utilities "github.com/couchbase/goxdcr/v8/utils"

	"fmt"
	"sync"
)

// RemoteMemcachedComponent is an object to be embedded as another and provides the ability
// to contact target memcached for specific memcached purposes
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
	KvMemClients    map[string]mcc.ClientIface
	KvMemClientsMtx sync.RWMutex

	TargetUsername   func() string
	TargetPassword   func() string
	TargetBucketname string

	RefGetter func() *metadata.RemoteClusterReference

	Utils utilities.UtilsIface

	UserAgent string

	AlternateAddressCheck func(reference *metadata.RemoteClusterReference) (bool, error)
}

func NewRemoteMemcachedComponent(logger *log.CommonLogger, finCh chan bool, utils utilities.UtilsIface, bucketName string) *RemoteMemcachedComponent {
	return &RemoteMemcachedComponent{
		InitConnOnce:     sync.Once{},
		InitConnDone:     make(chan bool),
		FinishCh:         finCh,
		LoggerImpl:       logger,
		KvMemClients:     make(map[string]mcc.ClientIface),
		KvMemClientsMtx:  sync.RWMutex{},
		Utils:            utils,
		TargetBucketname: bucketName,
	}
}

func (r *RemoteMemcachedComponent) SetTargetUsernameGetter(getter func() string) *RemoteMemcachedComponent {
	r.TargetUsername = getter
	return r
}

func (r *RemoteMemcachedComponent) SetTargetPasswordGetter(getter func() string) *RemoteMemcachedComponent {
	r.TargetPassword = getter
	return r
}

func (r *RemoteMemcachedComponent) SetTargetKvVbMapGetter(getter func() (base.KvVBMapType, error)) *RemoteMemcachedComponent {
	r.TargetKvVbMap = getter
	return r
}

func (r *RemoteMemcachedComponent) SetRefGetter(getter func() *metadata.RemoteClusterReference) *RemoteMemcachedComponent {
	r.RefGetter = getter
	return r
}

func (r *RemoteMemcachedComponent) SetUserAgent(id string) *RemoteMemcachedComponent {
	r.UserAgent = id
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
	r.InitConnOnce.Do(func() {
		defer close(r.InitConnDone)
		if r.RefGetter().IsFullEncryption() {
			err := r.InitSSLConStrMap()
			if err != nil {
				r.LoggerImpl.Errorf("failed to initialize ssl connection string map, err=%v\n", err)
				r.InitConnErr = err
				return
			}
		}

		targetKvVbMap, err := r.TargetKvVbMap()
		if err != nil {
			r.InitConnErr = err
			return
		}
		for server_addr, _ := range targetKvVbMap {
			client, err := r.GetNewMemcachedClient(server_addr, true /*initializing*/)
			if err != nil {
				r.LoggerImpl.Errorf("failed to construct memcached client for %v, err=%v\n", server_addr, err)
				r.InitConnErr = err
				return
			}
			r.KvMemClientsMtx.Lock()
			r.KvMemClients[server_addr] = client
			r.KvMemClientsMtx.Unlock()
		}
		return
	})

	r.WaitForInitConnDone()
	return r.InitConnErr
}

func (r *RemoteMemcachedComponent) InitSSLConStrMap() error {
	connStr, err := r.RefGetter().MyConnectionStr()
	if err != nil {
		return err
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := r.RefGetter().MyCredentials()
	if err != nil {
		return err
	}

	useExternal, err := r.AlternateAddressCheck(r.RefGetter())
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

	for server_addr, _ := range targetKvVbMap {
		ssl_port, ok := ssl_port_map[server_addr]
		if !ok {
			return fmt.Errorf("Can't get remote memcached ssl port for %v", server_addr)
		}
		host_name := base.GetHostName(server_addr)
		ssl_con_str := base.GetHostAddr(host_name, uint16(ssl_port))
		r.SslConStrMap[server_addr] = ssl_con_str
	}

	return nil
}

func (r *RemoteMemcachedComponent) GetNewMemcachedClient(server_addr string, initializing bool) (mcc.ClientIface, error) {
	if r.RefGetter().IsFullEncryption() {
		_, _, _, certificate, san_in_certificate, client_certificate, client_key, err := r.RefGetter().MyCredentials()
		if err != nil {
			return nil, err
		}
		ssl_con_str := r.SslConStrMap[server_addr]

		if !initializing {
			// if not initializing at replication startup time, retrieve up to date security settings
			latestTargetClusterRef := r.RefGetter()

			connStr, err := latestTargetClusterRef.MyConnectionStr()
			if err != nil {
				return nil, err
			}
			// hostAddr not used in full encryption mode
			_, _, _, err = r.Utils.GetSecuritySettingsAndDefaultPoolInfo("" /*hostAddr*/, connStr,
				r.TargetUsername(), r.TargetPassword(), certificate, client_certificate, client_key, false /*scramShaEnabled*/, r.LoggerImpl)
			if err != nil {
				return nil, err
			}
		}
		return base.NewTLSConn(ssl_con_str, r.TargetUsername(), r.TargetPassword(), certificate, san_in_certificate, client_certificate, client_key, r.TargetBucketname, r.LoggerImpl)
	} else {
		return r.Utils.GetRemoteMemcachedConnection(server_addr, r.TargetUsername(), r.TargetPassword(),
			r.TargetBucketname, r.UserAgent, !r.RefGetter().IsEncryptionEnabled(), /*plain_auth*/
			base.KeepAlivePeriod, r.LoggerImpl)
	}
}

func (r *RemoteMemcachedComponent) WaitForInitConnDone() {
	select {
	case <-r.FinishCh:
		return
	case <-r.InitConnDone:
		return
	}
}

func (r *RemoteMemcachedComponent) CloseConnections() {
	// Originally this code was part of CheckpointManager which ensured that InitConnections() would have been called
	// Since refactoring, it is not guaranteed that remComponent.InitConnections() has been called since
	// now it is possible that no one ever ended up using the targetKVComponent for anything related to
	// pipelines
	// Since if InitConnections() is a noop if it has been called,
	// call InitConnections() here to ensure CloseConnections() will not hang
	go r.InitConnections()

	r.WaitForInitConnDone()
	r.KvMemClientsMtx.Lock()
	defer r.KvMemClientsMtx.Unlock()
	for server_addr, client := range r.KvMemClients {
		err := client.Close()
		if err != nil {
			r.LoggerImpl.Warnf("error from closing connection for %v is %v\n", server_addr, err)
		}
	}
	r.KvMemClients = make(map[string]mcc.ClientIface)
}

func (r *RemoteMemcachedComponent) GetOneTimeTgtFailoverLogs(vbsList []uint16) (map[uint16]*mcc.FailoverLog, error) {
	err := r.InitConnections()
	if err != nil {
		return nil, err
	}

	kvVbMap, err := r.TargetKvVbMap()
	if err != nil {
		return nil, err
	}
	filteredKvVbMap := kvVbMap.FilterByVBs(vbsList)

	failoverLogsMap := make(map[string]map[uint16]*mcc.FailoverLog)
	var failoverLogsMapMtx sync.Mutex

	errMap := make(base.ErrorMap)
	var errMapMtx sync.Mutex

	var waitGrp sync.WaitGroup
	r.KvMemClientsMtx.RLock()
	for kvTransient, mccClientTransient := range r.KvMemClients {
		mccClient := mccClientTransient
		kv := kvTransient
		// Get failoverlogs in parallel
		waitGrp.Add(1)
		go func() {
			defer waitGrp.Done()
			feed, err := mccClient.NewUprFeed()
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}

			err = feed.UprOpen(r.UserAgent, 0, base.UprFeedBufferSize)
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}
			defer feed.Close()

			failoverLogs, err := mccClient.UprGetFailoverLog(filteredKvVbMap[kv])
			if err != nil {
				errMapMtx.Lock()
				errMap[kv] = err
				errMapMtx.Unlock()
				return
			}

			failoverLogsMapMtx.Lock()
			failoverLogsMap[kv] = failoverLogs
			failoverLogsMapMtx.Unlock()
		}()
	}
	r.KvMemClientsMtx.RUnlock()
	waitGrp.Wait()

	if len(errMap) > 0 {
		r.LoggerImpl.Errorf("err getting failoverlogs from target %v", errMap)
		return nil, fmt.Errorf(base.FlattenErrorMap(errMap))
	}

	// We shouldn't have conflicting VBs since each target KV should own non-intersecting VBs
	compiledMap := make(map[uint16]*mcc.FailoverLog)
	for _, failoverLogsPerVb := range failoverLogsMap {
		for vb, failoverLogs := range failoverLogsPerVb {
			compiledMap[vb] = failoverLogs
		}
	}

	return compiledMap, nil
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
		return nil, fmt.Errorf(base.FlattenErrorMap(errMap))
	}

	return nodeMaxCasMap, nil
}

func (r *RemoteMemcachedComponent) getMaxCasWithRetry(serverAddr string, vbnos []uint16, finCh chan bool, casMap base.HighSeqnosMapType, casMapMtx *sync.Mutex, errMap base.ErrorMap, errMapMtx *sync.Mutex, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	vbSeqnoMap := make(map[uint16]uint64)
	maxCasOp := func(param interface{}) (interface{}, error) {
		var err error
		r.KvMemClientsMtx.RLock()
		client, ok := r.KvMemClients[serverAddr]
		r.KvMemClientsMtx.RUnlock()
		if !ok {
			// memcached connection may have been closed in previous retries. create a new one
			client, err = r.GetNewMemcachedClient(serverAddr, false /*initializing*/)
			if err != nil {
				r.LoggerImpl.Warnf("Retrieval of maxCas stats failed. serverAddr=%v, vbnos=%v\n", serverAddr, vbnos)
				return nil, err
			} else {
				r.KvMemClientsMtx.Lock()
				r.KvMemClients[serverAddr] = client
				r.KvMemClientsMtx.Unlock()
			}
		}

		r.KvMemClientsMtx.Lock()
		vbSeqnoMapReturned, vbsUnableToParse, err := r.Utils.GetMaxCasStatsForVBs(vbnos, client, nil, &vbSeqnoMap)
		if err != nil {
			if err == base.ErrorNoVbSpecified {
				err = fmt.Errorf("KV node %v has no vbucket assigned to it", serverAddr)
			}
			err1 := client.Close()
			if err1 != nil {
				r.LoggerImpl.Warnf("error from closing connection for %v is %v\n", serverAddr, err1)
			}
			delete(r.KvMemClients, serverAddr)
			r.KvMemClientsMtx.Unlock()
			return nil, err
		}
		r.KvMemClientsMtx.Unlock()

		vbSeqnoMap = vbSeqnoMapReturned
		if len(vbsUnableToParse) > 0 {
			r.LoggerImpl.Warnf("parsing VbMaxCas had vbs unable to parse %v", vbsUnableToParse)
		}

		return nil, err
	}

	_, opErr := r.Utils.ExponentialBackoffExecutorWithFinishSignal("StatsMapOnVBMaxCas", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, maxCasOp, nil, finCh)

	if opErr != nil {
		r.LoggerImpl.Errorf("Retrieval of maxCas stats failed after %v retries. serverAddr=%v, vbnos=%v\n",
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
