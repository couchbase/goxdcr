package conflictlog

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/utils"
)

var _ Connection = (*MemcachedConn)(nil)

const (
	MCReadTimeout  = 30 * time.Second
	MCWriteTimeout = 30 * time.Second
)

type MemcachedConn struct {
	id            int64
	addr          string
	logger        *log.CommonLogger
	securityInfo  SecurityInfo
	bucketName    string
	bucketUUID    string
	vbCount       int
	connMap       map[string]mcc.ClientIface
	manifestCache *ManifestCache
	utilsObj      utils.UtilsIface
	bucketInfo    *BucketInfo
	opaque        uint32
	skipVerify    bool
}

func NewMemcachedConn(logger *log.CommonLogger, utilsObj utils.UtilsIface, manCache *ManifestCache, bucketName, bucketUUID string, vbCount int, addr string, securityInfo SecurityInfo, skipVerifiy bool) (m *MemcachedConn, err error) {
	if bucketUUID == "" {
		err = fmt.Errorf("bucketUUID cannot be empty")
		return
	}

	if vbCount == 0 {
		err = fmt.Errorf("vbCount cannot be zero")
		return
	}

	connId := NewConnId()

	user, passwd, err := cbauth.GetMemcachedServiceAuth(addr)
	if err != nil {
		return
	}

	m = &MemcachedConn{
		id:            connId,
		bucketName:    bucketName,
		bucketUUID:    bucketUUID,
		vbCount:       vbCount,
		addr:          addr,
		securityInfo:  securityInfo,
		logger:        logger,
		utilsObj:      utilsObj,
		manifestCache: manCache,
		skipVerify:    skipVerifiy,
	}

	conn, err := m.newMemcNodeConn(user, passwd, addr, false)
	if err != nil {
		return
	}

	m.connMap = map[string]mcc.ClientIface{
		addr: conn,
	}

	return
}

// newTLSConn creates an SSL connection to a memcached node.
// Note: this is subtly different than base.NewTlsConn(). In this we use cbauth's user/passwd
// over SSL connection instead of client certificate's SAN
func (m *MemcachedConn) newTLSConn(addr, user, passwd string) (conn mcc.ClientIface, err error) {
	// We load the certs everytime since the certs could have changed by ns_server
	// This will be actually not needed as these will be loaded only when the notification
	// from the ns_server. This will happen in security service.

	caCert := m.securityInfo.GetCACertificates()
	clientCert, clientKey := m.securityInfo.GetClientCertAndKey()

	x509Cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		InsecureSkipVerify: m.skipVerify,
		Certificates:       []tls.Certificate{x509Cert},
		RootCAs:            caCertPool,
	}

	tlsConn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, err
	}

	conn, err = mcc.Wrap(tlsConn)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	defer func() {
		if err != nil && conn != nil {
			conn.Close()
		}
	}()

	_, err = conn.Auth(user, passwd)
	if err != nil {
		return nil, err
	}

	_, err = conn.SelectBucket(m.bucketName)
	if err != nil {
		return nil, err
	}
	return
}

// newMemcNodeConn creates the new connection to a KV node.
// useSSL when false overrides the check for cluster being in strict mode.
// useSSL=false is used when establishing the connection the 'thisNode' which is using localhost.
// This is an initial connection to boot the rest of the connections and hence it has to be non-SSL
// The flip side is that when useSSL=true then it futher depends on whether cluster has encryption
// level strict or not. If yes then SSL connection is created
func (m *MemcachedConn) newMemcNodeConn(user, passwd, addr string, useSSL bool) (conn mcc.ClientIface, err error) {
	isEncStrict := false
	if useSSL {
		isEncStrict = m.securityInfo.IsClusterEncryptionLevelStrict()
	}

	var features utils.HELOFeatures
	features.Xattribute = true
	features.Xerror = true
	features.Collections = true
	features.DataType = true
	// For setMeta, negotiate compression, if it is set
	features.CompressionType = base.CompressionTypeSnappy
	m.logger.Infof("connecting to memcached id=%d user=%s addr=%s bucket=%s bucketUUID=%s encStrict=%v tlsSkipVerify=%v features=%+v",
		m.id, user, addr, m.bucketName, m.bucketUUID, isEncStrict, m.skipVerify, features)

	if isEncStrict {
		conn, err = m.newTLSConn(addr, user, passwd)
	} else {
		conn, err = base.NewConn(addr, user, passwd, m.bucketName, true, base.KeepAlivePeriod, m.logger)
	}

	if err != nil {
		return nil, err
	}

	userAgent := MemcachedConnUserAgent

	retFeatures, err := m.utilsObj.SendHELOWithFeatures(conn, userAgent, MCReadTimeout, MCWriteTimeout, features, m.logger)
	if err != nil {
		return
	}

	m.logger.Tracef("returned features: %s", retFeatures.String())

	return
}

func (m *MemcachedConn) fetchManifests(conn mcc.ClientIface) (man *metadata.CollectionsManifest, err error) {
	rsp, err := conn.GetCollectionsManifest()
	if err != nil {
		return
	}

	if rsp.Status != gomemcached.SUCCESS {
		err = fmt.Errorf("memcached request failed, req=GetCollectionManifest, status=%d, msg=%s", rsp.Status, string(rsp.Body))
		return
	}

	man = &metadata.CollectionsManifest{}
	err = man.UnmarshalJSON(rsp.Body)

	return
}

func (conn *MemcachedConn) Id() int64 {
	return conn.id
}

// getCollectionId first attempts to get the collectionId from the cache (if checkCache=true). If not found then
// it attempt to fetch it from the cluster using the same memcached connection. checkCache=false is generally used
// when we know that the value is cache is stale and a fresh one has to be fetched.
func (m *MemcachedConn) getCollectionId(conn mcc.ClientIface, target baseclog.Target, checkCache bool) (collId uint32, err error) {
	var ok bool

	if checkCache {
		collId, ok = m.manifestCache.GetCollectionId(target.Bucket, target.NS.ScopeName, target.NS.CollectionName)
		if ok {
			return
		}
	}

	m.logger.Infof("fetching manifests for checkCache=%v bucket=%s", checkCache, target.Bucket)

	man, err := m.fetchManifests(conn)
	if err != nil {
		return 0, err
	}

	collId, err = man.GetCollectionId(target.NS.ScopeName, target.NS.CollectionName)
	if err != nil {
		if err == base.ErrorNotFound {
			m.logger.Errorf("scope or collection not found. target=%s", target)
			err = baseclog.ErrScopeColNotFound
		}
		return 0, err
	}

	m.manifestCache.UpdateManifest(target.Bucket, man)
	return
}

func (m *MemcachedConn) setMeta(conn mcc.ClientIface, key string, vbNo uint16, body []byte, collId uint32, dataType uint8) (err error) {
	bufGetter := func(sz uint64) ([]byte, error) {
		return make([]byte, sz), nil
	}

	encCid, encLen, err := base.NewUleb128(collId, bufGetter, true)
	if err != nil {
		return
	}

	totalLen := encLen + len(key)
	keybuf := make([]byte, totalLen)
	copy(keybuf[0:encLen], encCid[0:encLen])
	copy(keybuf[encLen:], []byte(key))

	m.logger.Tracef("vbNo=%d encCid: %v, len=%d, keybuf:%v", vbNo, encCid[0:encLen], totalLen, keybuf)

	cas := uint64(time.Now().UnixNano())
	opaque := atomic.AddUint32(&m.opaque, 1)

	req := &gomemcached.MCRequest{
		Opcode:   base.SET_WITH_META,
		VBucket:  vbNo,
		Key:      keybuf,
		Keylen:   totalLen,
		Body:     body,
		Opaque:   opaque,
		Cas:      0,
		Extras:   make([]byte, 30),
		DataType: dataType,
	}

	var options uint32
	options |= base.SKIP_CONFLICT_RESOLUTION_FLAG
	binary.BigEndian.PutUint32(req.Extras[0:4], 0)
	binary.BigEndian.PutUint64(req.Extras[8:16], 0)
	binary.BigEndian.PutUint64(req.Extras[16:24], cas)
	binary.BigEndian.PutUint32(req.Extras[24:28], options)

	rsp, err := conn.Send(req)
	err2 := m.handleResponse(key, rsp, opaque)
	if err != nil || err2 != nil {
		newErr := fmt.Errorf("error in setMeta: err=%v, err2=%v", err, err2)
		return newErr
	}
	return
}

// Returns true if the response status should not be returned by design.
// Since each key of SetWithMeta is uniquely generated, we should never get:
// 1. KEY_EEXISTS
// 2. KEY_ENOENT
// 3. LOCKED
func IsCLogImpossibleResponse(status gomemcached.Status) bool {
	return base.IsMutationLockedError(status) ||
		base.IsENoEntError(status) ||
		base.IsEExistsError(status)
}

// Returns true if the response is fatal and cannot be fixed by resending.
// Such responses will be logged for debugging.
// This includes EINVAL and XATTR_EINVAL.
func IsCLogFatalResponse(status gomemcached.Status) bool {
	return status == gomemcached.EINVAL ||
		status == gomemcached.XATTR_EINVAL
}

func (m *MemcachedConn) handleResponse(key string, rsp *gomemcached.MCResponse, opaque uint32) (err error) {
	if rsp == nil {
		return
	}

	if rsp.Opaque != opaque {
		err = fmt.Errorf("opaque value mismatch expected=%d, got=%d", opaque, rsp.Opaque)
		return
	}

	status := rsp.Status
	if status == gomemcached.SUCCESS {
		err = nil
	} else if base.IsCollectionMappingError(status) {
		m.logger.Debugf("got clog unknown_collection response id=%d, key=%v%s%v, body=%v%s%v", m.id,
			base.UdTagBegin, key, base.UdTagEnd,
			base.UdTagBegin, rsp.Body, base.UdTagEnd,
		)
		err = baseclog.ErrUnknownCollection
	} else if base.IsTopologyChangeMCError(status) {
		m.logger.Debugf("got clog not_my_vbucket response id=%d, key=%v%s%v, body=%v%s%v", m.id,
			base.UdTagBegin, key, base.UdTagEnd,
			base.UdTagBegin, rsp.Body, base.UdTagEnd,
		)
		m.bucketInfo, err = parseNotMyVbucketValue(m.logger, rsp.Body, m.addr)
		if err != nil {
			return
		}
		err = baseclog.ErrNotMyVBucket
	} else if base.IsTemporaryMCError(status) {
		err = baseclog.ErrTMPFAIL
	} else if base.IsGuardRailError(status) {
		err = baseclog.ErrGuardrail
	} else if base.IsEAccessError(status) {
		err = baseclog.ErrEACCESS
	} else if IsCLogImpossibleResponse(status) {
		m.logger.Debugf("got clog impossible response id=%d, status=%s, key=%v%s%v", m.id, rsp.Status,
			base.UdTagBegin, key, base.UdTagEnd,
		)
		err = baseclog.ErrImpossibleResp
	} else if IsCLogFatalResponse(status) {
		m.logger.Errorf("got clog write fatal error id=%d, status=%s, key=%v%s%v, body=%v%v%v", m.id, rsp.Status,
			base.UdTagBegin, key, base.UdTagEnd,
			base.UdTagBegin, rsp.Body, base.UdTagEnd,
		)
	} else {
		// One may need to turn on debug logging to detect the unknown response.
		// There will be a counter to detect if we have received an unknown response.
		m.logger.Debugf("got clog write unknown error id=%d, status=%s, key=%v%s%v, body=%v%v%v", m.id,
			base.UdTagBegin, key, base.UdTagEnd,
		)
		err = baseclog.ErrUnknownResp
	}

	m.logger.Tracef("received rsp key=%s status=%d", rsp.Key, rsp.Status)

	return
}

// getConnByVB gets (or creates) connection to vbNo's memcached node
func (m *MemcachedConn) getConnByVB(vbno uint16, replicaNum int) (conn mcc.ClientIface, err error) {
	// The logic is as follows:
	//    We use non-tls addr if connecting to 'thisNode' (aka localhost).
	//    For everything else it depends if certs are enabled or not
	//    m.bucketInfo == nil implies that so far we have not received NOT_MY_VBUCKET error.

	addr2use := m.addr
	isEncStrict := m.securityInfo.IsClusterEncryptionLevelStrict()
	if m.bucketInfo != nil {
		_, hostname, port, sslPort, thisNode, err := m.bucketInfo.GetAddrByVB(vbno, replicaNum)
		if err != nil {
			return nil, err
		}

		addr2use = fmt.Sprintf("%s:%d", hostname, port)
		if !thisNode && isEncStrict {
			addr2use = fmt.Sprintf("%s:%d", hostname, sslPort)
		}
	}

	user, passwd, err := cbauth.GetMemcachedServiceAuth(addr2use)
	if err != nil {
		return
	}

	m.logger.Debugf("selecting id=%d certs=%v addr2use=%s for vb=%d", m.id, isEncStrict, addr2use, vbno)
	conn, ok := m.connMap[addr2use]
	if ok {
		return
	}

	conn, err = m.newMemcNodeConn(user, passwd, addr2use, true)
	if err != nil {
		return
	}

	m.connMap[addr2use] = conn
	return
}

func (m *MemcachedConn) SetMeta(key string, body []byte, dataType uint8, target baseclog.Target) (err error) {
	checkCache := true
	var collId uint32
	vbNo := base.GetVBucketNo(key, m.vbCount)

	var conn mcc.ClientIface

	for i := 0; i < 2; i++ {
		conn, err = m.getConnByVB(vbNo, 0)
		if err != nil {
			return err
		}

		collId, err = m.getCollectionId(conn, target, checkCache)
		if err != nil {
			return err
		}

		err = m.setMeta(conn, key, vbNo, body, collId, dataType)
		if err == nil {
			return
		}

		switch err {
		case baseclog.ErrUnknownCollection:
			m.logger.Infof("collection not found key=%s, target=%s", key, target.String())
			checkCache = false
		case baseclog.ErrNotMyVBucket:
		default:
			return err
		}
	}

	return
}

func (m *MemcachedConn) Close() error {
	m.logger.Infof("closing memcached conn id=%d", m.id)
	for _, conn := range m.connMap {
		conn.Close()
	}
	return nil
}

func parseNotMyVbucketValue(logger *log.CommonLogger, value []byte, sourceAddr string) (info *BucketInfo, err error) {
	logger.Tracef("parsing NOT_MY_VBUCKET response")

	sourceHost := base.GetHostName(sourceAddr)
	// Try to parse the value as a bucket configuration
	info, err = parseConfig(value, sourceHost)
	return
}

func parseConfig(config []byte, srcHost string) (info *BucketInfo, err error) {
	configStr := strings.Replace(string(config), "$HOST", srcHost, -1)

	info = new(BucketInfo)
	err = json.Unmarshal([]byte(configStr), info)
	if err != nil {
		return nil, err
	}

	return info, nil
}
