package conflictlog

import (
	"crypto/tls"
	"io"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	"github.com/couchbase/goxdcr/v8/utils"
)

var manager Manager
var singleton managerSingleton

var _ Manager = (*managerImpl)(nil)

// Manager defines behaviour for conflict manager
type Manager interface {
	NewLogger(logger *log.CommonLogger, replId string, eventsProducer common.PipelineEventsProducer, opts ...LoggerOpt) (l baseclog.Logger, err error)
	ConnPool() iopool.ConnPool
	SetConnLimit(limit int)
	SetIOPSLimit(limit int64)
}

type SecurityInfo interface {
	IsClusterEncryptionLevelStrict() bool
	GetCACertificates() []byte
	GetClientCertAndKey() ([]byte, []byte)
	GetClientCertAndKeyPair() []tls.Certificate
}

type managerSingleton struct {
	once sync.Once
}

// GetManager returns the global conflict manager
func GetManager() (Manager, error) {
	if manager == nil {
		return nil, baseclog.ErrManagerNotInitialized
	}

	return manager, nil
}

// InitManager intializes global conflict manager
func InitManager(loggerCtx *log.LoggerContext, topSvc service_def.XDCRCompTopologySvc, utils utils.UtilsIface, securityInfo SecurityInfo, throttler throttlerSvc.ThroughputThrottlerSvc, poolGCInterval, poolReapInterval time.Duration, connLimit int) {
	singleton.once.Do(func() {
		logger := log.NewLogger(ConflictManagerLoggerName, loggerCtx)

		logger.Info("intializing conflict manager")

		impl := &managerImpl{
			logger:           logger,
			securityInfo:     securityInfo,
			utils:            utils,
			manifestCache:    NewManifestCache(),
			poolGCInterval:   poolGCInterval,
			poolReapInterval: poolReapInterval,
			connLimit:        connLimit,
			throttlerSvc:     throttler,
			topSvc:           topSvc,
		}

		impl.setConnPool()

		manager = impl
	})
}

type connParams struct {
	bucketName string
	uuid       string
	vbCount    int
}

// managerImpl implements conflict manager
type managerImpl struct {
	logger        *log.CommonLogger
	securityInfo  SecurityInfo
	utils         utils.UtilsIface
	connPool      iopool.ConnPool
	manifestCache *ManifestCache
	throttlerSvc  throttlerSvc.ThroughputThrottlerSvc
	topSvc        service_def.XDCRCompTopologySvc

	poolGCInterval   time.Duration
	poolReapInterval time.Duration
	// connLimit is the max number of connections
	connLimit int

	skipTlsVerify bool
}

func (m *managerImpl) NewLogger(logger *log.CommonLogger, replId string, eventsProducer common.PipelineEventsProducer, opts ...LoggerOpt) (l baseclog.Logger, err error) {
	opts = append(opts, WithSkipTlsVerify(base.CLogSkipTlsVerify))
	l, err = newLoggerImpl(logger, replId, m.utils, m.securityInfo, m.throttlerSvc, m.topSvc, m.connPool, eventsProducer, m.manifestCache, opts...)
	return
}

func (m *managerImpl) SetConnLimit(limit int) {
	m.logger.Infof("setting connection limit = %d", limit)
	m.connPool.SetLimit(limit)
}

func (m *managerImpl) SetIOPSLimit(limit int64) {
	m.logger.Infof("setting IOPS limit = %d", limit)
	m.throttlerSvc.UpdateSettings(map[string]interface{}{
		throttlerSvc.LowTokensKey: limit,
	})
}

func (m *managerImpl) setConnPool() {
	m.logger.Infof("creating conflict manager gomemcached connection pool, connLimit=%v", m.connLimit)

	m.connPool = iopool.NewConnPool(m.logger,
		m.connLimit,
		m.poolGCInterval,
		m.poolReapInterval,
		m.newMemcachedConn,
		m.connPoolBucketDelFn)
}

func (m *managerImpl) ConnPool() iopool.ConnPool {
	return m.connPool
}

func (m *managerImpl) newMemcachedConn(bucketUUID string, params interface{}) (conn io.Closer, err error) {
	connParams := params.(*connParams)
	m.logger.Infof("creating new conflict memcached bucket=%s bucketUUID=%s vbCount=%d encStrict=%v",
		connParams.bucketName, connParams.uuid, connParams.vbCount, m.securityInfo.IsClusterEncryptionLevelStrict())
	addr, err := m.topSvc.MyMemcachedAddr()
	if err != nil {
		return
	}

	conn, err = NewMemcachedConn(m.logger, m.utils, m.manifestCache,
		connParams.bucketName, connParams.uuid, connParams.vbCount,
		addr, m.securityInfo, m.skipTlsVerify)
	return
}

// connPoolBucketDelFn is a callback which is called from connection pool
// The pool will call when bucket is being deleted. This happens when the bucket is not used for
// a defined interval.
func (m *managerImpl) connPoolBucketDelFn(bucketUUID string) {
	m.logger.Infof("deleting bucketUUID=%s from bucket cache", bucketUUID)
	m.manifestCache.DeleteByUUID(bucketUUID)
}
