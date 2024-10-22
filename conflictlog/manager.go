package conflictlog

import (
	"io"

	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	"github.com/couchbase/goxdcr/v8/service_impl/throttlerSvcImpl"
	"github.com/couchbase/goxdcr/v8/utils"
)

var manager Manager

var _ Manager = (*managerImpl)(nil)

// Manager defines behaviour for conflict manager
type Manager interface {
	NewLogger(logger *log.CommonLogger, replId string, opts ...LoggerOpt) (l Logger, err error)
	ConnPool() iopool.ConnPool
	SetConnLimit(limit int)
	SetIOPSLimit(limit int64)
	SetSkipTlsVerify(bool)
}

type SecurityInfo interface {
	IsClusterEncryptionLevelStrict() bool
	GetCACertificates() []byte
	GetClientCertAndKey() ([]byte, []byte)
}

type MemcachedAddrGetter interface {
	MyMemcachedAddr() (string, error)
}

type EncryptionInfoGetter interface {
	IsMyClusterEncryptionLevelStrict() bool
}

// GetManager returns the global conflict manager
func GetManager() (Manager, error) {
	if manager == nil {
		return nil, ErrManagerNotInitialized
	}

	return manager, nil
}

// InitManager intializes global conflict manager
func InitManager(loggerCtx *log.LoggerContext, utils utils.UtilsIface, memdAddrGetter MemcachedAddrGetter, securityInfo SecurityInfo) {
	logger := log.NewLogger(ConflictManagerLoggerName, loggerCtx)

	logger.Info("intializing conflict manager")

	throttlerSvc := throttlerSvcImpl.NewThroughputThrottlerSvc(loggerCtx)
	throttlerSvc.Start()

	impl := &managerImpl{
		logger:         logger,
		memdAddrGetter: memdAddrGetter,
		securityInfo:   securityInfo,
		utils:          utils,
		manifestCache:  newManifestCache(),
		connLimit:      DefaultPoolConnLimit,
		throttlerSvc:   throttlerSvc,
	}

	impl.setConnPool()
	manager = impl
}

// managerImpl implements conflict manager
type managerImpl struct {
	logger         *log.CommonLogger
	memdAddrGetter MemcachedAddrGetter
	securityInfo   SecurityInfo
	utils          utils.UtilsIface
	connPool       iopool.ConnPool
	manifestCache  *ManifestCache
	throttlerSvc   throttlerSvc.ThroughputThrottlerSvc

	// connLimit max number of connections
	connLimit int

	skipTlsVerify bool
}

func (m *managerImpl) NewLogger(logger *log.CommonLogger, replId string, opts ...LoggerOpt) (l Logger, err error) {
	opts = append(opts, WithSkipTlsVerify(m.skipTlsVerify))
	l, err = newLoggerImpl(logger, replId, m.utils, m.throttlerSvc, m.connPool, opts...)
	return
}

func (m *managerImpl) SetConnLimit(limit int) {
	m.logger.Infof("setting connection limit = %d", limit)
	m.connPool.SetLimit(limit)
}

func (m *managerImpl) SetSkipTlsVerify(v bool) {
	m.logger.Infof("setting tls skip verify=%v", v)
	m.skipTlsVerify = v
}

func (m *managerImpl) SetIOPSLimit(limit int64) {
	m.logger.Infof("setting IOPS limit = %d", limit)
	m.throttlerSvc.UpdateSettings(map[string]interface{}{
		throttlerSvc.LowTokensKey: limit,
	})
}

func (m *managerImpl) setConnPool() {
	m.logger.Infof("creating conflict manager gomemcached connection pool, connLimit=%v", m.connLimit)

	m.connPool = iopool.NewConnPool(m.logger, m.connLimit, m.newMemcachedConn)
}

func (m *managerImpl) ConnPool() iopool.ConnPool {
	return m.connPool
}

// Not in use. Was used for a POC.
// Refer to newMemcachedConn below, which is in use for conflict logging.
func (m *managerImpl) newGocbCoreConn(bucketName string) (conn io.Closer, err error) {
	m.logger.Infof("creating new conflict gocbcore bucket=%s encStrict=%v", bucketName, m.securityInfo.IsClusterEncryptionLevelStrict())

	return NewGocbConn(m.logger, m.memdAddrGetter, bucketName, m.securityInfo)
}

func (m *managerImpl) newMemcachedConn(bucketName string) (conn io.Closer, err error) {
	m.logger.Infof("creating new conflict memcached bucket=%s encStrict=%v", bucketName, m.securityInfo.IsClusterEncryptionLevelStrict())
	addr, err := m.memdAddrGetter.MyMemcachedAddr()
	if err != nil {
		return
	}

	conn, err = NewMemcachedConn(m.logger, m.utils, m.manifestCache, bucketName, addr, m.securityInfo, m.skipTlsVerify)
	return
}
