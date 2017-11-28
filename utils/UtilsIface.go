package utils

import (
	"expvar"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"net"
	"net/http"
	"regexp"
	"time"
)

type ExponentialOpFunc func() error
type ExponentialOpFunc2 func(interface{}) (interface{}, error)

type HELOFeatures struct {
	Xattribute      bool
	CompressionType base.CompressionType
}

type UtilsIface interface {
	// Please keep the interface alphabetically ordered
	/**
	 * ------------------------
	 * Memcached utilities
	 * ------------------------
	 */
	ComposeHELORequest(userAgent string, features HELOFeatures) *mc.MCRequest
	GetMemcachedClient(serverAddr, bucketName string, kv_mem_clients map[string]mcc.ClientIface, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetMemcachedConnection(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetMemcachedConnectionWFeatures(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error)
	GetMemcachedSSLPortMap(hostName, username, password string, certificate []byte, sanInCertificate bool, bucket string, logger *log.CommonLogger) (base.SSLPortMap, error)
	GetMemcachedRawConn(serverAddr, username, password, bucketName string, plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)

	/**
	 * ------------------------
	 * Local-Cluster Utilities
	 * ------------------------
	 */
	// Buckets related utilities
	BucketInfoParseError(bucketInfo map[string]interface{}, logger *log.CommonLogger) error
	CheckWhetherClusterIsESBasedOnBucketInfo(bucketInfo map[string]interface{}) bool
	GetBucketPasswordFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetBucketTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetBucketUuidFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetConflictResolutionTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetEvictionPolicyFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetLocalBuckets(hostAddr string, logger *log.CommonLogger) (map[string]string, error)
	LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error)
	LocalBucketUUID(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error)
	LocalBucketPassword(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error)
	ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) error
	ParseHighSeqnoAndVBUuidFromStats(vbnos []uint16, stats_map map[string]string, high_seqno_and_vbuuid_map map[uint16][]uint64)

	// Cluster related utilities
	GetClusterCompatibilityFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (int, error)
	GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error)
	GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetClusterUUIDFromURI(uri string) (string, error)
	GetServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}) (map[string][]uint16, error)
	GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error)

	// Network related utilities
	ConstructHttpRequest(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, httpCommand string, contentType string, body []byte, logger *log.CommonLogger) (*http.Request, string, error)
	EnforcePrefix(prefix string, str string) string
	EncodeHttpRequest(req *http.Request) ([]byte, error)
	EncodeHttpRequestHeader(reqBytes []byte, key, value string) []byte
	EncodeMapIntoByteArray(data map[string]interface{}) ([]byte, error)
	GetClientFromPoolWithRetry(componentName string, pool base.ConnPool, finish_ch chan bool, initialWait time.Duration, maxRetries, factor int, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetHttpClient(certificate []byte, san_in_certificate bool, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error)
	GetHostAddrFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetHostNameFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetSSLPort(hostAddr string, logger *log.CommonLogger) (uint16, error, bool)
	HttpsHostAddr(hostAddr string, logger *log.CommonLogger) (string, error, bool)
	RemovePrefix(prefix string, str string) string
	UrlForLog(urlStr string) string

	// Errors related utilities
	BucketNotFoundError(bucketName string) error
	GetNonExistentBucketError() error
	IsSeriousNetError(err error) bool
	InvalidRuneIndexErrorMessage(key string, index int) string
	NewEnhancedError(msg string, err error) error
	RecoverPanic(err *error)
	ReplicationStatusNotFoundError(topic string) error
	UnwrapError(infos map[string]interface{}) (err error)

	// Settings utilities
	GetIntSettingFromSettings(settings map[string]interface{}, settingName string) (int, error)
	GetStringSettingFromSettings(settings map[string]interface{}, settingName string) (string, error)
	GetSettingFromSettings(settings map[string]interface{}, settingName string) interface{}
	ValidateSettings(defs base.SettingDefinitions, settings map[string]interface{}, logger *log.CommonLogger) error

	// Miscellaneous helpers
	ExponentialBackoffExecutor(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc) error
	ExponentialBackoffExecutorWithFinishSignal(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc2, param interface{}, finCh chan bool) (interface{}, error)
	GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]interface{}
	GetMatchedKeys(expression string, keys []string) (map[string][][]int, error)
	RegexpMatch(regExp *regexp.Regexp, key []byte) bool

	/**
	 * ------------------------
	 * Remote-Cluster Utilities
	 * ------------------------
	 */
	// Buckets related utilities
	BucketUUID(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	BucketPassword(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	BucketValidationInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
		bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error)
	GetBuckets(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]string, error)
	GetBucketInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error)

	// Cluster related utilities
	GetClusterInfo(hostAddr, path, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error)
	GetClusterInfoWStatusCode(hostAddr, path, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error, int)
	GetClusterUUID(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	GetClusterUUIDAndNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, []interface{}, error)
	GetNodeListWithFullInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error)
	GetNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error)
	GetNodeNameListFromNodeList(nodeList []interface{}, connStr string, logger *log.CommonLogger) ([]string, error)

	// Network related utilities
	GetRemoteMemcachedConnection(serverAddr, username, password, bucketName, userAgent string, plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetRemoteMemcachedConnectionWFeatures(serverAddr, username, password, bucketName, userAgent string, plainAuth bool, keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error)
	InvokeRestWithRetry(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool,
		logger *log.CommonLogger, num_retry int) (error, int, *http.Client)
	InvokeRestWithRetryWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, san_in_certificate bool, insecureSkipVerify bool, httpCommand string, contentType string,
		body []byte, timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool, logger *log.CommonLogger, num_retry int) (error, int, *http.Client)
	LocalPool(localConnectStr string) (couchbase.Pool, error)
	NewTCPConn(hostName string) (*net.TCPConn, error)
	QueryRestApi(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, logger *log.CommonLogger) (error, int)
	QueryRestApiWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, san_in_certificate bool, httpCommand string, contentType string, body []byte,
		timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool, logger *log.CommonLogger) (error, int)
	SendHELO(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, logger *log.CommonLogger) error
	SendHELOWithFeatures(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, requestedFeatures HELOFeatures, logger *log.CommonLogger) (respondedFeatures HELOFeatures, err error)
}
