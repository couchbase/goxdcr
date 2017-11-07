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

type UtilsIface interface {
	//	loggerForFunc(logger *log.CommonLogger) *log.CommonLogger
	//	ValidateSettings(defs base.SettingDefinition, settings map[string]interface{}, logger *log.CommonLogger) error
	RecoverPanic(err *error)
	LocalPool(localConnectStr string) (couchbase.Pool, error)
	LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error)
	UnwrapError(infos map[string]interface{}) (err error)
	NewEnhancedError(msg string, err error) error
	GetHostAddr(hostName string, port uint16) string
	GetHostName(hostAddr string) string
	GetPortNumber(hostAddr string) (uint16, error)
	GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]interface{}
	ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) error
	ParseHighSeqnoAndVBUuidFromStats(vbnos []uint16, stats_map map[string]string, high_seqno_and_vbuuid_map map[uint16][]uint64)
	EncodeMapIntoByteArray(data map[string]interface{}) ([]byte, error)
	UrlForLog(urlStr string) string
	GetMatchedKeys(expression string, keys []string) (map[string][][]int, error)
	RegexpMatch(regExp *regexp.Regexp, key []byte) bool
	//	convertByteIndexToRuneIndex(key string, matches [][]int) ([][]int, error)
	InvalidRuneIndexErrorMessage(key string, index int) string
	LocalBucketUUID(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error)
	LocalBucketPassword(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error)
	ReplicationStatusNotFoundError(topic string) error
	BucketNotFoundError(bucketName string) error
	GetMemcachedConnection(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetRemoteMemcachedConnection(serverAddr, username, password, bucketName, userAgent string, plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	SendHELO(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, logger *log.CommonLogger) error
	ComposeHELORequest(userAgent string, enableXattr bool) *mc.MCRequest
	GetIntSettingFromSettings(settings map[string]interface{}, settingName string) (int, error)
	GetStringSettingFromSettings(settings map[string]interface{}, settingName string) (string, error)
	GetSettingFromSettings(settings map[string]interface{}, settingName string) interface{}
	GetMemcachedClient(serverAddr, bucketName string, kv_mem_clients map[string]mcc.ClientIface, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}) (map[string][]uint16, error)
	GetBucketTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetConflictResolutionTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetEvictionPolicyFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetMemcachedSSLPortMap(hostName, username, password string, certificate []byte, sanInCertificate bool, bucket string, logger *log.CommonLogger) (map[string]uint16, error)
	BucketInfoParseError(bucketInfo map[string]interface{}, logger *log.CommonLogger) error
	HttpsHostAddr(hostAddr string, logger *log.CommonLogger) (string, error, bool)
	GetSSLPort(hostAddr string, logger *log.CommonLogger) (uint16, error, bool)
	GetClusterInfo(hostAddr, path, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error)
	GetClusterUUID(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	GetNodeListWithFullInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error)
	GetNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error)
	GetClusterUUIDAndNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, []interface{}, error)
	GetBuckets(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]string, error)
	GetBucketInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error)
	GetLocalBuckets(hostAddr string, logger *log.CommonLogger) (map[string]string, error)
	BucketUUID(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	BucketPassword(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error)
	BucketValidationInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
		bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error)
	GetBucketUuidFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetClusterUUIDFromURI(uri string) (string, error)
	GetClusterCompatibilityFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (int, error)
	GetBucketPasswordFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error)
	GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error)
	GetNodeNameListFromNodeList(nodeList []interface{}, connStr string, logger *log.CommonLogger) ([]string, error)
	GetHostAddrFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetHostNameFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	QueryRestApi(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, logger *log.CommonLogger) (error, int)
	EnforcePrefix(prefix string, str string) string
	RemovePrefix(prefix string, str string) string
	QueryRestApiWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, san_in_certificate bool, httpCommand string, contentType string, body []byte,
		timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool, logger *log.CommonLogger) (error, int)
	//	prepareForRestCall(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, san_in_certificate bool, httpCommand string, contentType string, body []byte,
	//		client *http.Client, logger *log.CommonLogger) (*http.Client, *http.Request, error)
	//	cleanupAfterRestCall(keep_client_alive bool, err error, client *http.Client, logger *log.CommonLogger)
	//	doRestCall(req *http.Request, timeout time.Duration, out interface{}, client *http.Client, logger *log.CommonLogger) (error, int)
	InvokeRestWithRetry(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool,
		logger *log.CommonLogger, num_retry int) (error, int, *http.Client)
	InvokeRestWithRetryWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, san_in_certificate bool, insecureSkipVerify bool, httpCommand string, contentType string,
		body []byte, timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool, logger *log.CommonLogger, num_retry int) (error, int, *http.Client)
	GetHttpClient(certificate []byte, san_in_certificate bool, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error)
	//	maybeAddAuth(req *http.Request, username string, password string)
	ConstructHttpRequest(baseURL string, path string, preservePathEncoding bool, username string, password string, certificate []byte, httpCommand string, contentType string, body []byte, logger *log.CommonLogger) (*http.Request, string, error)
	EncodeHttpRequest(req *http.Request) ([]byte, error)
	EncodeHttpRequestHeader(reqBytes []byte, key, value string) []byte
	IsSeriousNetError(err error) bool
	GetNonExistentBucketError() error
	//	GetLoggerUtils (*log.CommonLogger)
	ValidateSettings(defs base.SettingDefinitions, settings map[string]interface{}, logger *log.CommonLogger) error
	SendHELOWithXattrFeature(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, logger *log.CommonLogger) (xattrEnabled bool, err error)
	CheckWhetherClusterIsESBasedOnBucketInfo(bucketInfo map[string]interface{}) bool
	NewTCPConn(hostName string) (*net.TCPConn, error)
}
