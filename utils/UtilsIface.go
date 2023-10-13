/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	"expvar"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
)

type ExponentialOpFunc func() error
type ExponentialOpFunc2 func(interface{}) (interface{}, error)
type ReleaseMemFunc func()
type RecycleObjFunc func(obj interface{})
type ErrReportFunc func(obj interface{})

type HELOFeatures struct {
	Xattribute      bool
	CompressionType base.CompressionType
	Xerror          bool
	Collections     bool
}

func (h *HELOFeatures) String() string {
	return fmt.Sprintf("Enabled features: Xattribute: %v CompressionType: %v Xerror: %v Collections: %v",
		h.Xattribute, base.CompressionTypeStrings[h.CompressionType], h.Xerror, h.Collections)
}

type UtilsIface interface {
	filter.FilterUtils

	// Please keep the interface alphabetically ordered
	/**
	 * ------------------------
	 * Memcached utilities
	 * ------------------------
	 */
	ComposeHELORequest(userAgent string, features HELOFeatures) *mc.MCRequest
	FilterExpressionMatchesDoc(expression, docId, bucketName string, collectionNs *base.CollectionNamespace, addr string, port uint16) (result bool, err error)
	GetMemcachedClient(serverAddr, bucketName string, kv_mem_clients map[string]mcc.ClientIface, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger, features HELOFeatures) (mcc.ClientIface, error)
	GetMemcachedConnection(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetMemcachedConnectionWFeatures(serverAddr, bucketName, userAgent string, keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error)
	GetMemcachedSSLPortMap(hostName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, bucket string, logger *log.CommonLogger, useExternal bool) (base.SSLPortMap, error)
	GetMemcachedRawConn(serverAddr, username, password, bucketName string, plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)

	/**
	 * ------------------------
	 * Local-Cluster Utilities
	 * ------------------------
	 */
	// Buckets related utilities
	BucketInfoParseError(bucketInfo map[string]interface{}, errMsg string, logger *log.CommonLogger) error
	BucketValidationInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
		logger *log.CommonLogger) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
		bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error)
	GetBucketTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetBucketUuidFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetCollectionManifestUidFromBucketInfo(bucketInfo map[string]interface{}) (uint64, error)
	GetConflictResolutionTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetEvictionPolicyFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error)
	GetHighSeqNos(vbnos []uint16, conn mcc.ClientIface, stats_map *map[string]string, collectionIds []uint32, recycledVbSeqnoMap *map[uint16]uint64) (*map[uint16]uint64, *map[string]string, []uint16, error)
	GetLocalBuckets(hostAddr string, logger *log.CommonLogger) (map[string]string, error)
	LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error)
	LocalBucketUUID(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error)
	BucketStorageBackend(bucketInfo map[string]interface{}) (string, error)
	GetCrossClusterVersioningFromBucketInfo(bucketInfo map[string]interface{}) (bool, error)
	GetVersionPruningWindowHrs(bucketInfo map[string]interface{}) (int, error)
	GetVbucketsMaxCas(bucketInfo map[string]interface{}) ([]interface{}, error)

	ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) ([]uint16, error)
	ParseHighSeqnoAndVBUuidFromStats(vbnos []uint16, stats_map map[string]string, high_seqno_and_vbuuid_map map[uint16][]uint64) ([]uint16, map[uint16]string)

	// Cluster related utilities
	GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error)
	GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	GetClusterUUIDFromURI(uri string) (string, error)
	GetReplicasInfo(bucketInfo map[string]interface{}, isStrictlySecure bool, recycledStringStringMap *base.StringStringMap, recycledVbHostMapGetter func(vbnos []uint16) *base.VbHostsMapType, recycledStringSliceGetter func() *[]string) (*base.VbHostsMapType, *base.StringStringMap, int, []uint16, error)
	GetServersListFromBucketInfo(bucketInfo map[string]interface{}) ([]string, error)
	GetServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}, recycledMapGetter func(nodes []string) *base.KvVBMapType, serversList []string) (*base.KvVBMapType, error)
	GetHostNamesFromBucketInfo(bucketInfo map[string]interface{}) ([]string, error)

	// Network related utilities
	ConstructHttpRequest(baseURL string, path string, preservePathEncoding bool, username string, password string, authMech base.HttpAuthMech, userAuthMode base.UserAuthMode, httpCommand string, contentType string, body []byte, logger *log.CommonLogger) (*http.Request, string, error)
	EnforcePrefix(prefix string, str string) string
	EncodeHttpRequest(req *http.Request) ([]byte, error)
	EncodeHttpRequestHeader(reqBytes []byte, key, value string) []byte
	EncodeMapIntoByteArray(data map[string]interface{}) ([]byte, error)
	GetHostAddrFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, isHttps bool, logger *log.CommonLogger, useExternal bool) (string, error)
	GetHttpClient(username string, authMech base.HttpAuthMech, certificate []byte, san_in_certificate bool, clientCertificate, clientKey []byte, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error)
	GetHostNameFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error)
	RemovePrefix(prefix string, str string) string
	UrlForLog(urlStr string) string

	// Errors related utilities
	BucketNotFoundError(bucketName string) error
	GetNonExistentBucketError() error
	GetBucketRecreatedError() error
	IsSeriousNetError(err error) bool
	InvalidRuneIndexErrorMessage(key string, index int) string
	NewEnhancedError(msg string, err error) error
	RecoverPanic(err *error)
	ReplicationStatusNotFoundError(topic string) error
	UnwrapError(infos map[string]interface{}) (err error)

	// Settings utilities
	GetIntSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) (int, error)
	GetStringSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) (string, error)
	GetSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) interface{}
	ValidateSettings(defs base.SettingDefinitions, settings metadata.ReplicationSettingsMap, logger *log.CommonLogger) error

	// Miscellaneous helpers
	ExponentialBackoffExecutor(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc) error
	ExponentialBackoffExecutorWithFinishSignal(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc2, param interface{}, finCh chan bool) (interface{}, error)
	ExponentialBackoffExecutorWithOriginalError(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc) (err error)
	GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]interface{}
	AddKeyToBeFiltered(currentValue, key []byte, dpGetter base.DpGetterFunc, toBeReleased *[][]byte, currentValueEndBody int) ([]byte, error, int64, int)
	StartDiagStopwatch(id string, threshold time.Duration) func()
	StartDebugExec(id string, threshold time.Duration, debugFunc func()) func()
	DumpStackTraceAfterThreshold(id string, threshold time.Duration, goroutines base.PprofLookupTypes) func()
	GetAuthMode(username string, clientCertificate []byte, path string, authMech base.HttpAuthMech) base.UserAuthMode

	/**
	 * ------------------------
	 * Remote-Cluster Utilities
	 * ------------------------
	 */
	// Buckets related utilities
	BucketUUID(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error)
	GetBuckets(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]string, error)
	GetBucketInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error)
	GetCurrentHostnameFromBucketInfo(bucketInfo map[string]interface{}) (string, error)
	GetIntExtHostNameKVPortTranslationMap(mapContainingNodesKey map[string]interface{}) (map[string]string, error)
	RemoteBucketValidationInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
		logger *log.CommonLogger, useExternal bool) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
		bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error)
	TranslateKvVbMap(kvVBMap base.KvVBMapType, targetBucketInfo map[string]interface{})
	VerifyTargetBucket(targetBucketName, targetBucketUuid string, remoteClusterRef *metadata.RemoteClusterReference, logger *log.CommonLogger) error

	// Collections related utilities
	GetCollectionsManifest(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (*metadata.CollectionsManifest, error)

	// Cluster related utilities
	GetClusterCompatibilityFromBucketInfo(bucketInfo map[string]interface{}, logger *log.CommonLogger) (int, error)
	GetClusterHeartbeatStatusFromNodeList(nodeList []interface{}) (map[string]base.HeartbeatStatus, error)
	GetClusterInfo(hostAddr, path, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error)
	GetClusterInfoWStatusCode(hostAddr, path, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error, int)
	GetClusterUUID(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error)
	GetClusterUUIDAndNodeListWithMinInfo(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, []interface{}, error)
	GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, []interface{}, error)
	GetDefaultPoolInfoUsingScramSha(hostAddr, username, password string, logger *log.CommonLogger) (map[string]interface{}, int, error)
	GetDefaultPoolInfoUsingHttps(hostHttpsAddr, username, password string,
		certificate []byte, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, int, error)
	GetExternalAddressAndKvPortsFromNodeInfo(nodeInfo map[string]interface{}) (string, int, error, int, error)
	GetExternalMgtHostAndPort(nodeInfo map[string]interface{}, isHttps bool) (string, int, error)
	GetHttpsMgtPortFromNodeInfo(nodeInfo map[string]interface{}) (int, error)
	GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error)
	GetNodeListWithMinInfo(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) ([]interface{}, error)
	GetNodeServicesInfo(hostAddr string, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error)
	GetPortsAndHostAddrsFromNodeServices(nodeServicesInfo map[string]interface{}, defaultConnStr string, logger *log.CommonLogger) (base.HostPortMapType, []string, error)
	GetRemoteNodeAddressesListFromNodeList(nodeList []interface{}, connStr string, needHttps bool, logger *log.CommonLogger, useExternal bool) (base.StringPairList, error)
	GetRemoteServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}, useExternal bool) (map[string][]uint16, error)
	GetSecuritySettingsAndDefaultPoolInfo(hostAddr, hostHttpsAddr, username, password string, certificate, clientCertificate, clientKey []byte, scramShaEnabled bool, logger *log.CommonLogger) (base.HttpAuthMech, map[string]interface{}, int, error)

	// Network related utilities
	GetRemoteMemcachedConnection(serverAddr, username, password, bucketName, userAgent string, plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error)
	GetRemoteMemcachedConnectionWFeatures(serverAddr, username, password, bucketName, userAgent string, plainAuth bool, keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error)
	GetRemoteSSLPorts(hostAddr string, logger *log.CommonLogger) (internalSSLPort uint16, internalSSLErr error, externalSSLPort uint16, externalSSLErr error)
	HttpsRemoteHostAddr(hostAddr string, logger *log.CommonLogger) (string, string, error)
	InvokeRestWithRetry(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, client *http.Client, keep_client_alive bool,
		logger *log.CommonLogger, num_retry int) (error, int)
	InvokeRestWithRetryWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, authMech base.HttpAuthMech,
		certificate []byte, san_in_certificate bool, clientCertificate, clientKey []byte,
		insecureSkipVerify bool, httpCommand string, contentType string, body []byte, timeout time.Duration,
		out interface{}, client *http.Client, keep_client_alive bool, logger *log.CommonLogger, num_retry int) (error, int)
	LocalPool(localConnectStr string) (couchbase.Pool, error)
	NewTCPConn(hostName string) (*net.TCPConn, error)
	QueryRestApi(baseURL string, path string, preservePathEncoding bool, httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{}, logger *log.CommonLogger) (error, int)
	QueryRestApiWithAuth(baseURL string, path string, preservePathEncoding bool, username string, password string, authMech base.HttpAuthMech,
		certificate []byte, san_in_certificate bool, clientCertificate, clientKey []byte,
		httpCommand string, contentType string, body []byte, timeout time.Duration, out interface{},
		client *http.Client, keep_client_alive bool, logger *log.CommonLogger) (error, int)

	ReplaceCouchApiBaseObjWithExternals(couchApiBase string, nodeInfo map[string]interface{}) string
	SendHELO(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, logger *log.CommonLogger) error
	SendHELOWithFeatures(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, requestedFeatures HELOFeatures, logger *log.CommonLogger) (respondedFeatures HELOFeatures, err error)
}
