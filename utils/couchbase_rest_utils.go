package utils

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strings"
	"syscall"
	"time"
)

func GetMemcachedSSLPortMap(hostName, username, password string, certificate []byte, sanInCertificate bool, bucket string, logger *log.CommonLogger) (map[string]uint16, error) {
	ret := make(map[string]uint16)

	logger.Infof("GetMemcachedSSLPort, hostName=%v\n", hostName)
	bucketInfo, err := GetClusterInfo(hostName, base.BPath+bucket, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return nil, err
	}

	nodesExt, ok := bucketInfo[base.NodeExtKey]
	if !ok {
		return nil, bucketInfoParseError(bucketInfo, logger)
	}

	nodesExtArray, ok := nodesExt.([]interface{})
	if !ok {
		return nil, bucketInfoParseError(bucketInfo, logger)
	}

	for _, nodeExt := range nodesExtArray {

		nodeExtMap, ok := nodeExt.(map[string]interface{})
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		hostname, err := GetHostNameFromNodeInfo(hostName, nodeExtMap, logger)
		if err != nil {
			return nil, err
		}

		service, ok := nodeExtMap[base.ServicesKey]
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		services_map, ok := service.(map[string]interface{})
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		kv_port, ok := services_map[base.KVPortKey]
		if !ok {
			// the node may not have kv services. skip the node
			continue
		}
		kvPortFloat, ok := kv_port.(float64)
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		hostAddr := GetHostAddr(hostname, uint16(kvPortFloat))

		kv_ssl_port, ok := services_map[base.KVSSLPortKey]
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		kvSSLPortFloat, ok := kv_ssl_port.(float64)
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
		}

		ret[hostAddr] = uint16(kvSSLPortFloat)
	}
	logger.Infof("memcached ssl port map=%v\n", ret)

	return ret, nil
}

func bucketInfoParseError(bucketInfo map[string]interface{}, logger *log.CommonLogger) error {
	errMsg := "Error parsing memcached ssl port of remote cluster."
	detailedErrMsg := errMsg + fmt.Sprintf("bucketInfo=%v", bucketInfo)
	logger.Errorf(detailedErrMsg)
	return fmt.Errorf(errMsg)
}

func HttpsHostAddr(hostAddr string, logger *log.CommonLogger) (string, error, bool) {
	hostName := GetHostName(hostAddr)
	sslPort, err, isInternalError := GetSSLPort(hostAddr, logger)
	if err != nil {
		return "", err, isInternalError
	}
	return GetHostAddr(hostName, sslPort), nil, false
}

func GetSSLPort(hostAddr string, logger *log.CommonLogger) (uint16, error, bool) {
	portInfo := make(map[string]interface{})
	err, statusCode := QueryRestApiWithAuth(hostAddr, base.SSLPortsPath, false, "", "", nil, false, base.MethodGet, "", nil, 0, &portInfo, nil, false, logger)
	if err != nil || statusCode != http.StatusOK {
		return 0, fmt.Errorf("Failed on calling %v, err=%v, statusCode=%v", base.SSLPortsPath, err, statusCode), false
	}
	sslPort, ok := portInfo[base.SSLPortKey]
	if !ok {
		errMsg := "Failed to parse port info. ssl port is missing."
		logger.Errorf("%v. portInfo=%v", errMsg, portInfo)
		return 0, fmt.Errorf(errMsg), true
	}

	sslPortFloat, ok := sslPort.(float64)
	if !ok {
		return 0, fmt.Errorf("ssl port is of wrong type. Expected type: float64; Actual type: %s", reflect.TypeOf(sslPort)), true
	}

	return uint16(sslPortFloat), nil, false
}

func GetClusterInfo(hostAddr, path, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error) {
	clusterInfo := make(map[string]interface{})
	err, statusCode := QueryRestApiWithAuth(hostAddr, path, false, username, password, certificate, sanInCertificate, base.MethodGet, "", nil, 0, &clusterInfo, nil, false, logger)
	if err != nil || statusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed on calling host=%v, path=%v, err=%v, statusCode=%v", hostAddr, path, err, statusCode)
	}
	return clusterInfo, nil
}

func GetClusterUUID(clusterConnInfoProvider base.ClusterConnectionInfoProvider, logger *log.CommonLogger) (string, error) {
	hostAddr, err := clusterConnInfoProvider.MyConnectionStr()
	if err != nil {
		return "", err
	}
	username, password, certificate, sanInCertificate, err := clusterConnInfoProvider.MyCredentials()
	if err != nil {
		return "", err
	}
	clusterInfo, err := GetClusterInfo(hostAddr, base.PoolsPath, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return "", err
	}
	clusterUUIDObj, ok := clusterInfo[base.UUIDKey]
	if !ok {
		return "", fmt.Errorf("Cannot find uuid key in cluster info. hostAddr=%v, clusterInfo=%v\n", hostAddr, clusterInfo)
	}
	clusterUUID, ok := clusterUUIDObj.(string)
	if !ok {
		// cluster uuid is "[]" for unintialized cluster
		_, ok = clusterUUIDObj.([]interface{})
		if ok {
			return "", fmt.Errorf("cluster %v is not initialized. clusterUUIDObj=%v\n", hostAddr, clusterUUIDObj)
		} else {
			return "", fmt.Errorf("uuid key in cluster info is not of string type. hostAddr=%v, clusterUUIDObj=%v\n", hostAddr, clusterUUIDObj)
		}
	}
	return clusterUUID, nil
}

// get a list of node infos with full info
// this api calls xxx/pools/nodes, which returns full node info including clustercompatibility, etc.
// the catch is that this xxx/pools/nodes is not supported by elastic search cluster
func GetNodeListWithFullInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error) {
	clusterInfo, err := GetClusterInfo(hostAddr, base.NodesPath, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return nil, err
	}

	return GetNodeListFromInfoMap(clusterInfo, logger)

}

// get a list of node infos with minimum info
// this api calls xxx/pools/default, which returns a subset of node info such as hostname
// this api can/needs to be used when connecting to elastic search cluster, which supports xxx/pools/default
func GetNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) ([]interface{}, error) {
	clusterInfo, err := GetClusterInfo(hostAddr, base.DefaultPoolPath, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return nil, err
	}

	return GetNodeListFromInfoMap(clusterInfo, logger)

}

func GetClusterUUIDAndNodeListWithMinInfo(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, []interface{}, error) {
	defaultPoolInfo, err := GetClusterInfo(hostAddr, base.DefaultPoolPath, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return "", nil, err
	}

	clusterUUID, err := GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo, logger)
	if err != nil {
		return "", nil, err
	}

	nodeList, err := GetNodeListFromInfoMap(defaultPoolInfo, logger)

	return clusterUUID, nodeList, err

}

// get bucket info
// a specialized case of GetClusterInfo
func GetBucketInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]interface{}, error) {
	bucketInfo := make(map[string]interface{})
	err, statusCode := QueryRestApiWithAuth(hostAddr, base.DefaultPoolBucketsPath+bucketName, false, username, password, certificate, sanInCertificate, base.MethodGet, "", nil, 0, &bucketInfo, nil, false, logger)
	if err == nil && statusCode == http.StatusOK {
		return bucketInfo, nil
	}
	if statusCode == http.StatusNotFound {
		return nil, NonExistentBucketError
	} else {
		logger.Errorf("Failed to get bucket info for bucket '%v'. host=%v, err=%v, statusCode=%v", bucketName, hostAddr, err, statusCode)
		return nil, fmt.Errorf("Failed to get bucket info.")
	}
}

// get bucket uuid
func BucketUUID(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error) {
	bucketInfo, err := GetBucketInfo(hostAddr, bucketName, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return "", err
	}

	return GetBucketUuidFromBucketInfo(bucketName, bucketInfo, logger)
}

// get bucket password
func BucketPassword(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (string, error) {
	bucketInfo, err := GetBucketInfo(hostAddr, bucketName, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return "", err
	}

	return GetBucketPasswordFromBucketInfo(bucketName, bucketInfo, logger)
}

// get a number of fields in bucket for validation purpose
// 1. bucket type
// 2. bucket uuid
// 3. bucket conflict resolution type
// 4. bucket eviction policy
// 6. bucket server vb map
func BucketValidationInfo(hostAddr, bucketName, username, password string, certificate []byte, sanInCertificate bool,
	logger *log.CommonLogger) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
	bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error) {
	bucketInfo, err = GetBucketInfo(hostAddr, bucketName, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return
	}

	bucketType, err = GetBucketTypeFromBucketInfo(bucketName, bucketInfo)
	if err != nil {
		err = fmt.Errorf("Error retrieving BucketType setting on bucket %v. err=%v", bucketName, err)
		return
	}
	bucketUUID, err = GetBucketUuidFromBucketInfo(bucketName, bucketInfo, logger)
	if err != nil {
		err = fmt.Errorf("Error retrieving UUID setting on bucket %v. err=%v", bucketName, err)
		return
	}
	bucketConflictResolutionType, err = GetConflictResolutionTypeFromBucketInfo(bucketName, bucketInfo)
	if err != nil {
		err = fmt.Errorf("Error retrieving ConflictResolutionType setting on bucket %v. err=%v", bucketName, err)
		return
	}
	bucketEvictionPolicy, err = GetEvictionPolicyFromBucketInfo(bucketName, bucketInfo)
	if err != nil {
		err = fmt.Errorf("Error retrieving EvictionPolicy setting on bucket %v. err=%v", bucketName, err)
		return
	}
	bucketKVVBMap, err = GetServerVBucketsMap(hostAddr, bucketName, bucketInfo)
	if err != nil {
		err = fmt.Errorf("Error retrieving server vb map on bucket %v. err=%v", bucketName, err)
		return
	}
	return
}

func GetBucketUuidFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	bucketUUID := ""
	bucketUUIDObj, ok := bucketInfo[base.UUIDKey]
	if !ok {
		return "", fmt.Errorf("Error looking up uuid of bucket %v", bucketName)
	} else {
		bucketUUID, ok = bucketUUIDObj.(string)
		if !ok {
			return "", fmt.Errorf("Uuid of bucket %v is of wrong type", bucketName)
		}
	}
	return bucketUUID, nil
}

func GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	bucketsObj, ok := defaultPoolInfo[base.BucketsKey]
	if !ok {
		errMsg := fmt.Sprintf("Cannot find buckets key in default pool info. defaultPoolInfo=%v\n", defaultPoolInfo)
		logger.Error(errMsg)
		return "", errors.New(errMsg)
	}
	bucketsInfo, ok := bucketsObj.(map[string]interface{})
	if !ok {
		errMsg := fmt.Sprintf("buckets in default pool info is not of map type. buckets=%v\n", bucketsObj)
		logger.Error(errMsg)
		return "", errors.New(errMsg)
	}
	uriObj, ok := bucketsInfo[base.URIKey]
	if !ok {
		errMsg := fmt.Sprintf("Cannot find uri key in buckets info. bucketsInfo=%v\n", bucketsInfo)
		logger.Error(errMsg)
		return "", errors.New(errMsg)
	}
	uri, ok := uriObj.(string)
	if !ok {
		errMsg := fmt.Sprintf("uri in buckets info is not of string type. uri=%v\n", uriObj)
		logger.Error(errMsg)
		return "", errors.New(errMsg)
	}

	return GetClusterUUIDFromURI(uri)
}

func GetClusterUUIDFromURI(uri string) (string, error) {
	// uri is in the form of /pools/default/buckets?uuid=d5dea23aa7ee3771becb3fcdb46ff956
	searchKey := base.UUIDKey + "="
	index := strings.LastIndex(uri, searchKey)
	if index < 0 {
		return "", fmt.Errorf("uri does not contain uuid. uri=%v", uri)
	}
	return uri[index+len(searchKey):], nil
}

func GetClusterCompatibilityFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (int, error) {
	nodeList, err := GetNodeListFromInfoMap(bucketInfo, logger)
	if err != nil {
		return 0, err
	}

	clusterCompatibility, err := GetClusterCompatibilityFromNodeList(nodeList)
	if err != nil {
		logger.Error(err.Error())
		return 0, err
	}

	return clusterCompatibility, nil
}

func GetBucketPasswordFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	bucketPassword := ""
	bucketPasswordObj, ok := bucketInfo[base.SASLPasswordKey]
	if !ok {
		return "", fmt.Errorf("Error looking up password of bucket %v", bucketName)
	} else {
		bucketPassword, ok = bucketPasswordObj.(string)
		if !ok {
			return "", fmt.Errorf("Password of bucket %v is of wrong type", bucketName)
		}
	}
	return bucketPassword, nil
}

func GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error) {
	// get node list from the map
	nodes, ok := infoMap[base.NodesKey]
	if !ok {
		// should never get here
		errMsg := fmt.Sprintf("info map contains no nodes. info map=%v", infoMap)
		logger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	nodeList, ok := nodes.([]interface{})
	if !ok {
		// should never get here
		errMsg := fmt.Sprintf("nodes is not of list type. type of nodes=%v", reflect.TypeOf(nodes))
		logger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	return nodeList, nil
}

func GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error) {
	if len(nodeList) > 0 {
		firstNode, ok := nodeList[0].(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("node info is of wrong type. node info=%v", nodeList[0])
		}
		clusterCompatibility, ok := firstNode[base.ClusterCompatibilityKey]
		if !ok {
			return 0, fmt.Errorf("Can't get cluster compatibility info. node info=%v", nodeList[0])
		}
		clusterCompatibilityFloat, ok := clusterCompatibility.(float64)
		if !ok {
			return 0, fmt.Errorf("cluster compatibility is not of int type. type=%v", reflect.TypeOf(clusterCompatibility))
		}
		return int(clusterCompatibilityFloat), nil
	}

	return 0, fmt.Errorf("node list is empty")
}

func GetNodeNameListFromNodeList(nodeList []interface{}, connStr string, logger *log.CommonLogger) ([]string, error) {
	nodeNameList := make([]string, 0)

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("node info is not of map type. type of node info=%v", reflect.TypeOf(node))
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}

		hostAddr, err := GetHostAddrFromNodeInfo(connStr, nodeInfoMap, logger)
		if err != nil {
			errMsg := fmt.Sprintf("cannot get hostname from node info %v", nodeInfoMap)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}

		nodeNameList = append(nodeNameList, hostAddr)
	}
	return nodeNameList, nil
}

func GetSSLProxyPortMap(hostAddr, username, password string, certificate []byte, sanInCertificate bool, logger *log.CommonLogger) (map[string]uint16, error) {
	nodeList, err := GetNodeListWithFullInfo(hostAddr, username, password, certificate, sanInCertificate, logger)
	if err != nil {
		return nil, err
	}

	ssl_port_map := make(map[string]uint16)
	for _, node := range nodeList {
		nodeMap, ok := node.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Node info is of wrong type. node info=%v\n", hostAddr, node)
		}

		hostname, err := GetHostNameFromNodeInfo(hostAddr, nodeMap, logger)
		if err != nil {
			return nil, err
		}

		portsObj, ok := nodeMap[base.PortsKey]
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Cannot find ports in node info =%v\n", hostAddr, node)
		}
		portsMap, ok := portsObj.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Ports info, %v, is of wrong type.\n", hostAddr, portsObj)
		}
		memcachedPortObj, ok := portsMap[base.DirectPortKey]
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Cannot find memcached port in ports info =%v\n", hostAddr, portsMap)
		}
		memcachedPort, ok := memcachedPortObj.(float64)
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Memcached port, %v, is of wrong type\n", hostAddr, memcachedPortObj)
		}
		sslProxyPortObj, ok := portsMap[base.SSLProxyPortKey]
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Cannot find ssl proxy port in ports info =%v\n", hostAddr, portsMap)
		}
		sslProxyPort, ok := sslProxyPortObj.(float64)
		if !ok {
			return nil, fmt.Errorf("Error constructing ssl port map for target cluster %v. Ssl proxy port, %v, is of wrong type\n", hostAddr, sslProxyPortObj)
		}
		hostAddr := GetHostAddr(hostname, uint16(memcachedPort))
		ssl_port_map[hostAddr] = uint16(sslProxyPort)
	}

	return ssl_port_map, nil
}

func GetHostAddrFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	var hostAddr string
	var ok bool
	hostAddrObj, ok := nodeInfo[base.HostNameKey]
	if !ok {
		logger.Infof("hostname is missing from node info %v. This could happen in local test env where target cluster consists of a single node, %v. Just use that node.\n", nodeInfo, adminHostAddr)
		hostAddr = adminHostAddr
	} else {
		hostAddr, ok = hostAddrObj.(string)
		if !ok {
			return "", fmt.Errorf("Error constructing ssl port map for target cluster %v. host name, %v, is of wrong type\n", hostAddr, hostAddrObj)
		}
	}

	return hostAddr, nil
}

func GetHostNameFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	hostAddr, err := GetHostAddrFromNodeInfo(adminHostAddr, nodeInfo, logger)
	if err != nil {
		return "", err
	}
	return GetHostName(hostAddr), nil
}

//convenient api for rest calls to local cluster
func QueryRestApi(baseURL string,
	path string,
	preservePathEncoding bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	logger *log.CommonLogger) (error, int) {
	return QueryRestApiWithAuth(baseURL, path, preservePathEncoding, "", "", nil, false, httpCommand, contentType, body, timeout, out, nil, false, logger)
}

func EnforcePrefix(prefix string, str string) string {
	var ret_str string = str
	if !strings.HasPrefix(str, prefix) {
		ret_str = prefix + str
	}
	return ret_str
}

func RemovePrefix(prefix string, str string) string {
	ret_str := strings.Replace(str, prefix, "", 1)
	return ret_str
}

//this expect the baseURL doesn't contain username and password
//if username and password passed in is "", assume it is local rest call,
//then call cbauth to add authenticate information
func QueryRestApiWithAuth(
	baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	certificate []byte,
	san_in_certificate bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger) (error, int) {
	http_client, req, err := prepareForRestCall(baseURL, path, preservePathEncoding, username, password, certificate, san_in_certificate, httpCommand, contentType, body, client, logger)
	if err != nil {
		return err, 0
	}

	err, statusCode := doRestCall(req, timeout, out, http_client, logger)
	cleanupAfterRestCall(keep_client_alive, err, http_client, logger)

	return err, statusCode
}

func prepareForRestCall(baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	certificate []byte,
	san_in_certificate bool,
	httpCommand string,
	contentType string,
	body []byte,
	client *http.Client,
	logger *log.CommonLogger) (*http.Client, *http.Request, error) {
	var l *log.CommonLogger = loggerForFunc(logger)
	var ret_client *http.Client = client
	req, host, err := ConstructHttpRequest(baseURL, path, preservePathEncoding, username, password, certificate, httpCommand, contentType, body, l)
	if err != nil {
		return nil, nil, err
	}

	if ret_client == nil {
		ret_client, err = GetHttpClient(certificate, san_in_certificate, host, l)
		if err != nil {
			l.Errorf("Failed to get client for request, err=%v, req=%v\n", err, req)
			return nil, nil, err
		}
	}
	return ret_client, req, nil
}

func cleanupAfterRestCall(keep_client_alive bool, err error, client *http.Client, logger *log.CommonLogger) {
	if !keep_client_alive || IsSeriousNetError(err) {
		if client != nil && client.Transport != nil {
			transport, ok := client.Transport.(*http.Transport)
			if ok {
				if IsSeriousNetError(err) {
					logger.Debugf("Encountered %v, close all idle connections for this http client.\n", err)
				}
				transport.CloseIdleConnections()
			}
		}
	}
}

func doRestCall(req *http.Request,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	logger *log.CommonLogger) (error, int) {
	var l *log.CommonLogger = loggerForFunc(logger)
	if timeout > 0 {
		client.Timeout = timeout
	} else if client.Timeout != base.DefaultHttpTimeout {
		client.Timeout = base.DefaultHttpTimeout
	}

	res, err := client.Do(req)
	if err == nil && res != nil && res.Body != nil {
		defer res.Body.Close()
		bod, e := ioutil.ReadAll(io.LimitReader(res.Body, res.ContentLength))
		if e != nil {
			l.Infof("Failed to read response body, err=%v\n req=%v\n", e, req)
			return e, res.StatusCode
		}
		if out != nil {
			err_marshal := json.Unmarshal(bod, out)
			if err_marshal != nil {
				l.Infof("Failed to unmarshal the response as json, err=%v, bod=%v\n req=%v\n", err_marshal, bod, req)
				out = bod
			} else {
				l.Debugf("out=%v\n", out)
			}
		} else {
			l.Debugf("out is nil")
		}
		return nil, res.StatusCode
	}

	return err, 0

}

//convenient api for rest calls to local cluster
func InvokeRestWithRetry(baseURL string,
	path string,
	preservePathEncoding bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger, num_retry int) (error, int, *http.Client) {
	return InvokeRestWithRetryWithAuth(baseURL, path, preservePathEncoding, "", "", nil, false, true, httpCommand, contentType, body, timeout, out, client, keep_client_alive, logger, num_retry)
}

func InvokeRestWithRetryWithAuth(baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	certificate []byte,
	san_in_certificate bool,
	insecureSkipVerify bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger, num_retry int) (error, int, *http.Client) {

	var http_client *http.Client = nil
	var ret_err error
	var statusCode int
	var req *http.Request = nil
	backoff_time := 500 * time.Millisecond

	for i := 0; i < num_retry; i++ {
		http_client, req, ret_err = prepareForRestCall(baseURL, path, preservePathEncoding, username, password, certificate, san_in_certificate, httpCommand, contentType, body, client, logger)
		if ret_err == nil {
			ret_err, statusCode = doRestCall(req, timeout, out, http_client, logger)
		}

		if ret_err == nil {
			break
		}

		logger.Infof("Received error when making rest call. baseURL=%v, path=%v, ret_err=%v, statusCode=%v, num_retry=%v\n", baseURL, path, ret_err, statusCode, i)

		//cleanup the idle connection if the error is serious network error
		cleanupAfterRestCall(true, ret_err, http_client, logger)

		//backoff
		backoff_time = backoff_time + backoff_time
		time.Sleep(backoff_time)
	}

	return ret_err, statusCode, http_client

}

func GetHttpClient(certificate []byte, san_in_certificate bool, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error) {
	var client *http.Client
	if len(certificate) != 0 {
		//https
		caPool := x509.NewCertPool()
		ok := caPool.AppendCertsFromPEM(certificate)
		if !ok {
			return nil, base.InvalidCerfiticateError
		}

		//using a separate tls connection to verify certificate
		//it can be changed in 1.4 when DialTLS is avaialbe in http.Transport
		conn, tlsConfig, err := base.MakeTLSConn(ssl_con_str, certificate, san_in_certificate, logger)
		if err != nil {
			return nil, err
		}
		conn.Close()

		tr := &http.Transport{TLSClientConfig: tlsConfig, Dial: base.DialTCPWithTimeout}
		client = &http.Client{Transport: tr,
			Timeout: base.DefaultHttpTimeout}

	} else {
		client = &http.Client{Timeout: base.DefaultHttpTimeout}
	}
	return client, nil
}

func maybeAddAuth(req *http.Request, username string, password string) {
	if username != "" && password != "" {
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	}
}

//this expect the baseURL doesn't contain username and password
//if username and password passed in is "", assume it is local rest call,
//then call cbauth to add authenticate information
func ConstructHttpRequest(
	baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	certificate []byte,
	httpCommand string,
	contentType string,
	body []byte,
	logger *log.CommonLogger) (*http.Request, string, error) {
	var baseURL_new string

	//process the URL
	if len(certificate) == 0 {
		baseURL_new = EnforcePrefix("http://", baseURL)
	} else {
		baseURL_new = EnforcePrefix("https://", baseURL)
	}
	u, err := couchbase.ParseURL(baseURL_new)
	if err != nil {
		return nil, "", err
	}

	var l *log.CommonLogger = loggerForFunc(logger)
	var req *http.Request = nil

	if !preservePathEncoding {
		if q := strings.Index(path, "?"); q > 0 {
			u.Path = path[:q]
			u.RawQuery = path[q+1:]
		} else {
			u.Path = path
		}

		req, err = http.NewRequest(httpCommand, u.String(), bytes.NewBuffer(body))
		if err != nil {
			return nil, "", err
		}
	} else {
		// use url.Opaque to preserve encoding
		u.Opaque = "//"

		index := strings.Index(baseURL_new, "//")
		if index < len(baseURL_new)-2 {
			u.Opaque += baseURL_new[index+2:]
		}
		u.Opaque += path

		req, err = http.NewRequest(httpCommand, baseURL_new, bytes.NewBuffer(body))
		if err != nil {
			return nil, "", err
		}

		// get the original Opaque back
		req.URL.Opaque = u.Opaque
	}

	if contentType == "" {
		contentType = base.DefaultContentType
	}
	req.Header.Set(base.ContentType, contentType)

	req.Header.Set(base.UserAgent, base.GoxdcrUserAgent)

	// username is nil when calling /nodes/self/xdcrSSLPorts on target
	// other username can be nil only in local rest calls
	if username == "" && path != base.SSLPortsPath {
		err := cbauth.SetRequestAuth(req)
		if err != nil {
			l.Errorf("Failed to set authentication to request, req=%v\n", req)
			return nil, "", err
		}
	} else {
		req.SetBasicAuth(username, password)
	}

	//TODO: log request would log password barely
	l.Debugf("http request=%v\n", req)

	return req, u.Host, nil
}

// encode http request into wire format
// it differs from HttpRequest.Write() in that it preserves the Content-Length in the header,
// and ignores Body in request
func EncodeHttpRequest(req *http.Request) ([]byte, error) {
	reqBytes := make([]byte, 0)
	reqBytes = append(reqBytes, []byte(req.Method)...)
	reqBytes = append(reqBytes, []byte(" ")...)
	reqBytes = append(reqBytes, []byte(req.URL.String())...)
	reqBytes = append(reqBytes, []byte(" HTTP/1.1\r\n")...)

	hasHost := false
	for key, value := range req.Header {
		if key == "Host" {
			hasHost = true
		}
		if value != nil && len(value) > 0 {
			reqBytes = EncodeHttpRequestHeader(reqBytes, key, value[0])
		} else {
			reqBytes = EncodeHttpRequestHeader(reqBytes, key, "")
		}
	}
	if !hasHost {
		// ensure that host name is in header
		reqBytes = EncodeHttpRequestHeader(reqBytes, "Host", req.Host)
	}

	// add extra "\r\n" as separator for Body
	reqBytes = append(reqBytes, []byte("\r\n")...)

	if req.Body != nil {
		defer req.Body.Close()

		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		reqBytes = append(reqBytes, bodyBytes...)
	}
	return reqBytes, nil
}

func EncodeHttpRequestHeader(reqBytes []byte, key, value string) []byte {
	reqBytes = append(reqBytes, []byte(key)...)
	reqBytes = append(reqBytes, []byte(": ")...)
	reqBytes = append(reqBytes, []byte(value)...)
	reqBytes = append(reqBytes, []byte("\r\n")...)
	return reqBytes
}

func IsSeriousNetError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	netError, ok := err.(*net.OpError)
	return err == syscall.EPIPE ||
		err == io.EOF ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "http: can't write HTTP request on broken connection") ||
		(ok && (!netError.Temporary() && !netError.Timeout()))
}
