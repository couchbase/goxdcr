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

func GetMemcachedSSLPort(hostName, username, password, bucket string, logger *log.CommonLogger) (map[string]uint16, error) {
	ret := make(map[string]uint16)
	bucketInfo := make(map[string]interface{})

	logger.Infof("GetMemcachedSSLPort, hostName=%v\n", hostName)
	url := base.BPath + base.UrlDelimiter + bucket
	err, _ := QueryRestApiWithAuth(hostName, url, false, username, password, nil, base.MethodGet, "", nil, 0, &bucketInfo, nil, false, logger)
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

		hostname, ok := nodeExtMap[base.HostNameKey]
		if !ok {
			if len(nodesExtArray) == 1 {
				logger.Infof("hostname is missing from nodeExtMap %v. target cluster consists of a single node, %v. Just use that node.\n", nodeExtMap, hostName)
				hostname = GetHostName(hostName)
			} else {
				logger.Infof("hostname is missing from nodeExtMap %v. target cluster has multiple nodes. This is possible only in local test env where hostname is set to localhost. Use localhost.\n", nodeExtMap)
				hostname = base.LocalHostName
			}
		}
		hostnameStr, ok := hostname.(string)
		if !ok {
			return nil, bucketInfoParseError(bucketInfo, logger)
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

		hostAddr := GetHostAddr(hostnameStr, uint16(kvPortFloat))

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

func GetXDCRSSLPort(hostName, userName, password string, logger *log.CommonLogger) (uint16, error, bool) {

	portsInfo := make(map[string]interface{})
	err, _ := QueryRestApiWithAuth(hostName, base.SSLPortsPath, false, userName, password, nil, base.MethodGet, "", nil, 0, &portsInfo, nil, false, logger)
	if err != nil {
		return 0, err, false
	}
	// get ssl port from the map
	sslPort, ok := portsInfo[base.SSLPortKey]
	if !ok {
		// should never get here
		return 0, fmt.Errorf("Error parsing ssl port of remote cluster. portInfo=%v", portsInfo), true
	}

	sslPortFloat, ok := sslPort.(float64)
	if !ok {
		// should never get here
		return 0, errors.New(fmt.Sprintf("ssl port of remote cluster is of wrong type. Expected type: float64; Actual type: %s", reflect.TypeOf(sslPort))), true
	}
	return uint16(sslPortFloat), nil, false
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
	return QueryRestApiWithAuth(baseURL, path, preservePathEncoding, "", "", nil, httpCommand, contentType, body, timeout, out, nil, false, logger)
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
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger) (error, int) {
	http_client, req, err := prepareForRestCall(baseURL, path, preservePathEncoding, username, password, certificate, httpCommand, contentType, body, client, logger)
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
		ret_client, err = GetHttpClient(certificate, host, l)
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
	} else {
		client.Timeout = base.DefaultHttpTimeout
	}

	res, err := client.Do(req)
	if err == nil && res != nil && res.Body != nil {
		defer res.Body.Close()
		bod, e := ioutil.ReadAll(io.LimitReader(res.Body, res.ContentLength))
		if e != nil {
			l.Errorf("Failed to read response body, err=%v\n", e)
			return e, res.StatusCode
		}
		if out != nil {
			err_marshal := json.Unmarshal(bod, out)
			if err_marshal != nil {
				l.Debugf("Failed to unmarshal the response as json, err=%v\n", err)
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
	return InvokeRestWithRetryWithAuth(baseURL, path, preservePathEncoding, "", "", nil, true, httpCommand, contentType, body, timeout, out, client, keep_client_alive, logger, num_retry)
}

func InvokeRestWithRetryWithAuth(baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	certificate []byte,
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
		http_client, req, ret_err = prepareForRestCall(baseURL, path, preservePathEncoding, username, password, certificate, httpCommand, contentType, body, client, logger)
		if ret_err == nil {
			ret_err, statusCode = doRestCall(req, timeout, out, http_client, logger)
		}

		if ret_err == nil {
			break
		}

		logger.Infof("Received error when making rest call. baseURL=%v, path=%v, ret_err=%v, statusCode=%v, num_retry=%v\n", baseURL, path, ret_err, statusCode, num_retry)

		//cleanup the idle connection if the error is serious network error
		cleanupAfterRestCall(true, ret_err, http_client, logger)

		//backoff
		backoff_time = backoff_time + backoff_time
		time.Sleep(backoff_time)
	}

	return ret_err, statusCode, http_client

}

func GetHttpClient(certificate []byte, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error) {
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
		conn, tlsConfig, err := base.MakeTLSConn(ssl_con_str, certificate, logger)
		if err != nil {
			return nil, err
		}
		conn.Close()

		tr := &http.Transport{TLSClientConfig: tlsConfig, Dial: base.DialTCPWithTimeout}
		client = &http.Client{Transport: tr}

	} else {
		client = http.DefaultClient
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

	//if username is nil, assume it is local rest call
	if username == "" {
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
