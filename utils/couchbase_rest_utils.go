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
	"net/http"
	"reflect"
	"strings"
	"time"
)

//errors
var ErrorRetrievingSSLPort = errors.New("Could not get ssl port of remote cluster.")
var ErrorRetrievingMemcachedSSLPort = errors.New("Could not get memcached ssl port of remote cluster")
var ErrorRetrievingCouchApiBase = errors.New("Could not get couchApiBase in the response of /nodes/self.")

func GetMemcachedSSLPort(hostName, username, password, bucket string, logger *log.CommonLogger) (map[string]uint16, error) {
	ret := make(map[string]uint16)
	servicesInfo := make(map[string]interface{})

	pool, err := RemotePool(hostName, username, password)
	if err != nil {
		return nil, err
	}
	nodes := pool.Nodes

	if len(nodes) > 0 {

		logger.Infof("GetMemcachedSSLPort, hostName=%v\n", hostName)
		url := base.BPath + base.UrlDelimiter + bucket
		err, _ = QueryRestApiWithAuth(hostName, url, false, username, password, nil, base.MethodGet, "", nil, 0, &servicesInfo, logger)
		if err != nil {
			return nil, err
		}

		nodesExt, ok := servicesInfo[base.NodeExtKey]
		if !ok {
			return nil, ErrorRetrievingMemcachedSSLPort
		}

		nodesExtArray, ok := nodesExt.([]interface{})
		if !ok {
			return nil, ErrorRetrievingMemcachedSSLPort
		}

		for index, nodeExt := range nodesExtArray {
			nodeExtMap, ok := nodeExt.(map[string]interface{})
			if !ok {
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			hostnameStr := GetHostName(nodes[index].Hostname)

			service, ok := nodeExtMap[base.ServicesKey]
			if !ok {
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			services_map, ok := service.(map[string]interface{})
			if !ok {
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			kv_port, ok := services_map[base.KVPortKey]
			if !ok {
				return nil, ErrorRetrievingMemcachedSSLPort
			}
			kvPortFloat, ok := kv_port.(float64)
			if !ok {
				// should never get here
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			hostAddr := GetHostAddr(hostnameStr, uint16(kvPortFloat))

			kv_ssl_port, ok := services_map[base.KVSSLPortKey]
			if !ok {
				// should never get here
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			kvSSLPortFloat, ok := kv_ssl_port.(float64)
			if !ok {
				// should never get here
				return nil, ErrorRetrievingMemcachedSSLPort
			}

			ret[hostAddr] = uint16(kvSSLPortFloat)
		}
	}
	logger.Infof("ret=%v\n", ret)

	return ret, nil
}
func GetXDCRSSLPort(hostName, userName, password string, logger *log.CommonLogger) (uint16, error, bool) {

	portsInfo := make(map[string]interface{})
	err, _ := QueryRestApiWithAuth(hostName, base.SSLPortsPath, false, userName, password, nil, base.MethodGet, "", nil, 0, &portsInfo, logger)
	if err != nil {
		return 0, err, false
	}
	// get ssl port from the map
	sslPort, ok := portsInfo[base.SSLPortKey]
	if !ok {
		// should never get here
		return 0, ErrorRetrievingSSLPort, true
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
	return QueryRestApiWithAuth(baseURL, path, preservePathEncoding, "", "", nil, httpCommand, contentType, body, timeout, out, logger)
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
	logger *log.CommonLogger) (error, int) {

	req, host, err := ConstructHttpRequest(baseURL, path, preservePathEncoding, username, password, certificate, httpCommand, contentType, body, logger)
	if err != nil {
		return err, 0
	}

	var l *log.CommonLogger = loggerForFunc(logger)

	client, err := getHttpClient(certificate, host, logger)
	if err != nil {
		l.Errorf("Failed to get client for request, req=%v\n", req)
		return err, 0
	}
	defer func() {
		if client.Transport != nil {
			client.Transport.(*http.Transport).CloseIdleConnections()
		}
	}()

	if timeout > 0 {
		client.Timeout = timeout
	} else {
		client.Timeout = base.DefaultHttpTimeout
	}

	res, err := client.Do(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
		bod, e := ioutil.ReadAll(io.LimitReader(res.Body, res.ContentLength))
		if e != nil {
			l.Errorf("Failed to read response body, err=%v\n", e)
			return err, res.StatusCode
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
		return err, res.StatusCode
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
	logger *log.CommonLogger, num_retry int) (error, int) {
	return InvokeRestWithRetryWithAuth(baseURL, path, preservePathEncoding, "", "", nil, true, httpCommand, contentType, body, timeout, out, logger, num_retry)
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
	logger *log.CommonLogger, num_retry int) (error, int) {
	err, statusCode := QueryRestApiWithAuth(baseURL,
		path, preservePathEncoding, username,
		password, certificate,
		httpCommand,
		contentType,
		body,
		timeout,
		out,
		logger)
	if err != nil {
		remain_retries := num_retry - 1
		if remain_retries < 0 {
			return err, statusCode
		} else {
			return InvokeRestWithRetryWithAuth(baseURL, path, preservePathEncoding, username, password, certificate, insecureSkipVerify, httpCommand, contentType, body, timeout, out, logger, remain_retries)
		}
	}
	return err, statusCode

}

func getHttpClient(certificate []byte, ssl_con_str string, logger *log.CommonLogger) (*http.Client, error) {
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

		tr := &http.Transport{TLSClientConfig: tlsConfig}
		tr.DisableKeepAlives = true
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
	var req *http.Request

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
