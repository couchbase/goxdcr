package utils

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/cbauth"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbaselabs/go-couchbase"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

//errors
var ErrorRetrievingSSLPort = errors.New("Could not get ssl port of remote cluster.")
var ErrorRetrievingCouchApiBase = errors.New("Could not get couchApiBase in the response of /nodes/self.")

//// TODO incorporate cbauth
//func SendHttpRequest(request *http.Request) (*http.Response, error) {
//	return http.DefaultClient.Do(request)
//}
//
//// TODO incorporate cbauth
//func SendHttpRequestThroughSSL(request *http.Request, certificate []byte) (*http.Response, error) {
//	caPool := x509.NewCertPool()
//	ok := caPool.AppendCertsFromPEM(certificate)
//	if !ok {
//		return nil, errors.New("Invalid certificate")
//	}
//
//	tlsConfig := &tls.Config{
//		RootCAs: caPool,
//	}
//	tlsConfig.BuildNameToCertificate()
//
//	tr := &http.Transport{
//		TLSClientConfig: tlsConfig,
//	}
//	client := &http.Client{Transport: tr}
//	return client.Do(request)
//}

const (
	ContentType        = "Content-Type"
	DefaultContentType = "application/x-www-form-urlencoded"
	JsonContentType    = "application/json"
)

func GetXDCRSSLPort(hostName, userName, password string, logger *log.CommonLogger) (uint16, error) {

	portsInfo := make(map[string]interface{})
	QueryRestApiWithAuth(hostName, base.SSLPortsPath, userName, password, base.MethodGet, "", nil, &portsInfo, logger, nil)
	// get ssl port from the map
	sslPort, ok := portsInfo[base.SSLPortKey]
	if !ok {
		// should never get here
		return 0, ErrorRetrievingSSLPort
	}

	sslPortFloat, ok := sslPort.(float64)
	if !ok {
		// should never get here
		return 0, errors.New(fmt.Sprintf("ssl port of remote cluster is of wrong type. Expected type: float64; Actual type: %s", reflect.TypeOf(sslPort)))
	}
	return uint16(sslPortFloat), nil
}

func CouchApiBase(hostName, userName, password string, logger *log.CommonLogger, bSSL bool) (string, error) {
	nodeInfo := make(map[string]interface{})
	err, _ := QueryRestApiWithAuth(hostName, base.NodesSelfPath, userName, password, base.MethodGet, "", nil, &nodeInfo, logger, nil)
	if err != nil {
		return "", err
	}

	var attrName string
	if bSSL {
		attrName = base.CouchApiBaseHttps
	} else {
		attrName = base.CouchApiBase
	}
	logger.Infof("nodeInfo=%v\n", nodeInfo)
	logger.Infof("attrName=%v\n", attrName)
	couchApiBase, ok := nodeInfo[attrName]
	if !ok {
		return "", ErrorRetrievingCouchApiBase
	}

	return couchApiBase.(string), nil
}

//convenient api for rest calls to local cluster
func QueryRestApi(baseURL string,
	path string,
	httpCommand string,
	contentType string,
	body []byte,
	out interface{},
	logger *log.CommonLogger, certificate []byte) (error, int) {
	return QueryRestApiWithAuth(baseURL, path, "", "", httpCommand, contentType, body, out, logger, certificate)
}

func EnforcePrefix(prefix string, str string) string {
	var ret_str string = str
	if !strings.HasPrefix(str, prefix) {
		ret_str = prefix + str
	}
	return ret_str
}

//this expect the baseURL doesn't contain username and password
//if username and password passed in is "", assume it is local rest call,
//then call cbauth to add authenticate information
func QueryRestApiWithAuth(
	baseURL string,
	path string,
	username string,
	password string,
	httpCommand string,
	contentType string,
	body []byte,
	out interface{},
	logger *log.CommonLogger, certificate []byte) (error, int) {
	var baseURL_new string

	//process the URL
	if certificate == nil {
		baseURL_new = EnforcePrefix("http://", baseURL)
	} else {
		baseURL_new = EnforcePrefix("https://", baseURL)
	}
	u, err := couchbase.ParseURL(baseURL_new)
	if err != nil {
		return err, 0
	}

	var l *log.CommonLogger = loggerForFunc(logger)

	if username != "" {
		u.User = url.UserPassword(username, password)
	}
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	req, err := http.NewRequest(httpCommand, u.String(), bytes.NewBuffer(body))
	if err != nil {
		return err, 0
	}
	if contentType == "" {
		contentType = DefaultContentType
	}
	req.Header.Set(ContentType, contentType)

	//TODO: log request would log password barely
	l.Debugf("req=%v\n", req)

	//if username is nil, assume it is local rest call
	if username == "" {
		err := cbauth.SetRequestAuth(req)
		if err != nil {
			l.Errorf("Failed to set authentication to request, req=%v\n", req)
			return err, 0
		}
	}

	client, err := getHttpClient(certificate)

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
	httpCommand string,
	contentType string,
	body []byte,
	out interface{},
	logger *log.CommonLogger, certificate []byte, num_retry int) (error, int) {
	return InvokeRestWithRetryWithAuth(baseURL, path, "", "", httpCommand, contentType, body, out, logger, certificate, num_retry)
}

func InvokeRestWithRetryWithAuth(baseURL string,
	path string,
	username string,
	password string,
	httpCommand string,
	contentType string,
	body []byte,
	out interface{},
	logger *log.CommonLogger, certificate []byte, num_retry int) (error, int) {
	err, statusCode := QueryRestApiWithAuth(baseURL,
		path, username,
		password,
		httpCommand,
		contentType,
		body,
		out,
		logger, certificate)
	if err != nil {
		if certificate != nil {
			//got https error, no need to retry
			return err, statusCode
		} else {
			remain_retries := num_retry - 1
			if remain_retries < 0 {
				return err, statusCode
			} else {
				return InvokeRestWithRetryWithAuth(baseURL, path, username, password, httpCommand, contentType, body, out, logger, certificate, remain_retries)
			}
		}
	}
	return err, statusCode

}

func getHttpClient(certificate []byte) (*http.Client, error) {
	var client *http.Client
	if certificate != nil {
		//https
		caPool := x509.NewCertPool()
		ok := caPool.AppendCertsFromPEM(certificate)
		if !ok {
			return nil, errors.New("Invalid certificate")
		}

		tlsConfig := &tls.Config{RootCAs: caPool}
		tlsConfig.BuildNameToCertificate()
		tr := &http.Transport{TLSClientConfig: tlsConfig}
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
