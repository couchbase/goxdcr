package utils

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/couchbaselabs/go-couchbase"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"bytes"
	"bufio"
	"strconv"
	"code.google.com/p/goprotobuf/proto"
)

type BucketBasicStats struct {
	ItemCount int `json:"itemCount"`
}
type CouchBucket struct {
	Name string           `json:"name"`
	Stat BucketBasicStats `json:"basicStats"`
}

var logger_utils *log.CommonLogger = log.NewLogger("Utils", log.DefaultLoggerContext)
var MaxIdleConnsPerHost = 256
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func loggerForFunc(logger *log.CommonLogger) *log.CommonLogger {
	var l *log.CommonLogger
	if logger != nil {
		l = logger
	} else {
		l = logger_utils
	}
	return l
}

func ValidateSettings(defs base.SettingDefinitions,
	settings map[string]interface{},
	logger *log.CommonLogger) error {
	var l *log.CommonLogger = loggerForFunc(logger)

	l.Infof("Start validate setting=%v, defs=%v", settings, defs)
	var err *base.SettingsError = nil
	for key, def := range defs {
		val, ok := settings[key]
		if !ok && def.Required {
			if err == nil {
				err = base.NewSettingsError()
			}
			err.Add(key, errors.New("required, but not supplied"))
		} else {
			if val != nil && def.Data_type != reflect.PtrTo(reflect.TypeOf(val)) {
				if err == nil {
					err = base.NewSettingsError()
				}
				err.Add(key, errors.New(fmt.Sprintf("expected type is %v, supplied type is %v",
					def.Data_type, reflect.TypeOf(val))))
			}
		}
	}
	if err != nil {
		l.Infof("setting validation result = %v", *err)
		return *err
	}
	return nil
}

func RecoverPanic(err *error) {
	if r := recover(); r != nil {
		*err = errors.New(fmt.Sprint(r))
	}
}

func QueryRestAPI(
	baseURL *url.URL,
	path string,
	username string,
	password string,
	httpCommand string,
	out interface{},
	logger *log.CommonLogger) error {

	var l *log.CommonLogger = loggerForFunc(logger)

	u := *baseURL
	if username != "" {
		u.User = url.UserPassword(username, password)
	}
	if q := strings.Index(path, "?"); q > 0 {
		u.Path = path[:q]
		u.RawQuery = path[q+1:]
	} else {
		u.Path = path
	}

	req, err := http.NewRequest(httpCommand, u.String(), nil)
	if err != nil {
		return err
	}
	//	maybeAddAuth(req, username, password)

	l.Infof("req=%v\n", req)

	res, err := HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("HTTP error %v getting %q: %s",
			res.Status, u.String(), bod)
	}

	if out != nil {
		l.Infof("rest response=%v\n", res.Body)
		d := json.NewDecoder(res.Body)
		if err = d.Decode(&out); err != nil {
			return err
		}
	} else {
		l.Info("out is nil")
	}
	return nil
}

func maybeAddAuth(req *http.Request, username string, password string) {
	if username != "" && password != "" {
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	}
}

func Bucket(connectStr string, bucketName string, clusterUserName, clusterPassword string) (*couchbase.Bucket, error) {
	url := fmt.Sprintf("http://%s:%s@%s", clusterUserName, clusterPassword, connectStr)
	bucketInfos, err := couchbase.GetBucketList (url)
	if err != nil {
		return nil, NewEnhancedError("Error getting bucketlist with url:" + url, err)
	}

	var password string
	for _, bucketInfo := range bucketInfos {
		if bucketInfo.Name == bucketName {
			password = bucketInfo.Password
		}
	}
	couch, err := couchbase.Connect("http://" + bucketName + ":" + password + "@" + connectStr)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. bucketName=%v; password=%v; connectStr=%v", bucketName, password, connectStr), err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, NewEnhancedError("Error getting pool with name 'default'.", err)
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error getting bucket, %v, from pool.", bucketName), err)
	}
	return bucket, err
}

// encode http request into a byte array
func EncodeHttpRequestIntoByteArray(r *http.Request) ([]byte, error) {
	buffer := new(bytes.Buffer)
	err := r.Write(buffer)
	if err == nil {
		return buffer.Bytes(), nil
	} else {
		return nil, err
	}
}

// decode http request from byte array
func DecodeHttpRequestFromByteArray(data []byte) (*http.Request, error) {
	buffer := bytes.NewBuffer(data)
	return http.ReadRequest(bufio.NewReader(buffer))
}

// given a http request encoded as a byte array, extract request body and
// decode a message from it
// this method should be used for messages with static paths, where message
// content depends on only request body and nothing else
func DecodeMessageFromByteArray(data []byte, msg proto.Message, logger *log.CommonLogger) error {
	var l *log.CommonLogger = loggerForFunc(logger)

	request, err := DecodeHttpRequestFromByteArray(data)
	l.Infof("decoded http %v", request)
	if err != nil {
		return err
	}

	return DecodeMessageFromHttpRequest(request, msg, logger)
}

// given a http request, extract requesy body and
// decode a message from it
func DecodeMessageFromHttpRequest(request *http.Request, msg proto.Message, logger *log.CommonLogger) error {
	requestBody := make([]byte, request.ContentLength)
	err := RequestRead(request.Body, requestBody)
	if err != nil {
		return err
	}
	err = proto.Unmarshal(requestBody, msg)
	return err
}

// TODO may expose and reuse the same func in si
func RequestRead(r io.Reader, data []byte) (err error) {
	var c int

	n, start := len(data), 0
	for n > 0 && err == nil {
		// Per http://golang.org/pkg/io/#Reader, it is valid for Read to
		// return EOF with non-zero number of bytes at the end of the
		// input stream
		c, err = r.Read(data[start:])
		n -= c
		start += c
	}
	if n == 0 {
		return nil
	}
	return err
}

func InvalidParameterInHttpRequestError(param string) error {
	return errors.New(fmt.Sprintf("Invalid parameter, %v, in http request.", param))
}

func InvalidValueInHttpRequestError(param string, val interface{}) error {
	return errors.New(fmt.Sprintf("Invalid value, %v, for parameter, %v, in http request.", val, param))
}

func IncorrectValueTypeInHttpRequestError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for key, %v, in http request has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func InvalidPathInHttpRequestError(path string) error {
	return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", path))
}

func WrapError (err error) map[string]interface{} {
	infos := make (map[string]interface{})
	infos ["error"] = err
	return infos
}

func UnwrapError (infos map[string]interface{}) (err error) {
	if infos != nil && len(infos) > 0 {
		err = infos ["error"].(error)
	}
	return err
}

func MissingParametersInHttpRequestError(params []string) error {
	return errors.New(fmt.Sprintf("Parameters, %v, are missing in http request.", params))
}

func MissingReplicationIdInHttpRequestError(path string) error {
	return errors.New(fmt.Sprintf("Replication id is missing from request url, %v.", path))
}

func MissingParameterInHttpResponseError(param string) error {
	return errors.New(fmt.Sprintf("Parameter, %v, is missing in http response.", param))
}

func IncorrectValueTypeInHttpResponseError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for key, %v, in http response has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func IncorrectValueTypeInMapError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, with key, %v, in map has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

// returns an enhanced error with erroe message being "msg + old error message"
func NewEnhancedError (msg string, err error) error {
	return errors.New(msg + "\n" + err.Error())
}

// return host address in the form of hostName:port
func GetHostAddr(hostName string, port int) string {
	return hostName + base.UrlPortNumberDelimiter + strconv.FormatInt(int64(port), base.ParseIntBase)
}

