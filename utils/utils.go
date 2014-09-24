package utils

import (
	"bufio"
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/couchbaselabs/go-couchbase"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

type BucketBasicStats struct {
	ItemCount int `json:"itemCount"`
}
type CouchBucket struct {
	Name string           `json:"name"`
	Stat BucketBasicStats `json:"basicStats"`
}

var logger_utils *log.CommonLogger = log.NewLogger("Utils", log.LogLevelInfo)
var MaxIdleConnsPerHost = 256
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func ValidateSettings(defs base.SettingDefinitions, settings map[string]interface{}) error {
	logger_utils.Infof("Start validate setting=%v, defs=%v", settings, defs)
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
		logger_utils.Infof("setting validation result = %v", *err)
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
	out interface{}) error {
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

	logger_utils.Infof("req=%v\n", req)

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
		logger_utils.Infof("rest response=%v\n", res.Body)
		d := json.NewDecoder(res.Body)
		if err = d.Decode(&out); err != nil {
			return err
		}
	} else {
		logger_utils.Info("out is nil")
	}
	return nil
}

func maybeAddAuth(req *http.Request, username string, password string) {
	if username != "" && password != "" {
		req.Header.Set("Authorization", "Basic "+
			base64.StdEncoding.EncodeToString([]byte(username+":"+password)))
	}
}

func Bucket(connectStr string, bucketName string) (*couchbase.Bucket, error) {
	couch, err := couchbase.Connect("http://" + connectStr)
	if err != nil {
		return nil, err
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, err
	}
	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, err
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
func DecodeMessageFromByteArray(data []byte, msg proto.Message) error {
	request, err := DecodeHttpRequestFromByteArray(data)
	logger_utils.Infof("decoded http %v", request)
	if err != nil {
		return err
	}

	return DecodeMessageFromHttpRequest(request, msg)
}

// given a http request, extract requesy body and
// decode a message from it
func DecodeMessageFromHttpRequest(request *http.Request, msg proto.Message) error {
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

func InvalidValueInHttpRequestError(param, val string) error {
	return errors.New(fmt.Sprintf("Invalid value, %v, for parameter, %v, in http request.", val, param))
}

func InvalidPathInHttpRequestError(path string) error {
	return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", path))
}
