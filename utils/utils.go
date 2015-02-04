package utils

import (
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/cbauth"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbaselabs/go-couchbase"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
)

type BucketBasicStats struct {
	ItemCount int `json:"itemCount"`
}

//Only used by unit test
//TODO: replace with go-couchbase bucket stats API
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

	l.Debugf("Start validate setting=%v, defs=%v", settings, defs)
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

// Get bucket in local cluster
func LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error) {
	logger_utils.Debugf("Getting local bucket name=%v\n", bucketName)

	url := fmt.Sprintf("http://%s", localConnectStr)
	client, err := couchbase.ConnectWithAuth(url, cbauth.NewAuthHandler(nil))
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. url=%v", url), err)
	}
	pool, err := client.GetPool("default")
	if err != nil {
		return nil, NewEnhancedError("Error getting pool with name 'default'.", err)
	}
	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error getting bucket, %v, from pool.", bucketName), err)
	}

	logger_utils.Debugf("Got local bucket successfully name=%v\n", bucket.Name)
	return bucket, err
}

func RemoteBucket(remoteConnectStr, bucketName, remoteUsername, remotePassword string) (*couchbase.Bucket, error) {
	logger_utils.Debugf("Getting remote bucket name=%v connstr=%v\n", bucketName, remoteConnectStr)

	var url string
	if remoteUsername == "" || remotePassword == "" {
		return nil, errors.New(fmt.Sprintf("Error retrieving remote bucket, %v, since remote username and/or password are missing.", bucketName))

	}

	url = fmt.Sprintf("http://%s:%s@%s", remoteUsername, remotePassword, remoteConnectStr)
	bucketInfos, err := couchbase.GetBucketList(url)
	if err != nil {
		return nil, NewEnhancedError("Error getting bucketlist with url:"+url, err)
	}

	var password string
	for _, bucketInfo := range bucketInfos {
		if bucketInfo.Name == bucketName {
			password = bucketInfo.Password
		}
	}
	couch, err := couchbase.Connect("http://" + bucketName + ":" + password + "@" + remoteConnectStr)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. bucketName=%v; password=%v; remoteConnectStr=%v", bucketName, password, remoteConnectStr), err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		return nil, NewEnhancedError("Error getting pool with name 'default'.", err)
	}
	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error getting bucket, %v, from pool.", bucketName), err)
	}

	logger_utils.Debugf("Got remote bucket successfully name=%v\n", bucket.Name)
	return bucket, err
}

func IncorrectValueTypeInHttpRequestError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for key, %v, in http request has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func IncorrectValueTypeError(expectedType string) error {
	return errors.New(fmt.Sprintf("The value must be %v", expectedType))
}

func InvalidValueError(expectedType string, minVal, maxVal interface{}) error {
	return errors.New(fmt.Sprintf("The value must be %v between %v and %v", expectedType, minVal, maxVal))
}

func InvalidPathInHttpRequestError(path string) error {
	return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", path))
}

func WrapError(err error) map[string]interface{} {
	infos := make(map[string]interface{})
	infos["error"] = err
	return infos
}

func UnwrapError(infos map[string]interface{}) (err error) {
	if infos != nil && len(infos) > 0 {
		err = infos["error"].(error)
	}
	return err
}

func MissingValueError(param string) error {
	return errors.New(fmt.Sprintf("%v cannot be empty", param))
}

func GenericInvalidValueError(param string) error {
	return errors.New(fmt.Sprintf("%v is invalid", param))
}

func MissingParameterError(param string) error {
	return errors.New(fmt.Sprintf("%v is missing", param))
}

func MissingParameterInHttpRequestUrlError(paramName, path string) error {
	return errors.New(fmt.Sprintf("%v is missing from request url, %v.", paramName, path))
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
func NewEnhancedError(msg string, err error) error {
	return errors.New(msg + "\n err = " + err.Error())
}

// return host address in the form of hostName:port
func GetHostAddr(hostName string, port uint16) string {
	return hostName + base.UrlPortNumberDelimiter + strconv.FormatInt(int64(port), base.ParseIntBase)
}

// extract host name from hostAddr, which is in the form of hostName:port
func GetHostName(hostAddr string) string {
	return strings.Split(hostAddr, base.UrlPortNumberDelimiter)[0]
}

func GetPortNumber(hostAddr string) (uint16, error) {
	port_str := strings.Split(hostAddr, base.UrlPortNumberDelimiter)[1]
	port, err := strconv.ParseUint(port_str, 10, 16)
	if err == nil {
		return uint16(port), nil
	} else {
		return 0, err
	}
}

func GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]string {
	regMap := make(map[string]string)

	expvarMap.Do(func(keyValue expvar.KeyValue) {
		regMap[keyValue.Key] = keyValue.Value.String()
		return
	})
	return regMap
}

func Contains(value interface{}, slice []interface{}) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

//convert the format returned by go-memcached StatMap - map[string]string to map[uint16]uint64
func ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) error {

	for _, vbno := range vbnos {
		stats_key := fmt.Sprintf(base.VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT, vbno)
		highseqnostr := stats_map[stats_key]
		highseqno, err := strconv.ParseUint(highseqnostr, 10, 64)
		if err != nil {
			return err
		}
		highseqno_map[vbno] = highseqno
	}

	return nil
}

// encode data in a map into a byte array, which can then be used as
// the body part of a http request
// so far only five types are supported: string, int, bool, LogLevel, []byte
// which should be sufficient for all cases at hand
func EncodeMapIntoByteArray(data map[string]interface{}) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	params := make(url.Values)
	for key, val := range data {
		var strVal string
		switch val.(type) {
		case string:
			strVal = val.(string)
		case int:
			strVal = strconv.FormatInt(int64(val.(int)), base.ParseIntBase)
		case bool:
			strVal = strconv.FormatBool(val.(bool))
		case log.LogLevel:
			strVal = val.(log.LogLevel).String()
		case []byte:
			strVal = string(val.([]byte))
		default:
			return nil, IncorrectValueTypeInMapError(key, val, "string/int/bool/LogLevel/[]byte")
		}
		params.Add(key, strVal)
	}

	return []byte(params.Encode()), nil
}
