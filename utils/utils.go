package utils

import (
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/simple_utils"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
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

var NonExistentBucketError error = errors.New("Bucket doesn't exist")

var logger_utils *log.CommonLogger = log.NewLogger("Utils", log.DefaultLoggerContext)

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

func LocalPool(localConnectStr string) (couchbase.Pool, error) {
	localURL := fmt.Sprintf("http://%s", localConnectStr)
	client, err := couchbase.ConnectWithAuth(localURL, cbauth.NewAuthHandler(nil))
	if err != nil {
		return couchbase.Pool{}, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. url=%v", UrlForLog(localURL)), err)
	}
	return client.GetPool("default")
}

func RemotePool(remoteConnectStr string, remoteUsername string, remotePassword string) (couchbase.Pool, error) {
	remoteURL := fmt.Sprintf("http://%s:%s@%s", url.QueryEscape(remoteUsername), url.QueryEscape(remotePassword), remoteConnectStr)
	return RemotePoolWithTimeout(remoteURL)
}

// call to remoteURL may never return, e.g., in case of network parition,
// wrap it with timeout to ensure that it will not block forever
func RemotePoolWithTimeout(remoteURL string) (couchbase.Pool, error) {
	poolObj, err := simple_utils.ExecWithTimeout2(remotePool, remoteURL, base.DefaultHttpTimeout, logger_utils)
	if poolObj != nil {
		return poolObj.(couchbase.Pool), err
	} else {
		var pool couchbase.Pool
		return pool, err
	}
}

// an auxiliary function that conforms to simple_utils.action2 interface
// so that it can be called by simple_utils.ExecWithTimeout2
func remotePool(remoteURLObj interface{}) (interface{}, error) {
	remoteURL := remoteURLObj.(string)
	client, err := couchbase.Connect(remoteURL)
	if err != nil {
		return couchbase.Pool{}, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. url=%v", UrlForLog(remoteURL)), err)
	}
	return client.GetPool("default")
}

// call to remoteURL may never return, e.g., in case of network parition,
// wrap it with timeout to ensure that it will not block forever
func RemoteBucketList(remoteURL string) ([]couchbase.BucketInfo, error) {
	bucketInfosObj, err := simple_utils.ExecWithTimeout2(remoteBucketList, remoteURL, base.DefaultHttpTimeout, logger_utils)
	if bucketInfosObj != nil {
		return bucketInfosObj.([]couchbase.BucketInfo), err
	} else {
		return nil, err
	}
}

// an auxiliary function that conforms to simple_utils.action2 interface
// so that it can be called by simple_utils.ExecWithTimeout2
func remoteBucketList(remoteURLObj interface{}) (interface{}, error) {
	remoteURL := remoteURLObj.(string)
	return couchbase.GetBucketList(remoteURL)
}

// Get bucket in local cluster
func LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error) {
	logger_utils.Debugf("Getting local bucket name=%v\n", bucketName)

	pool, err := LocalPool(localConnectStr)
	if err != nil {
		return nil, err
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

	if remoteUsername == "" || remotePassword == "" {
		return nil, errors.New(fmt.Sprintf("Error retrieving remote bucket, %v, since remote username and/or password are missing.", bucketName))

	}

	remoteURL := fmt.Sprintf("http://%s:%s@%s", url.QueryEscape(remoteUsername), url.QueryEscape(remotePassword), remoteConnectStr)
	bucketInfos, err := RemoteBucketList(remoteURL)
	if err != nil {
		return nil, NewEnhancedError("Error getting bucketlist with url:"+UrlForLog(remoteURL), err)
	}

	var password string
	for _, bucketInfo := range bucketInfos {
		if bucketInfo.Name == bucketName {
			password = bucketInfo.Password
		}
	}
	client, err := couchbase.Connect("http://" + url.QueryEscape(remoteUsername) + ":" + url.QueryEscape(remotePassword) + "@" + remoteConnectStr)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. bucketName=%v; remoteConnectStr=%v", bucketName, remoteConnectStr), err)
	}
	pool, err := client.GetPool("default")
	if err != nil {
		return nil, NewEnhancedError("Error getting pool with name 'default'.", err)
	}
	bucket, err := pool.GetBucketWithAuth(bucketName, bucketName, password)
	if err != nil {
		return nil, NewEnhancedError(fmt.Sprintf("Error getting bucket, %v, from pool.", bucketName), err)
	}

	logger_utils.Debugf("Got remote bucket successfully name=%v\n", bucket.Name)
	return bucket, err
}

func UnwrapError(infos map[string]interface{}) (err error) {
	if infos != nil && len(infos) > 0 {
		err = infos["error"].(error)
	}
	return err
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

func GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]interface{} {
	regMap := make(map[string]interface{})

	expvarMap.Do(func(keyValue expvar.KeyValue) {
		valueStr := keyValue.Value.String()
		// first check if valueStr is an integer
		valueInt, err := strconv.Atoi(valueStr)
		if err == nil {
			regMap[keyValue.Key] = valueInt
		} else {
			// then check if valueStr is a float
			valueFloat, err := strconv.ParseFloat(valueStr, 64)
			if err == nil {
				regMap[keyValue.Key] = valueFloat
			} else {
				// should never happen
				logger_utils.Errorf("Invalid value in expvarMap. Only float and integer values are supported")
			}
		}
	})
	return regMap
}

//convert the format returned by go-memcached StatMap - map[string]string to map[uint16]uint64
func ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) error {

	for _, vbno := range vbnos {
		stats_key := fmt.Sprintf(base.VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT, vbno)
		highseqnostr, ok := stats_map[stats_key]
		if !ok {
			logger_utils.Infof("Can't find high seqno for vbno=%v in stats map. Source topology may have changed.\n", vbno)
			continue
		}
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
			return nil, simple_utils.IncorrectValueTypeInMapError(key, val, "string/int/bool/LogLevel/[]byte")
		}
		params.Add(key, strVal)
	}

	return []byte(params.Encode()), nil
}

func UrlForLog(urlStr string) string {
	result, err := url.Parse(urlStr)
	if err == nil {
		if result.User != nil {
			result.User = url.UserPassword(result.User.Username(), "xxxx")
		}
		return result.String()
	} else {
		return urlStr
	}
}

func GetMatchedKeys(expression string, keys []string) (map[string][][]int, error) {
	logger_utils.Infof("GetMatchedKeys expression=%v, expression in bytes=%v\n", expression, []byte(expression))
	if !utf8.ValidString(expression) {
		return nil, errors.New("expression is not valid utf8")
	}
	for _, key := range keys {
		logger_utils.Infof("key=%v, key_bytes=%v\n", key, []byte(key))
		if !utf8.ValidString(key) {
			return nil, errors.New("key is not valid utf8")
		}
	}

	regExp, err := regexp.Compile(expression)
	if err != nil {
		return nil, err
	}

	matchesMap := make(map[string][][]int)

	for _, key := range keys {
		var matches [][]int
		if RegexpMatch(regExp, []byte(key)) {
			matches = regExp.FindAllStringIndex(key, -1)
		} else {
			matches = make([][]int, 0)
		}
		logger_utils.Debugf("key=%v, matches with byte index=%v\n", key, matches)
		convertedMatches, err := convertByteIndexToRuneIndex(key, matches)
		if err != nil {
			return nil, err
		}
		matchesMap[key] = convertedMatches
	}

	return matchesMap, nil
}

func RegexpMatch(regExp *regexp.Regexp, key []byte) bool {
	return regExp.Match(key)
}

// given a matches map, convert the indices from byte index to rune index
func convertByteIndexToRuneIndex(key string, matches [][]int) ([][]int, error) {
	convertedMatches := make([][]int, 0)
	if len(key) == 0 || len(matches) == 0 {
		return matches, nil
	}

	// parse key and build a byte index to rune index map
	indexMap := make(map[int]int)
	byteIndex := 0
	runeIndex := 0
	keyBytes := []byte(key)
	keyLen := len(key)
	for {
		indexMap[byteIndex] = runeIndex
		if byteIndex < keyLen {
			_, runeLen := utf8.DecodeRune(keyBytes[byteIndex:])
			byteIndex += runeLen
			runeIndex++
		} else {
			break
		}
	}

	logger_utils.Debugf("key=%v, indexMap=%v\n", key, indexMap)

	var ok bool
	for _, match := range matches {
		convertedMatch := make([]int, 2)
		convertedMatch[0], ok = indexMap[match[0]]
		if !ok {
			// should not happen
			errMsg := InvalidRuneIndexErrorMessage(key, match[0])
			logger_utils.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		convertedMatch[1], ok = indexMap[match[1]]
		if !ok {
			// should not happen
			errMsg := InvalidRuneIndexErrorMessage(key, match[1])
			logger_utils.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		convertedMatches = append(convertedMatches, convertedMatch)
	}

	return convertedMatches, nil
}

func InvalidRuneIndexErrorMessage(key string, index int) string {
	return fmt.Sprintf("byte index, %v, in match for key, %v, is not a starting index for a rune", index, key)
}

func LocalBucketUUID(local_connStr string, bucketName string) (string, error) {
	local_default_pool, err := LocalPool(local_connStr)
	if err != nil {
		return "", err
	}
	bucket, ok := local_default_pool.BucketMap[bucketName]
	if !ok {
		return "", NonExistentBucketError
	}
	return bucket.UUID, nil
}

func RemoteBucketUUID(remote_connStr, remote_userName, remote_password, bucketName string) (string, error) {
	remote_default_pool, err := RemotePool(remote_connStr, remote_userName, remote_password)
	if err != nil {
		return "", err
	}
	bucket, ok := remote_default_pool.BucketMap[bucketName]
	if !ok {
		return "", NonExistentBucketError
	}
	return bucket.UUID, nil

}

func ReplicationStatusNotFoundError(topic string) error {
	return fmt.Errorf("Cannot find replication status for topic %v", topic)
}

func BucketNotFoundError(bucketName string) error {
	return fmt.Errorf("Bucket `%v` not found.", bucketName)
}

func GetMemcachedConnection(serverAddr, bucketName string, logger *log.CommonLogger) (*mcc.Client, error) {

	if serverAddr == "" {
		panic("serverAddr is empty")
	}
	username, password, err := cbauth.GetMemcachedServiceAuth(serverAddr)
	logger.Debugf("memcached auth: username=%v, password=%v, err=%v\n", username, password, err)
	if err != nil {
		return nil, err
	}

	conn, err := base.NewConn(serverAddr, username, password)
	if err != nil {
		return nil, err
	}

	_, err = conn.SelectBucket(bucketName)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func GetIntSettingFromSettings(settings map[string]interface{}, settingName string) (int, error) {
	settingObj := GetSettingFromSettings(settings, settingName)
	if settingObj == nil {
		return -1, nil
	}

	setting, ok := settingObj.(int)
	if !ok {
		return -1, fmt.Errorf("Setting %v is of wrong type", settingName)
	}

	return setting, nil
}

func GetStringSettingFromSettings(settings map[string]interface{}, settingName string) (string, error) {
	settingObj := GetSettingFromSettings(settings, settingName)
	if settingObj == nil {
		return "", nil
	}

	setting, ok := settingObj.(string)
	if !ok {
		return "", fmt.Errorf("Setting %v is of wrong type", settingName)
	}

	return setting, nil
}

func GetSettingFromSettings(settings map[string]interface{}, settingName string) interface{} {
	if settings == nil {
		return nil
	}

	setting, ok := settings[settingName]
	if !ok {
		return nil
	}

	return setting
}

func GetMemcachedClient(serverAddr, bucketName string, kv_mem_clients map[string]*mcc.Client, logger *log.CommonLogger) (*mcc.Client, error) {
	client, ok := kv_mem_clients[serverAddr]
	if ok {
		return client, nil
	} else {
		if bucketName == "" {
			panic("unexpected empty bucketName")
		}

		var client, err = GetMemcachedConnection(serverAddr, bucketName, logger)
		if err == nil {
			kv_mem_clients[serverAddr] = client
			return client, nil
		} else {
			return nil, err
		}
	}
}
