/*
Copyright 2014-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goutils/scramsha"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbaselabs/gojsonsm"
)

var NonExistentBucketError error = errors.New("Bucket doesn't exist")
var BucketRecreatedError error = errors.New("Bucket has been deleted and recreated")
var TargetMayNotSupportScramShaError error = errors.New("Target may not support ScramSha")

func (f *HELOFeatures) NumberOfActivatedFeatures() int {
	var result int
	if f.Xattribute {
		result++
	}
	if f.CompressionType != base.CompressionTypeNone {
		result++
	}
	if f.Xerror {
		result++
	}
	return result
}

type Utilities struct {
	*filter.FilterUtilsImpl
	logger_utils *log.CommonLogger
}

/**
 * NOTE - ideally we want to be able to pass in utility interfaces so we can do much
 * better unit testing with mocks. This constructor should be used in main() and then
 * passed down level by levels.
 * Currently, this method is being called in many places, and each place that is using
 * this method should ideally be using a passed in interface from another parent level.
 */
func NewUtilities() *Utilities {
	retVar := &Utilities{
		logger_utils: log.NewLogger(base.UtilsKey, log.GetOrCreateContext(base.UtilsKey)),
	}
	retVar.FilterUtilsImpl = &filter.FilterUtilsImpl{}
	return retVar
}

/**
 * This utils file contains both regular non-REST related utilities as well as REST related.
 * The first section is non-REST related utility functions
 */

type BucketBasicStats struct {
	ItemCount int `json:"itemCount"`
}

// Only used by unit test
// TODO: replace with go-couchbase bucket stats API
type CouchBucket struct {
	Name string           `json:"name"`
	Stat BucketBasicStats `json:"basicStats"`
}

func (u *Utilities) GetNonExistentBucketError() error {
	return NonExistentBucketError
}

func (u *Utilities) GetBucketRecreatedError() error {
	return BucketRecreatedError
}

func (u *Utilities) loggerForFunc(logger *log.CommonLogger) *log.CommonLogger {
	var l *log.CommonLogger
	if logger != nil {
		l = logger
	} else {
		l = u.logger_utils
	}
	return l
}

func (u *Utilities) ValidateSettings(defs base.SettingDefinitions,
	settings metadata.ReplicationSettingsMap,
	logger *log.CommonLogger) error {
	var l *log.CommonLogger = u.loggerForFunc(logger)

	if l.GetLogLevel() >= log.LogLevelDebug {
		l.Debugf("Start validate setting=%v, defs=%v", settings.CloneAndRedact(), defs)
	}
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

func (u *Utilities) RecoverPanic(err *error) {
	if r := recover(); r != nil {
		*err = errors.New(fmt.Sprint(r))
	}
}

func (u *Utilities) LocalPool(localConnectStr string) (couchbase.Pool, error) {
	localURL := fmt.Sprintf("http://%s", localConnectStr)
	client, err := couchbase.ConnectWithAuth(localURL, cbauth.NewAuthHandler(nil))
	if err != nil {
		return couchbase.Pool{}, u.NewEnhancedError(fmt.Sprintf("Error connecting to couchbase. url=%v", u.UrlForLog(localURL)), err)
	}
	return client.GetPool("default")
}

// Get bucket in local cluster
func (u *Utilities) LocalBucket(localConnectStr, bucketName string) (*couchbase.Bucket, error) {
	u.logger_utils.Debugf("Getting local bucket name=%v\n", bucketName)

	pool, err := u.LocalPool(localConnectStr)
	if err != nil {
		return nil, err
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, u.NewEnhancedError(fmt.Sprintf("Error getting bucket, %v, from pool.", bucketName), err)
	}

	u.logger_utils.Debugf("Got local bucket successfully name=%v\n", bucket.Name)
	return bucket, err
}

func (u *Utilities) UnwrapError(infos map[string]interface{}) (err error) {
	if infos != nil && len(infos) > 0 {
		err = infos["error"].(error)
	}
	return err
}

// returns an enhanced error with erroe message being "msg + old error message"
func (u *Utilities) NewEnhancedError(msg string, err error) error {
	return errors.New(msg + "\n err = " + err.Error())
}

func (u *Utilities) GetMapFromExpvarMap(expvarMap *expvar.Map) map[string]interface{} {
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
				u.logger_utils.Errorf("Invalid value in expvarMap. Only float and integer values are supported")
			}
		}
	})
	return regMap
}

// convert the format returned by go-memcached StatMap - map[string]string to map[uint16]uint64
// Returns a list of vbnos that was not able to parsed. If all vbnos were not parsed, then return an error instead
func (u *Utilities) ParseHighSeqnoStat(vbnos []uint16, stats_map map[string]string, highseqno_map map[uint16]uint64) ([]uint16, error) {
	var unableToParseVBs []uint16
	if len(vbnos) == 0 {
		return nil, base.ErrorNoVbSpecified
	}

	for _, vbno := range vbnos {
		stats_key := base.ComposeVBHighSeqnoStatsKey(vbno)
		highseqnostr, ok := stats_map[stats_key]
		if !ok || highseqnostr == "" {
			unableToParseVBs = append(unableToParseVBs, vbno)
			continue
		}
		highseqno, parseIntErr := strconv.ParseUint(highseqnostr, 10, 64)
		if parseIntErr != nil {
			// Log error instead of because format errors seem to be more serious
			// So far hasn't been hit in testing anyway
			u.logger_utils.Errorf("high seqno for vbno=%v in stats map is not a valid uint64. high seqno=%v", vbno, highseqnostr)
			unableToParseVBs = append(unableToParseVBs, vbno)
			continue
		}
		highseqno_map[vbno] = highseqno
	}
	if len(unableToParseVBs) == len(vbnos) {
		return nil, fmt.Errorf("All Requested VBs %v were not able to be parsed from statsMap %v", vbnos, stats_map)
	} else if len(unableToParseVBs) > 0 {
		u.logger_utils.Warnf("(Requested VBs: %v) Can't find high seqno for vbnos=%v in stats map. Source topology may have changed", vbnos, unableToParseVBs)
	}
	return unableToParseVBs, nil
}

func (u *Utilities) ParseMaxCasStat(vbnos []uint16, statsMap map[string]string, maxCasStatsMap map[uint16]uint64) ([]uint16, error) {
	var unableToParseVBs []uint16
	if len(vbnos) == 0 {
		return nil, base.ErrorNoVbSpecified
	}

	for _, vbno := range vbnos {
		statsKey := base.ComposeVBMaxCasStatsKey(vbno)
		maxCasStr, ok := statsMap[statsKey]
		if !ok || maxCasStr == "" {
			unableToParseVBs = append(unableToParseVBs, vbno)
			continue
		}

		maxCas, parseIntErr := strconv.ParseUint(maxCasStr, 10, 64)
		if parseIntErr != nil {
			u.logger_utils.Errorf("maxCas stats for vbno=%v in stats map is not a valid uint64. maxCas=%v", vbno, maxCasStr)
			unableToParseVBs = append(unableToParseVBs, vbno)
		}
		maxCasStatsMap[vbno] = maxCas
	}
	if len(unableToParseVBs) == len(vbnos) {
		return nil, fmt.Errorf("All Requested VBs %v were not able to be parsed from statsMap %v", vbnos, statsMap)
	} else if len(unableToParseVBs) > 0 {
		u.logger_utils.Warnf("(Requested VBs: %v) Can't find maxCas for vbnos=%v in stats map. Source topology may have changed", vbnos, unableToParseVBs)
	}
	return unableToParseVBs, nil
}

// convert the format returned by go-memcached StatMap - map[string]string to map[uint16][]uint64
func (u *Utilities) ParseHighSeqnoAndVBUuidFromStats(vbnos []uint16, stats_map map[string]string, high_seqno_and_vbuuid_map map[uint16][]uint64) ([]uint16, map[uint16]string) {
	invalidVbnos := make([]uint16, 0)
	warnings := make(map[uint16]string)
	for _, vbno := range vbnos {
		high_seqno_stats_key := fmt.Sprintf(base.VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT, vbno)
		highseqnostr, ok := stats_map[high_seqno_stats_key]
		if !ok {
			invalidVbnos = append(invalidVbnos, vbno)
			if _, ok := warnings[vbno]; !ok {
				warnings[vbno] = fmt.Sprintf("Can't find high seqno for vbno=%v in stats map. Target topology may have changed.\n", vbno)
			}
			continue
		}
		high_seqno, err := strconv.ParseUint(highseqnostr, 10, 64)
		if err != nil {
			invalidVbnos = append(invalidVbnos, vbno)
			if _, ok := warnings[vbno]; !ok {
				warnings[vbno] = fmt.Sprintf("high seqno for vbno=%v in stats map is not a valid uint64. high seqno=%v\n", vbno, highseqnostr)
			}
			continue
		}

		vbuuid_stats_key := fmt.Sprintf(base.VBUCKET_UUID_STAT_KEY_FORMAT, vbno)
		vbuuidstr, ok := stats_map[vbuuid_stats_key]
		if !ok {
			invalidVbnos = append(invalidVbnos, vbno)
			if _, ok := warnings[vbno]; !ok {
				warnings[vbno] = fmt.Sprintf("Can't find vbuuid for vbno=%v in stats map. Target topology may have changed.\n", vbno)
			}
			continue
		}
		vbuuid, err := strconv.ParseUint(vbuuidstr, 10, 64)
		if err != nil {
			invalidVbnos = append(invalidVbnos, vbno)
			if _, ok := warnings[vbno]; !ok {
				warnings[vbno] = fmt.Sprintf("vbuuid for vbno=%v in stats map is not a valid uint64. vbuuid=%v\n", vbno, vbuuidstr)
			}
			continue
		}

		high_seqno_and_vbuuid_map[vbno] = []uint64{high_seqno, vbuuid}
	}

	return invalidVbnos, warnings
}

// encode data in a map into a byte array, which can then be used as
// the body part of a http request
// so far only five types are supported: string, int, bool, LogLevel, []byte
// which should be sufficient for all cases at hand
func (u *Utilities) EncodeMapIntoByteArray(data map[string]interface{}) ([]byte, error) {
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
			return nil, base.IncorrectValueTypeInMapError(key, val, "string/int/bool/LogLevel/[]byte")
		}
		params.Add(key, strVal)
	}

	return []byte(params.Encode()), nil
}

func (u *Utilities) UrlForLog(urlStr string) string {
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

// Called by UI to run a test on a specific document. This cannot be unit-tested as the authentication will fail
// Queries ns_server REST endpoint for a document content
// ns_server returns the document in a special json format, so then massage the data into gojsonsm compatible format
// and pass it through gojsonsm for testing
func (u *Utilities) FilterExpressionMatchesDoc(expression, docId, bucketName string, collectionNs *base.CollectionNamespace, addr string, port uint16) (result bool, err error) {
	nsServerDocContent := make(map[string]interface{})
	hostAddr := base.GetHostAddr(addr, port)
	var statusCode int

	matcher, err := base.ValidateAndGetAdvFilter(expression)
	if err != nil {
		return
	}

	if collectionNs == nil {
		collectionNs = base.NewDefaultCollectionNamespace()
	}

	urlPath := u.composeNsServerDocGetPath(bucketName, collectionNs, docId)

	retryOp := func() error {
		err, statusCode = u.QueryRestApi(hostAddr, urlPath, false /*preservePathEncoding*/, base.MethodGet, "" /*contentType*/, nil, /*body*/
			0 /*timeout*/, &nsServerDocContent, u.logger_utils)

		if err != nil {
			return err
		} else if statusCode != http.StatusOK {
			return fmt.Errorf("Err returned: %v along with http status code: %v", err, statusCode)
		} else {
			return nil
		}
	}

	err = u.ExponentialBackoffExecutor("filterTesterXattrRetriever", base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor, retryOp)
	if err != nil {
		if strings.Contains(err.Error(), base.ErrorResourceDoesNotExist.Error()) {
			err = fmt.Errorf("Specified document not found")
		}
		return
	}

	return u.processNsServerDocForFiltering(matcher, nsServerDocContent, docId)
}

func (u *Utilities) processNsServerDocForFiltering(matcher gojsonsm.Matcher, nsServerDocContent map[string]interface{}, docId string) (result bool, err error) {
	// Take care of the body - it shows up as a string of pre-formatted json
	// i.e. the whole {"k":"v"} as the value of specified key below
	processedJson := make(map[string]interface{})
	if body, ok := nsServerDocContent[base.BucketDocBodyKey].(string); ok {
		marshaledBytes := []byte(body)
		err = json.Unmarshal(marshaledBytes, &processedJson)
		if err != nil {
			err = fmt.Errorf("Unable to process document body: %v", err)
			return
		}
	}

	// Add document key in
	processedJson[base.InternalKeyKey] = docId

	// Add xattribute
	if xattrs, ok := nsServerDocContent[base.BucketDocXattrKey].(map[string]interface{}); ok {
		processedJson[base.InternalKeyXattr] = xattrs
	}

	byteSlice, err := json.Marshal(processedJson)
	if err != nil {
		err = fmt.Errorf("Unable to marshal postprocessed data for matching: %v", err)
		return
	}

	matched, status, err := base.MatchWrapper(matcher, byteSlice)
	if u.logger_utils.GetLogLevel() >= log.LogLevelDebug && status&gojsonsm.MatcherCollateUsed > 0 {
		u.logger_utils.Debugf("Matcher used collate to determine outcome (%v) for document %v%v%v", matched, base.UdTagBegin, docId, base.UdTagEnd)
	}
	return matched, err
}

// given a matches map, convert the indices from byte index to rune index
func (u *Utilities) convertByteIndexToRuneIndex(key string, matches [][]int) ([][]int, error) {
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

	if u.logger_utils.GetLogLevel() >= log.LogLevelDebug {
		u.logger_utils.Debugf("key=%v, indexMap=%v%v%v\n", base.UdTagBegin, key, base.UdTagEnd, indexMap)
	}

	var ok bool
	for _, match := range matches {
		convertedMatch := make([]int, 2)
		convertedMatch[0], ok = indexMap[match[0]]
		if !ok {
			// should not happen
			errMsg := u.InvalidRuneIndexErrorMessage(key, match[0])
			u.logger_utils.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		convertedMatch[1], ok = indexMap[match[1]]
		if !ok {
			// should not happen
			errMsg := u.InvalidRuneIndexErrorMessage(key, match[1])
			u.logger_utils.Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		convertedMatches = append(convertedMatches, convertedMatch)
	}

	return convertedMatches, nil
}

func (u *Utilities) InvalidRuneIndexErrorMessage(key string, index int) string {
	return fmt.Sprintf("byte index, %v, in match for key, %v, is not a starting index for a rune", index, key)
}

func (u *Utilities) LocalBucketUUID(local_connStr string, bucketName string, logger *log.CommonLogger) (string, error) {
	return u.BucketUUID(local_connStr, bucketName, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, logger)
}

func (u *Utilities) BucketStorageBackend(bucketInfo map[string]interface{}) (string, error) {
	storageBackendObj, ok := bucketInfo[base.StorageBackendKey]
	if !ok {
		return "", fmt.Errorf("Error looking up %v from bucketInfo", base.StorageBackendKey)
	}
	storageBackend, ok := storageBackendObj.(string)
	if !ok {
		return "", fmt.Errorf("%v is of wrong type. Expect string but got %v.", base.StorageBackendKey, reflect.TypeOf(storageBackendObj))
	}
	return storageBackend, nil
}

func (u *Utilities) GetCollectionManifestUidFromBucketInfo(bucketInfo map[string]interface{}) (uint64, error) {
	manifestUidObj, ok := bucketInfo[base.CollectionsManifestUidKey]
	if !ok {
		return 0, fmt.Errorf("Error looking up %v from bucketInfo %v", base.CollectionsManifestUidKey, bucketInfo)
	}
	manifestUidStr, ok := manifestUidObj.(string)
	if !ok {
		return 0, fmt.Errorf("%v is of wrong type. Expect string but got %v.", base.CollectionsManifestUidKey, reflect.TypeOf(manifestUidObj))
	}
	manifestUid, err := strconv.ParseUint(manifestUidStr, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("Error '%v' while parsing %v as manifest Uid.", err.Error(), manifestUidStr)
	}
	return manifestUid, nil
}

func (u *Utilities) GetCrossClusterVersioningFromBucketInfo(bucketInfo map[string]interface{}) (bool, error) {
	ccvObj, ok := bucketInfo[base.EnableCrossClusterVersioningKey]
	if !ok {
		return false, fmt.Errorf("Error looking up %v from bucketInfo %v", base.EnableCrossClusterVersioningKey, bucketInfo)
	}
	ccv, ok := ccvObj.(bool)
	if !ok {
		return false, fmt.Errorf("%v is of wrong type. Expect bool but got %v.", base.EnableCrossClusterVersioningKey, reflect.TypeOf(ccvObj))
	}
	return ccv, nil
}

func (u *Utilities) GetVersionPruningWindowHrs(bucketInfo map[string]interface{}) (int, error) {
	pruningWindowObj, ok := bucketInfo[base.VersionPruningWindowHrsKey]
	if !ok {
		return 0, fmt.Errorf("Error looking up %v from bucketInfo %v", base.VersionPruningWindowHrsKey, bucketInfo)
	}
	pruningWindowFloat, ok := pruningWindowObj.(float64)
	if !ok {
		return 0, fmt.Errorf("%v is of wrong type. Expect float64 but got %v.", base.VersionPruningWindowHrsKey, reflect.TypeOf(pruningWindowFloat))
	}
	return int(pruningWindowFloat), nil
}

// These max_cas's are specifically stamped in ns_server for HLV specific uses and NOT the traditional KV stats max cas
func (u *Utilities) GetVbucketsMaxCas(bucketInfo map[string]interface{}) ([]interface{}, error) {
	maxCasObj, ok := bucketInfo[base.HlvVbMaxCasKey]
	if !ok {
		return nil, fmt.Errorf("Error looking up %v from bucketInfo %v", base.HlvVbMaxCasKey, bucketInfo)
	}
	maxCas, ok := maxCasObj.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%v is of wrong type. Expect []interface{} but got %v.", base.HlvVbMaxCasKey, reflect.TypeOf(maxCasObj))
	}
	return maxCas, nil
}

func (u *Utilities) ReplicationStatusNotFoundError(topic string) error {
	return fmt.Errorf("Cannot find replication status for topic %v", topic)
}

func (u *Utilities) BucketNotFoundError(bucketName string) error {
	return fmt.Errorf("Bucket `%v` not found.", bucketName)
}

// creates a local memcached connection.
// always use plain auth
func (u *Utilities) GetMemcachedConnectionWFeatures(serverAddr, bucketName, userAgent string,
	keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error) {
	var respondedFeatures HELOFeatures

	logger.Infof("GetMemcachedConnection serverAddr=%v, bucketName=%v\n", serverAddr, bucketName)
	if serverAddr == "" {
		err := fmt.Errorf("Failed to get memcached connection because serverAddr is empty. bucketName=%v, userAgent=%v", bucketName, userAgent)
		logger.Warnf(err.Error())
		return nil, respondedFeatures, err
	}

	stopFunc := u.StartDiagStopwatch(fmt.Sprintf("cbauth.GetMemcachedServiceAuth(%v)", serverAddr), base.DiagInternalThreshold)
	username, password, err := cbauth.GetMemcachedServiceAuth(serverAddr)
	if u.logger_utils.GetLogLevel() >= log.LogLevelDebug {
		logger.Debugf("memcached auth: username=%v%v%v, password=%v%v%v, err=%v\n", base.UdTagBegin, username, base.UdTagEnd, base.UdTagBegin, password, base.UdTagEnd, err)
	}
	stopFunc()
	if err != nil {
		return nil, respondedFeatures, err
	}

	return u.GetRemoteMemcachedConnectionWFeatures(serverAddr, username, password, bucketName, userAgent, true /*plainAuth*/, keepAlivePeriod, features, logger)
}

func (u *Utilities) GetMemcachedConnection(serverAddr, bucketName, userAgent string,
	keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error) {
	var noFeatures HELOFeatures
	clientIface, _, err := u.GetMemcachedConnectionWFeatures(serverAddr, bucketName, userAgent, keepAlivePeriod, noFeatures, logger)
	return clientIface, err
}

func (u *Utilities) GetMemcachedRawConn(serverAddr, username, password, bucketName string, plainAuth bool,
	keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error) {
	// getting a conn should not take too long
	stopFunc := u.StartDiagStopwatch(fmt.Sprintf("GetMemcachedRawConn(%v, %v, %v, %v)", serverAddr, bucketName, plainAuth, keepAlivePeriod), base.DiagInternalThreshold)
	defer stopFunc()
	conn, err := base.NewConn(serverAddr, username, password, bucketName, plainAuth, keepAlivePeriod, logger)
	if err != nil {
		return nil, err
	}
	return conn, err
}

func (u *Utilities) GetRemoteMemcachedConnectionWFeatures(serverAddr, username, password, bucketName, userAgent string,
	plainAuth bool, keepAlivePeriod time.Duration, features HELOFeatures, logger *log.CommonLogger) (mcc.ClientIface, HELOFeatures, error) {
	var err error
	var conn mcc.ClientIface
	var respondedFeatures HELOFeatures

	getRemoteMcConnOp := func() error {
		conn, err = u.GetMemcachedRawConn(serverAddr, username, password, bucketName, plainAuth, keepAlivePeriod, logger)
		if err != nil {
			logger.Warnf("Failed to construct memcached client for %v, err=%v\n", serverAddr, err)
			return err
		}

		respondedFeatures, err = u.SendHELOWithFeatures(conn, userAgent, base.HELOTimeout, base.HELOTimeout, features, logger)

		if err != nil {
			conn.Close()
			logger.Warnf("Failed to send HELO for %v, err=%v\n", conn, err)
			return err
		}
		return nil
	}

	opErr := u.ExponentialBackoffExecutor("GetRemoteMemcachedConnection", base.RemoteMcRetryWaitTime, base.MaxRemoteMcRetry,
		base.RemoteMcRetryFactor, getRemoteMcConnOp)

	if opErr != nil {
		logger.Errorf(opErr.Error())
		return nil, respondedFeatures, err
	}

	return conn, respondedFeatures, err
}

func (u *Utilities) GetRemoteMemcachedConnection(serverAddr, username, password, bucketName, userAgent string,
	plainAuth bool, keepAlivePeriod time.Duration, logger *log.CommonLogger) (mcc.ClientIface, error) {
	var noFeatureEnabled HELOFeatures
	conn, _, err := u.GetRemoteMemcachedConnectionWFeatures(serverAddr, username, password, bucketName, userAgent, plainAuth, keepAlivePeriod,
		noFeatureEnabled, logger)
	return conn, err
}

// send helo to memcached with data type (including xattr) feature enabled
// we need to know whether data type is indeed enabled from helo response
// unsuccessful response is treated as errors
func (u *Utilities) SendHELOWithFeatures(client mcc.ClientIface, userAgent string, readTimeout, writeTimeout time.Duration, requestedFeatures HELOFeatures, logger *log.CommonLogger) (respondedFeatures HELOFeatures, err error) {
	stopFunc := u.StartDiagStopwatch(fmt.Sprintf("SendHELOWithFeatures(%v)", requestedFeatures), base.DiagNetworkThreshold)
	defer stopFunc()
	// Initially set initial respondedFeatures to None since no compression negotiated should not be invalid
	respondedFeatures.CompressionType = base.CompressionTypeNone

	// Translate XDCR feature set to gomemcached featureset
	var clientFeatureSet mcc.Features

	// TCP No delay is always first
	clientFeatureSet = append(clientFeatureSet, mcc.FeatureTcpNoDelay)

	if requestedFeatures.Xattribute {
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureXattr)
	}

	if requestedFeatures.CompressionType == base.CompressionTypeSnappy {
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureSnappyCompression)
	}

	if requestedFeatures.Xerror {
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureXerror)
	}

	if requestedFeatures.Collections {
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureCollections)
	}

	if requestedFeatures.DataType {
		// This is named JSON in kv_engine's feature.h
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureDataType)
	}

	if requestedFeatures.SnappyEverywhere {
		clientFeatureSet = append(clientFeatureSet, mcc.FeatureSnappyEverywhere)
	}

	client.SetConnName(userAgent)
	response, err := client.EnableFeatures(clientFeatureSet)

	if err != nil {
		logger.Errorf("Received error response from HELO command. userAgent=%v, err=%v.", userAgent, err)
	} else if response.Status != mc.SUCCESS {
		errMsg := fmt.Sprintf("Received unexpected response from HELO command. userAgent=%v, response status=%v.", userAgent, response.Status)
		logger.Error(errMsg)
		err = errors.New(errMsg)
	} else {
		// helo succeeded. parse response body for features enabled
		bodyLen := len(response.Body)
		if (bodyLen & 1) != 0 {
			// body has to have even number of bytes
			logger.Errorf("Received response body with odd number of bytes from HELO command. userAgent=%v, (redacted) response body=%v%v%v.", userAgent, base.UdTagBegin, response.Body, base.UdTagEnd)
			err = errors.New(fmt.Sprintf("Received response body with odd number of bytes from HELO command. userAgent=%v.", userAgent))
			return
		}
		pos := 0
		for {
			if pos >= bodyLen {
				break
			}
			feature := binary.BigEndian.Uint16(response.Body[pos : pos+2])
			if feature == base.HELO_FEATURE_XATTR {
				respondedFeatures.Xattribute = true
			}
			if feature == base.HELO_FEATURE_SNAPPY {
				respondedFeatures.CompressionType = base.CompressionTypeSnappy
			}
			if feature == base.HELO_FEATURE_XERROR {
				respondedFeatures.Xerror = true
			}
			if feature == base.HELO_FEATURE_COLLECTIONS {
				respondedFeatures.Collections = true
			}
			if feature == base.HELO_FEATURE_JSON {
				respondedFeatures.DataType = true
			}
			if feature == base.HELO_FEATURE_SNAPPYEVERYWHERE {
				respondedFeatures.SnappyEverywhere = true
			}
			pos += 2
		}
		logger.Infof("Successful HELO for %v with %+v", userAgent, respondedFeatures)
	}
	return
}

// compose a HELO command
func (u *Utilities) ComposeHELORequest(userAgent string, features HELOFeatures) *mc.MCRequest {
	var value []byte
	var numOfFeatures = features.NumberOfActivatedFeatures()
	var sliceIndex int
	bytesToAllocate := base.HELO_BYTES_PER_FEATURE * (numOfFeatures + 1) // TCP_NO_DELAY is included by default
	value = make([]byte, bytesToAllocate)

	// tcp no delay - [0:2]
	binary.BigEndian.PutUint16(value[sliceIndex:sliceIndex+base.HELO_BYTES_PER_FEATURE], base.HELO_FEATURE_TCP_NO_DELAY)
	sliceIndex += base.HELO_BYTES_PER_FEATURE

	// Xattribute
	if features.Xattribute {
		binary.BigEndian.PutUint16(value[sliceIndex:sliceIndex+base.HELO_BYTES_PER_FEATURE], base.HELO_FEATURE_XATTR)
		sliceIndex += base.HELO_BYTES_PER_FEATURE
	}

	// Compression
	if features.CompressionType == base.CompressionTypeSnappy {
		binary.BigEndian.PutUint16(value[sliceIndex:sliceIndex+base.HELO_BYTES_PER_FEATURE], base.HELO_FEATURE_SNAPPY)
		sliceIndex += base.HELO_BYTES_PER_FEATURE
	}

	// Xerror
	if features.Xerror {
		binary.BigEndian.PutUint16(value[sliceIndex:sliceIndex+base.HELO_BYTES_PER_FEATURE], base.HELO_FEATURE_XERROR)
		sliceIndex += base.HELO_BYTES_PER_FEATURE
	}

	return &mc.MCRequest{
		Key:    []byte(userAgent),
		Opcode: mc.HELLO,
		Body:   value,
	}
}

func (u *Utilities) GetIntSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) (int, error) {
	settingObj := u.GetSettingFromSettings(settings, settingName)
	if settingObj == nil {
		return -1, nil
	}

	setting, ok := settingObj.(int)
	if !ok {
		return -1, fmt.Errorf("Setting %v is of wrong type", settingName)
	}

	return setting, nil
}

func (u *Utilities) GetStringSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) (string, error) {
	settingObj := u.GetSettingFromSettings(settings, settingName)
	if settingObj == nil {
		return "", nil
	}

	setting, ok := settingObj.(string)
	if !ok {
		return "", fmt.Errorf("Setting %v is of wrong type", settingName)
	}

	return setting, nil
}

func (u *Utilities) GetSettingFromSettings(settings metadata.ReplicationSettingsMap, settingName string) interface{} {
	if settings == nil {
		return nil
	}

	setting, ok := settings[settingName]
	if !ok {
		return nil
	}

	return setting
}

func (u *Utilities) GetMemcachedClient(serverAddr, bucketName string, kv_mem_clients map[string]mcc.ClientIface,
	userAgent string, keepAlivePeriod time.Duration, logger *log.CommonLogger, features HELOFeatures) (mcc.ClientIface, error) {
	client, ok := kv_mem_clients[serverAddr]
	if ok {
		return client, nil
	} else {
		if bucketName == "" {
			err := fmt.Errorf("Failed to get memcached client because of unexpected empty bucketName. serverAddr=%v, userAgent=%v", serverAddr, userAgent)
			logger.Warnf(err.Error())
			return nil, err
		}

		var client, respFeatures, err = u.GetMemcachedConnectionWFeatures(serverAddr, bucketName, userAgent, keepAlivePeriod, features, logger)
		if err == nil {
			if features != respFeatures {
				logger.Warnf("GetMemcachedClient for %v requested features: %v but got %v", serverAddr, features, respFeatures)
			}
			kv_mem_clients[serverAddr] = client
			return client, nil
		} else {
			return nil, err
		}
	}
}

func (u *Utilities) GetServersListFromBucketInfo(bucketInfo map[string]interface{}) ([]string, error) {
	vbucketServerMapObj, ok := bucketInfo[base.VBucketServerMapKey]
	if !ok {
		// The returned error will be displayed on UI. We don't want to include the bucketInfo map since it is too much info for UI.
		u.logger_utils.Errorf("Error getting vbucket server map from bucket info. bucketInfo=%v", bucketInfo)
		return nil, fmt.Errorf("Error getting %v from bucket info", base.VBucketServerMapKey)
	}
	vbucketServerMap, ok := vbucketServerMapObj.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Vbucket server map is of wrong type. vbucketServerMap=%v", vbucketServerMapObj)
	}

	// get server list
	serverListObj, ok := vbucketServerMap[base.ServerListKey]
	if !ok {
		return nil, fmt.Errorf("Error getting server list from vbucket server map. vbucketServerMap=%v", vbucketServerMap)
	}
	serverList, ok := serverListObj.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Server list is of wrong type. serverList=%v", serverListObj)
	}

	servers := make([]string, len(serverList))
	for index, serverName := range serverList {
		serverNameStr, ok := serverName.(string)
		if !ok {
			return nil, fmt.Errorf("Server name is of wrong type. serverName=%v", serverName)
		}
		servers[index] = serverNameStr
	}
	return servers, nil
}

func (u *Utilities) GetServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}, recycledMapGetter func(nodes []string) *base.KvVBMapType, serversList []string) (*base.KvVBMapType, error) {
	vbucketServerMapObj, ok := bucketInfo[base.VBucketServerMapKey]
	if !ok {
		// The returned error will be displayed on UI. We don't want to include the bucketInfo map since it is too much info for UI.
		u.logger_utils.Errorf("Error getting vbucket server map from bucket info. connStr=%v, bucketName=%v, bucketInfo=%v\n", connStr, bucketName, bucketInfo)
		return nil, fmt.Errorf("Error getting %v from bucket info. connStr=%v, bucketName=%v\n", base.VBucketServerMapKey, connStr, bucketName)
	}
	vbucketServerMap, ok := vbucketServerMapObj.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Vbucket server map is of wrong type. connStr=%v, bucketName=%v, vbucketServerMap=%v\n", connStr, bucketName, vbucketServerMapObj)
	}

	// get vbucket "map"
	vbucketMapObj, ok := vbucketServerMap[base.VBucketMapKey]
	if !ok {
		return nil, fmt.Errorf("Error getting vbucket map from vbucket server map. connStr=%v, bucketName=%v, vbucketServerMap=%v\n", connStr, bucketName, vbucketServerMap)
	}
	vbucketMap, ok := vbucketMapObj.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Vbucket map is of wrong type. connStr=%v, bucketName=%v, vbucketMap=%v\n", connStr, bucketName, vbucketMapObj)
	}

	servers, err := u.GetServersListFromBucketInfo(bucketInfo)
	if err != nil {
		return nil, err
	}

	var serverVBMap *base.KvVBMapType
	var keepMap map[string]bool
	if recycledMapGetter != nil {
		keepMap = make(map[string]bool)
		serverVBMap = recycledMapGetter(serversList)
		for server, _ := range *serverVBMap {
			(*serverVBMap)[server] = (*serverVBMap)[server][:0]
		}
	} else {
		newMap := make(base.KvVBMapType)
		serverVBMap = &newMap
	}

	for vbno, indexListObj := range vbucketMap {
		indexList, ok := indexListObj.([]interface{})
		if !ok {
			return nil, fmt.Errorf("Index list is of wrong type. connStr=%v, bucketName=%v, indexList=%v\n", connStr, bucketName, indexListObj)
		}
		if len(indexList) == 0 {
			return nil, fmt.Errorf("Index list is empty. connStr=%v, bucketName=%v, vbno=%v\n", connStr, bucketName, vbno)
		}
		indexFloat, ok := indexList[0].(float64)
		if !ok {
			return nil, fmt.Errorf("Master index is of wrong type. connStr=%v, bucketName=%v, index=%v\n", connStr, bucketName, indexList[0])
		}
		indexInt := int(indexFloat)
		if indexInt >= len(servers) {
			return nil, fmt.Errorf("Master index is out of range. connStr=%v, bucketName=%v, index=%v\n", connStr, bucketName, indexInt)
		} else if indexInt < 0 {
			// During rebalancing or topology changes, it's possible ns_server may return a -1 for index. Callers should treat it as an transient error.
			return nil, fmt.Errorf(fmt.Sprintf("%v connStr=%v, bucketName=%v, index=%v\n", base.ErrorMasterNegativeIndex, connStr, bucketName, indexInt))
		}

		server := servers[indexInt]
		if keepMap != nil {
			keepMap[server] = true
		}
		var vbList []uint16
		vbList, ok = (*serverVBMap)[server]
		if !ok {
			vbList = make([]uint16, 0)
		}
		vbList = append(vbList, uint16(vbno))
		(*serverVBMap)[server] = vbList
	}

	if recycledMapGetter != nil {
		for checkName, _ := range *serverVBMap {
			if _, exists := keepMap[checkName]; !exists {
				delete(*serverVBMap, checkName)
			}
		}
	}
	return serverVBMap, nil
}

func (u *Utilities) GetRemoteServerVBucketsMap(connStr, bucketName string, bucketInfo map[string]interface{}, useExternal bool) (map[string][]uint16, error) {
	if len(bucketInfo) == 0 {
		return nil, base.ErrorInvalidInput
	}

	kvVbMapPtr, err := u.GetServerVBucketsMap(connStr, bucketName, bucketInfo, nil, nil)
	if err != nil {
		return nil, err
	}
	if useExternal {
		u.TranslateKvVbMap(*kvVbMapPtr, bucketInfo)
	}
	return *kvVbMapPtr, nil
}

// get bucket type setting from bucket info
func (u *Utilities) GetBucketTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error) {
	bucketType := ""
	bucketTypeObj, ok := bucketInfo[base.BucketTypeKey]
	if !ok {
		return "", fmt.Errorf("Error looking up bucket type of bucket %v", bucketName)
	} else {
		bucketType, ok = bucketTypeObj.(string)
		if !ok {
			return "", fmt.Errorf("bucketType on bucket %v is of wrong type.", bucketName)
		}
	}
	return bucketType, nil
}

// get conflict resolution type setting from bucket info
func (u *Utilities) GetConflictResolutionTypeFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error) {
	conflictResolutionType := base.ConflictResolutionType_Seqno
	conflictResolutionTypeObj, ok := bucketInfo[base.ConflictResolutionTypeKey]
	if ok {
		conflictResolutionType, ok = conflictResolutionTypeObj.(string)
		if !ok {
			return "", fmt.Errorf("ConflictResolutionType on bucket %v is of wrong type.", bucketName)
		}
	}
	return conflictResolutionType, nil
}

// get EvictionPolicy setting from bucket info
func (u *Utilities) GetEvictionPolicyFromBucketInfo(bucketName string, bucketInfo map[string]interface{}) (string, error) {
	evictionPolicy := ""
	evictionPolicyObj, ok := bucketInfo[base.EvictionPolicyKey]
	if ok {
		evictionPolicy, ok = evictionPolicyObj.(string)
		if !ok {
			return "", fmt.Errorf("EvictionPolicy on bucket %v is of wrong type.", bucketName)
		}
	}
	return evictionPolicy, nil
}

/**
 * The second section is couchbase REST related utility functions
 */
// This method is used to get the SSL port for target nodes - will use alternate fields if requested
func (u *Utilities) GetMemcachedSSLPortMap(connStr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool,
	clientCertificate []byte, clientKey []byte, bucket string, logger *log.CommonLogger, useExternal bool) (base.SSLPortMap, error) {
	logger.Infof("GetMemcachedSSLPort, connStr=%v, useExternal=%v, authMech=%v", connStr, useExternal, authMech)

	bucketInfo, err := u.GetClusterInfo(connStr, base.BPath+bucket, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return nil, err
	}

	nonTerseInfo, err := u.GetBucketInfo(connStr, bucket, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return nil, err
	}

	kvVbMapHasSSLPort, err := u.ShouldUseTerseBucketInfo(nonTerseInfo, connStr, bucket, useExternal, authMech == base.HttpAuthMechHttps)
	if err != nil {
		return nil, err
	}

	portMap, err := u.getMemcachedSSLPortMapInternal(connStr, bucketInfo, logger, useExternal, kvVbMapHasSSLPort)
	if err != nil {
		return nil, err
	}

	return portMap, nil
}

// getMemcachedSSLPortMapInternal returns the mapping of hostAddr in kv_vb_map to kvSSL ports.
// kvVbMapHasSSLPort is true when kv_vb_map already has kvSSL ports in hostAddress keys.
func (u *Utilities) getMemcachedSSLPortMapInternal(connStr string, bucketInfo map[string]interface{}, logger *log.CommonLogger, useExternal bool, kvVbMapHasSSLPort bool) (base.SSLPortMap, error) {
	if len(bucketInfo) == 0 {
		return nil, base.ErrorInvalidInput
	}

	nodesExt, ok := bucketInfo[base.NodeExtKey]
	if !ok {
		errMsg := fmt.Sprintf("%v not found in bucketInfo=%v", base.NodeExtKey, bucketInfo)
		return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
	}

	nodesExtArray, ok := nodesExt.([]interface{})
	if !ok {
		errMsg := fmt.Sprintf("nodesExt=%v cannot be parsed as []interface{}, as it is of incompatible type, its type=%v", nodesExt, reflect.TypeOf(nodesExt))
		return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
	}

	var hostName, hostAddr string
	var err error
	portMap := make(base.SSLPortMap)
	for _, nodeExt := range nodesExtArray {
		var portNumberToUse uint16
		nodeExtMap, ok := nodeExt.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("nodeExt=%v cannot be parsed as map[string]interface{}, as it is of incompatible type, its type=%v", nodeExtMap, reflect.TypeOf(nodeExtMap))
			return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
		}

		// Internal key
		service, ok := nodeExtMap[base.ServicesKey]
		if !ok {
			errMsg := fmt.Sprintf("%v not found in nodeExtMap=%v", base.ServicesKey, nodeExtMap)
			return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
		}

		services_map, ok := service.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("service=%v cannot be parsed as map[string]interface{}, as it is of incompatible type, its type=%v", service, reflect.TypeOf(service))
			return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
		}

		kv_port, ok := services_map[base.KVPortKey]
		if !ok {
			// the node may not have kv services. skip the node
			logger.Debugf("Skipping the node as it may not have the KV service, nodeExtMap=%v", nodeExtMap)
			continue
		}

		kvPortFloat, ok := kv_port.(float64)
		if !ok {
			errMsg := fmt.Sprintf("kv_port=%v cannot be parsed as float64, as it is of incompatible type, its type=%v", kv_port, reflect.TypeOf(kv_port))
			return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
		}

		hostAddr = ""
		portNumberToUse = 0
		// Since this is a call intended for targets, get the external info if requested
		if useExternal {
			externalHostAddr, externalKVPort, externalKVPortErr, externalSSLPort, externalSSLPortErr := u.GetExternalAddressAndKvPortsFromNodeInfo(nodeExtMap)
			if len(externalHostAddr) == 0 {
				msg := fmt.Sprintf("%v, bucketInfo=%v", base.ErrorTargetNoAltHostName, bucketInfo)
				logger.Infof(msg)
			} else {
				if externalKVPortErr == nil {
					// External address and port both exist
					hostAddr = base.GetHostAddr(externalHostAddr, uint16(externalKVPort))
				} else if externalKVPortErr == base.ErrorNoPortNumber {
					// External address exists, but port does not. Use internal host's port number
					hostAddr = base.GetHostAddr(externalHostAddr, uint16(kvPortFloat))
				}

				if externalSSLPortErr == nil {
					portNumberToUse = uint16(externalSSLPort)
				}
			}
		}

		if hostAddr == "" {
			// note that this is the only place where nodeExtMap contains a hostname without port
			// instead of a host address with port. This represents the internal IP.
			hostName, _, err = u.getHostNameWithoutPortFromNodeInfo(connStr, nodeExtMap, logger)
			if err != nil {
				return nil, u.BucketInfoParseError(bucketInfo, fmt.Sprintf("%v", err), logger)
			}
			hostAddr = base.GetHostAddr(hostName, uint16(kvPortFloat))
		}

		if portNumberToUse == 0 {
			kv_ssl_port, ok := services_map[base.KVSSLPortKey]
			if !ok {
				errMsg := fmt.Sprintf("%v not found in services_map=%v", base.KVSSLPortKey, services_map)
				return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
			}
			kvSSLPortFloat, ok := kv_ssl_port.(float64)
			if !ok {
				errMsg := fmt.Sprintf("kv_ssl_port=%v cannot be parsed as float64, as it is of incompatible type, its type=%v", kv_ssl_port, reflect.TypeOf(kv_ssl_port))
				return nil, u.BucketInfoParseError(bucketInfo, errMsg, logger)
			}
			portNumberToUse = uint16(kvSSLPortFloat)
		}

		if kvVbMapHasSSLPort {
			// This is a special case where kv_vb_map was constructed using terse bucket info
			// pools/default/b/bucketName instead of pools/default/buckets/bucketName. So kvSSL port
			// will already be used to construct hostAddr key of kv_vb_map. To match the keys of ssl_port_map
			// and kv_vb_map, we need to also construct the key using kvSSLPort.
			hostAddr = base.GetHostAddr(base.GetHostName(hostAddr), portNumberToUse)
		}
		portMap[hostAddr] = portNumberToUse
	}
	logger.Infof("memcached ssl port map=%v\n", portMap)
	return portMap, nil
}

func nodeServicesInfoParseError(nodeServicesInfo interface{}, logger *log.CommonLogger) error {
	errMsg := "Error parsing resource from Node Services endpoint of remote cluster."
	detailedErrMsg := errMsg + fmt.Sprintf(" nodeServicesInfo=%v", nodeServicesInfo)
	if logger != nil {
		logger.Errorf(detailedErrMsg)
	}
	return fmt.Errorf(errMsg)
}

func getExternalInfoFromServicesMap(logger *log.CommonLogger, nodeExtMap map[string]interface{}) (string, bool, map[string]interface{}, bool) {
	alternateAddresses, ok := nodeExtMap[base.AlternateKey]
	if !ok {
		return "", false, nil, false
	}
	alternateAddressesMap, ok := alternateAddresses.(map[string]interface{})
	if !ok {
		logger.Errorf("alternateAddresses is not of type map[string]interface{}, alternateAddresses=%v", alternateAddresses)
		return "", false, nil, false
	}
	externalAddresses, ok := alternateAddressesMap[base.ExternalKey]
	if !ok {
		return "", false, nil, false
	}
	externalAddressesMap, ok := externalAddresses.(map[string]interface{})
	if !ok {
		logger.Errorf("externalAddresses is not of type map[string]interface{}, externalAddresses=%v", externalAddresses)
		return "", false, nil, false
	}
	ports, portsExists := externalAddressesMap[base.PortsKey]
	var portsMap map[string]interface{}
	if portsExists {
		portsMap, ok = ports.(map[string]interface{})
		if !ok {
			logger.Errorf("ports is not of type map[string]interface{}, ports=%v", ports)
			portsExists = false
		}
	}
	hostname, hostnameExists := externalAddressesMap[base.HostNameKey]
	var hostnameStr string
	if hostnameExists {
		hostnameStr, ok = hostname.(string)
		if !ok {
			logger.Errorf("hostname is not of type string, hostname=%v", hostname)
			hostnameExists = false
		}
	}

	return hostnameStr, hostnameExists, portsMap, portsExists
}

func getInternalOrExternalPort(servicesMap map[string]interface{}, extPorts map[string]interface{}, useExternal, extPortsExists bool, portKey string) (port interface{}, found bool) {
	port, found = servicesMap[portKey]
	if useExternal && extPortsExists {
		extPort, extFound := extPorts[portKey]
		if extFound {
			port = extPort
			found = true
		}
	}

	return port, found
}

// Input is the result for pools/default/nodeServices, port keys and a default hostAddr
// returns hostAddr -> <portKey -> port> mapping and list of hostAddrs (only for KV nodes)
// Also returns a boolean which indicates if nodes have "hostname" field missing, indicating defaultConnStr was used instead.
func (u *Utilities) GetPortsAndHostAddrsFromNodeServices(nodesList []interface{}, defaultConnStr string, useSecurePort bool, useExternal bool, logger *log.CommonLogger) (base.HostPortMapType, []string, bool, error) {
	portsMap := make(base.HostPortMapType)
	hostAddrs := make([]string, 0)

	var hostName string
	var hasNodeWithMissingHostname bool
	var err error
	for _, nodeExt := range nodesList {
		var missingHostname bool

		nodeExtMap, ok := nodeExt.(map[string]interface{})
		if !ok {
			return nil, nil, false, nodeServicesInfoParseError(nodesList, logger)
		}

		// Internal key
		service, ok := nodeExtMap[base.ServicesKey]
		if !ok {
			return nil, nil, false, nodeServicesInfoParseError(nodesList, logger)
		}

		servicesMap, ok := service.(map[string]interface{})
		if !ok {
			return nil, nil, false, nodeServicesInfoParseError(nodesList, logger)
		}

		extHostname, extHostnameExists, extPorts, extPortsExists := getExternalInfoFromServicesMap(logger, nodeExtMap)

		// note that this is the only place where nodeExtMap contains a hostname without port
		// instead of a host address with port
		hostName, missingHostname, err = u.getHostNameWithoutPortFromNodeInfo(defaultConnStr, nodeExtMap, logger)
		if err != nil {
			return nil, nil, false, fmt.Errorf("%v | err=%v", nodeServicesInfoParseError(nodesList, logger), err)
		}

		if missingHostname {
			hasNodeWithMissingHostname = true
		}

		if useExternal && extHostnameExists {
			hostName = extHostname
		}

		_, hasKV := getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, base.KVPortKey)
		_, hasKVSSL := getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, base.KVSSLPortKey)
		_, hasMgmtSSL := getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, base.SSLMgtPortKey)
		useSecurePort = useSecurePort && hasMgmtSSL

		// consider the nodes if it has KV service only
		if !hasKV && !hasKVSSL {
			continue
		}

		hostAddr := hostName
		var port interface{}
		if useSecurePort {
			port, ok = getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, base.SSLMgtPortKey)
		} else {
			port, ok = getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, base.MgtPortKey)
		}
		if ok {
			portFloat, ok := port.(float64)
			if !ok {
				return nil, nil, false, nodeServicesInfoParseError(nodesList, logger)
			}
			mgmtPort := uint16(portFloat)
			hostAddr = base.GetHostAddr(hostName, mgmtPort)
		}

		for _, portKey := range base.PortsKeysForConnectionPreCheck {
			var portInt uint16
			port, ok = getInternalOrExternalPort(servicesMap, extPorts, useExternal, extPortsExists, portKey)
			if !ok {
				// the node may not have the service. skip the node
				continue
			}

			portFloat, ok := port.(float64)
			if !ok {
				return nil, nil, false, nodeServicesInfoParseError(nodesList, logger)
			}

			portInt = uint16(portFloat)

			_, ok = portsMap[hostAddr]
			if !ok {
				portsMap[hostAddr] = make(map[string]uint16)
			}
			portsMap[hostAddr][portKey] = portInt
		}

		hostAddrs = append(hostAddrs, hostAddr)
	}
	logger.Infof("Ports=%v, HostAddrs=%v, missingHostname=%v in GetPortsAndHostAddrsFromNodeServices", portsMap, hostAddrs, hasNodeWithMissingHostname)
	return portsMap, hostAddrs, hasNodeWithMissingHostname, nil
}

func (u *Utilities) BucketInfoParseError(bucketInfo map[string]interface{}, err string, logger *log.CommonLogger) error {
	errMsg := fmt.Sprintf("Error parsing memcached ssl port of remote cluster, err=%v. ", err)
	detailedErrMsg := errMsg + fmt.Sprintf("bucketInfo=%v", bucketInfo)
	logger.Errorf(detailedErrMsg)
	return fmt.Errorf(errMsg)
}

// The input is a non-https address, potentially with or without a port
// Returns 2 pairs of strings:
// 1. Hostname:internalSSLPort
// 2. Hostname:externalSSLPort (or "" if no external port)
func (u *Utilities) HttpsRemoteHostAddr(hostAddr string, logger *log.CommonLogger) (string, string, error) {
	// Extract hostname to be combined with SSL port
	hostName := base.GetHostName(hostAddr)

	internalSSLPort, internalErr, externalSSLPort, externalErr := u.GetRemoteSSLPorts(hostAddr, logger)
	if internalErr != nil {
		return "", "", internalErr
	}

	internalHostPort := base.GetHostAddr(hostName, internalSSLPort)
	var externalHostPort string
	if externalErr == nil {
		externalHostPort = base.GetHostAddr(hostName, externalSSLPort)
	}

	return internalHostPort, externalHostPort, nil
}

func (u *Utilities) GetRemoteSSLPorts(hostAddr string, logger *log.CommonLogger) (internalSSLPort uint16, internalSSLErr error, externalSSLPort uint16, externalSSLErr error) {
	externalSSLErr = base.ErrorNoPortNumber

	portInfo := make(map[string]interface{})
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.SSLPortsPath, false, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, base.MethodGet, "", nil, base.HttpsPortLookupTimeout, &portInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusUnauthorized {
		// SSLPorts request normally do not require any user credentials
		// the only place unauthorized error could be returned is when target is elasticsearch cluster
		// treat this case differently so that a more specific error message can be returned to user
		internalSSLErr = base.ErrorUnauthorized
		return
	}
	if err != nil || statusCode != http.StatusOK {
		if strings.Contains(err.Error(), hostAddr) {
			internalSSLErr = fmt.Errorf("failed on err=%v, statusCode=%v", err, statusCode)
		} else {
			internalSSLErr = fmt.Errorf("failed on calling %v on host %v, err=%v, statusCode=%v", base.SSLPortsPath, hostAddr, err, statusCode)
		}
		return
	}

	sslPort, ok := portInfo[base.SSLPortKey]
	if !ok {
		errMsg := "Failed to parse port info. ssl port is missing."
		logger.Errorf("%v. portInfo=%v", errMsg, portInfo)
		internalSSLErr = fmt.Errorf(errMsg)
		return
	}

	sslPortFloat, ok := sslPort.(float64)
	if !ok {
		internalSSLErr = fmt.Errorf("ssl port is of wrong type. Expected type: float64; Actual type: %s", reflect.TypeOf(sslPort))
		return
	}
	internalSSLPort = uint16(sslPortFloat)

	portNumber, externalSSLErr := u.getExternalSSLMgtPort(portInfo)
	if externalSSLErr == nil {
		externalSSLPort = (uint16)(portNumber)
	}
	return
}

func (u *Utilities) GetClusterInfoWStatusCode(hostAddr, path, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error, int) {
	clusterInfo := make(map[string]interface{})
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, path, false, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, base.MethodGet, "", nil, 0, &clusterInfo, nil, false, logger, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed on calling host=%v, path=%v, err=%v, statusCode=%v", hostAddr, path, err, statusCode), statusCode
	}
	return clusterInfo, nil, statusCode
}

func (u *Utilities) GetClusterInfo(hostAddr, path, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	clusterInfo, err, _ := u.GetClusterInfoWStatusCode(hostAddr, path, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	return clusterInfo, err
}

func (u *Utilities) GetClusterUUID(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error) {
	clusterInfo, err := u.GetClusterInfo(hostAddr, base.PoolsPath, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
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

// get a list of node infos with minimum info
// this api calls xxx/pools/default, which returns a subset of node info such as hostname
// this api can/needs to be used when connecting to elastic search cluster, which supports xxx/pools/default
func (u *Utilities) GetNodeListWithMinInfo(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) ([]interface{}, error) {
	clusterInfo, err := u.GetClusterInfo(hostAddr, base.DefaultPoolPath, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return nil, err
	}

	return u.GetNodeListFromInfoMap(clusterInfo, logger)

}

func (u *Utilities) GetNodeServicesInfo(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	nodeServicesInfo := make(map[string]interface{})
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.NodeServicesPath, false, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, base.MethodGet, "", nil, 0, &nodeServicesInfo, nil, false, logger, nil)

	if err != nil || statusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed on calling host=%v, path=%v, err=%v, statusCode=%v", hostAddr, base.NodeServicesPath, err, statusCode)
	}

	return nodeServicesInfo, nil
}

func (u *Utilities) GetNodesListFromNodeServicesInfo(logger *log.CommonLogger, nodeServicesInfo map[string]interface{}) ([]interface{}, error) {
	nodesExt, ok := nodeServicesInfo[base.NodeExtKey]
	if !ok {
		return nil, nodeServicesInfoParseError(nodeServicesInfo, logger)
	}

	nodesExtArray, ok := nodesExt.([]interface{})
	if !ok {
		return nil, nodeServicesInfoParseError(nodeServicesInfo, logger)
	}

	return nodesExtArray, nil
}

func (u *Utilities) GetClusterUUIDAndNodeListWithMinInfo(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, []interface{}, error) {
	defaultPoolInfo, err := u.GetClusterInfo(hostAddr, base.DefaultPoolPath, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return "", nil, err
	}

	return u.GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo(defaultPoolInfo, logger)

}

func (u *Utilities) GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, []interface{}, error) {
	clusterUUID, err := u.GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo, logger)
	if err != nil {
		return "", nil, err
	}

	nodeList, err := u.GetNodeListFromInfoMap(defaultPoolInfo, logger)

	return clusterUUID, nodeList, err

}

// get bucket info
// a specialized case of GetClusterInfo
func (u *Utilities) GetBucketInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCert []byte, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("Bucket name cannot be empty")
	}
	bucketInfo := make(map[string]interface{})

	// This is used for local as well - but log only if atrociously bad
	stopFunc := u.StartDiagStopwatch(fmt.Sprintf("GetBucketInfo(%v, %v)", hostAddr, bucketName), base.DiagNetworkThreshold)
	defer stopFunc()
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.DefaultPoolBucketsPath+bucketName, false, username, password, authMech, certificate, sanInCertificate, clientCert, clientKey, base.MethodGet, "", nil, 0, &bucketInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusOK {
		return bucketInfo, nil
	}

	switch statusCode {
	case http.StatusNotFound:
		return nil, u.GetNonExistentBucketError()
	case http.StatusUnauthorized:
		return nil, base.ErrorUnauthorized
	case http.StatusForbidden:
		return nil, base.ErrorForbidden
	default:
		logger.Errorf("Failed to get bucket info for bucket '%v'. host=%v, err=%v, statusCode=%v", bucketName, hostAddr, err, statusCode)
		return nil, fmt.Errorf("Failed to get bucket info.")
	}
}

// get bucket uuid
func (u *Utilities) BucketUUID(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error) {
	bucketInfo, err := u.GetBucketInfo(hostAddr, bucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return "", err
	}

	return u.GetBucketUuidFromBucketInfo(bucketName, bucketInfo, logger)
}

func (u *Utilities) GetLocalBuckets(hostAddr string, logger *log.CommonLogger) (map[string]string, error) {
	return u.GetBuckets(hostAddr, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, logger)
}

// return a map of buckets
// key = bucketName, value = bucketUUID
func (u *Utilities) GetBuckets(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]string, error) {
	bucketListInfo := make([]interface{}, 0)
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.DefaultPoolBucketsPath, false, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, base.MethodGet, "", nil, 0, &bucketListInfo, nil, false, logger, nil)
	if err != nil || statusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed on calling host=%v, path=%v, err=%v, statusCode=%v", hostAddr, base.DefaultPoolBucketsPath, err, statusCode)
	}

	return u.GetBucketsFromInfoMap(bucketListInfo, logger)
}

func (u *Utilities) GetBucketsFromInfoMap(bucketListInfo []interface{}, logger *log.CommonLogger) (map[string]string, error) {
	buckets := make(map[string]string)
	for _, bucketInfo := range bucketListInfo {
		bucketInfoMap, ok := bucketInfo.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("bucket info is not of map type.  bucket info=%v", bucketInfo)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		bucketNameInfo, ok := bucketInfoMap[base.BucketNameKey]
		if !ok {
			errMsg := fmt.Sprintf("bucket info does not contain bucket name.  bucket info=%v", bucketInfoMap)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		bucketName, ok := bucketNameInfo.(string)
		if !ok {
			errMsg := fmt.Sprintf("bucket name is not of string type.  bucket name=%v", bucketNameInfo)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		bucketUUIDInfo, ok := bucketInfoMap[base.UUIDKey]
		if !ok {
			errMsg := fmt.Sprintf("bucket info does not contain bucket uuid.  bucket info=%v", bucketInfoMap)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		bucketUUID, ok := bucketUUIDInfo.(string)
		if !ok {
			errMsg := fmt.Sprintf("bucket uuid is not of string type.  bucket uuid=%v", bucketUUIDInfo)
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		buckets[bucketName] = bucketUUID
	}

	return buckets, nil
}

func (u *Utilities) BucketValidationInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
	logger *log.CommonLogger) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
	bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error) {

	return u.bucketValidationInfoInternal(hostAddr, bucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger, false /*external*/)
}

func (u *Utilities) RemoteBucketValidationInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
	logger *log.CommonLogger, useExternal bool) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
	bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error) {

	return u.bucketValidationInfoInternal(hostAddr, bucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger, useExternal)
}

// get a number of fields in bucket for validation purpose
// 1. bucket type
// 2. bucket uuid
// 3. bucket conflict resolution type
// 4. bucket eviction policy
// 5. bucket server vb map
func (u *Utilities) bucketValidationInfoInternal(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
	logger *log.CommonLogger, useExternal bool) (bucketInfo map[string]interface{}, bucketType string, bucketUUID string, bucketConflictResolutionType string,
	bucketEvictionPolicy string, bucketKVVBMap map[string][]uint16, err error) {

	bucketValidationInfoOp := func() error {
		bucketInfo, err = u.GetBucketInfo(hostAddr, bucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
		if err != nil {
			return err
		}

		bucketType, err = u.GetBucketTypeFromBucketInfo(bucketName, bucketInfo)
		if err != nil {
			err = fmt.Errorf("error retrieving BucketType setting on bucket %v. err=%v", bucketName, err)
			return err
		}

		bucketUUID, err = u.GetBucketUuidFromBucketInfo(bucketName, bucketInfo, logger)
		if err != nil {
			err = fmt.Errorf("error retrieving UUID setting on bucket %v. err=%v", bucketName, err)
			return err
		}

		bucketConflictResolutionType, err = u.GetConflictResolutionTypeFromBucketInfo(bucketName, bucketInfo)
		if err != nil {
			err = fmt.Errorf("error retrieving ConflictResolutionType setting on bucket %v. err=%v", bucketName, err)
			return err
		}

		bucketEvictionPolicy, err = u.GetEvictionPolicyFromBucketInfo(bucketName, bucketInfo)
		if err != nil {
			err = fmt.Errorf("error retrieving EvictionPolicy setting on bucket %v. err=%v", bucketName, err)
			return err
		}

		shouldUseTerseInfo, err := u.ShouldUseTerseBucketInfo(bucketInfo, hostAddr, bucketName, useExternal, authMech == base.HttpAuthMechHttps)
		if err != nil {
			err = fmt.Errorf("error checking for the need of terse info for bucket %v: %w", bucketName, err)
			return err
		}

		bucketInfoForKvVBMap := bucketInfo
		if shouldUseTerseInfo {
			terseBucketInfo, err := u.GetTerseBucketInfo(hostAddr, bucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
			if err != nil {
				err = fmt.Errorf("error getting terse info for bucket %v: %w", bucketName, err)
				return err
			}

			bucketInfoForKvVBMap = terseBucketInfo
		}

		bucketKVVBMapPtr, err := u.GetServerVBucketsMap(hostAddr, bucketName, bucketInfoForKvVBMap, nil, nil)
		if err != nil {
			err = fmt.Errorf("error getServerVBucketsMap on bucket %v. err=%v", bucketName, err)
			return err
		}

		bucketKVVBMap = *bucketKVVBMapPtr
		if useExternal {
			u.TranslateKvVbMap(bucketKVVBMap, bucketInfoForKvVBMap)
		}
		return nil
	}

	err = u.ExponentialBackoffExecutorWithOriginalError("BucketValidationInfo", base.BucketInfoOpWaitTime, base.BucketInfoOpMaxRetry, base.BucketInfoOpRetryFactor, bucketValidationInfoOp)
	return
}

func (u *Utilities) GetBucketUuidFromBucketInfo(bucketName string, bucketInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
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

func (u *Utilities) GetClusterUUIDFromDefaultPoolInfo(defaultPoolInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
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

	return u.GetClusterUUIDFromURI(uri)
}

func (u *Utilities) GetClusterUUIDFromURI(uri string) (string, error) {
	// uri is in the form of /pools/default/buckets?uuid=d5dea23aa7ee3771becb3fcdb46ff956
	searchKey := base.UUIDKey + "="
	index := strings.LastIndex(uri, searchKey)
	if index < 0 {
		return "", fmt.Errorf("uri does not contain uuid. uri=%v", uri)
	}
	return uri[index+len(searchKey):], nil
}

func (u *Utilities) GetClusterCompatibilityFromBucketInfo(bucketInfo map[string]interface{}, logger *log.CommonLogger) (int, error) {
	nodeList, err := u.GetNodeListFromInfoMap(bucketInfo, logger)
	if err != nil {
		return 0, err
	}

	clusterCompatibility, err := u.GetClusterCompatibilityFromNodeList(nodeList)
	if err != nil {
		logger.Error(err.Error())
		return 0, err
	}

	return clusterCompatibility, nil
}

// GetNodeExtListFromInfoMap returns 'nodesExt' list from infoMap. It filters and retains only the nodes which has
// data service running in it.
// Returns error if it doesn't exist.
func (u *Utilities) GetNodeExtListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error) {
	if len(infoMap) == 0 {
		return nil, base.ErrorInvalidInput
	}

	nodesExt, ok := infoMap[base.NodeExtKey]
	if !ok {
		errMsg := fmt.Sprintf("%v not found in bucketInfo=%v for terse info", base.NodeExtKey, infoMap)
		return nil, u.BucketInfoParseError(infoMap, errMsg, logger)
	}

	nodesExtArray, ok := nodesExt.([]interface{})
	if !ok {
		errMsg := fmt.Sprintf("nodesExt=%v cannot be parsed as []interface{}, as it is of incompatible type for terse info, its type=%v", nodesExt, reflect.TypeOf(nodesExt))
		return nil, u.BucketInfoParseError(infoMap, errMsg, logger)
	}

	nodesWithOnlyKV := make([]interface{}, 0, len(nodesExtArray))
	for _, node := range nodesExtArray {
		nodeMap, ok := node.(map[string]interface{})
		if !ok {
			u.logger_utils.Warnf("unable to parse node of nodesExt, type=%T, node=%v", node, node)
			continue
		}

		portsObj, ok := nodeMap[base.ServicesKey]
		if !ok {
			u.logger_utils.Warnf("%s is missing in node of nodesExt, node=%v", base.ServicesKey, node)
			continue
		}

		portsMap, ok := portsObj.(map[string]interface{})
		if !ok {
			u.logger_utils.Warnf("unable to parse portsMap of nodesExt, type=%T, node=%v", portsObj, node)
			continue
		}

		_, ok = portsMap[base.KVPortKey]
		if !ok {
			// This node doesn't have data-service. Skip it.
			continue
		}

		nodesWithOnlyKV = append(nodesWithOnlyKV, node)
	}

	return nodesWithOnlyKV, nil
}

func (u *Utilities) GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error) {
	return base.GetNodeListFromInfoMap(infoMap, logger)
}

func (u *Utilities) GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error) {
	return base.GetClusterCompatibilityFromNodeList(nodeList)
}

// Returns a map of hostname: status from the perspective of the node that contains the nodelist
// The key of the map will be "local" hostnames, since it is from the perspective of the node's nodelist
func (u *Utilities) GetClusterHeartbeatStatusFromNodeList(nodeList []interface{}) (map[string]base.HeartbeatStatus, error) {
	if len(nodeList) > 0 {
		heartbeatMap := make(map[string]base.HeartbeatStatus)
		for _, nodeInfoRaw := range nodeList {
			nodeInfo, ok := nodeInfoRaw.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("nodeInfo is of type %v", reflect.TypeOf(nodeInfoRaw))
			}
			statusRaw, ok := nodeInfo[base.StatusKey]
			if !ok {
				return nil, fmt.Errorf("%v does not exist", base.StatusKey)
			}
			statusString, ok := statusRaw.(string)
			if !ok {
				return nil, fmt.Errorf("status is of type %v", reflect.TypeOf(statusRaw))
			}
			hostnameRaw, ok := nodeInfo[base.HostNameKey]
			if !ok {
				return nil, fmt.Errorf("%v does not exist", base.HostNameKey)
			}
			hostname, ok := hostnameRaw.(string)
			if !ok {
				return nil, fmt.Errorf("hostname is of type %v", reflect.TypeOf(hostnameRaw))
			}
			var err error
			heartbeatMap[hostname], err = base.NewHeartbeatStatusFromString(statusString)
			if err != nil {
				return nil, err
			}
		}
		return heartbeatMap, nil
	}
	return nil, nil
}

// Used externally only - returns a list of nodes for management access
// if needHttps is true, returns both http addresses and https addresses
// if needHttps is false, returns http addresses and empty https addresses
func (u *Utilities) GetRemoteNodeAddressesListFromNodeList(nodeList []interface{}, connStr string, needHttps bool, logger *log.CommonLogger, useExternal bool) (base.StringPairList, error) {
	nodeAddressesList := make(base.StringPairList, len(nodeList))
	var hostAddr string
	var hostHttpsAddr string
	var err error
	index := 0

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("node info is not of map type. type of node info=%v", reflect.TypeOf(node))
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}

		hostAddr, err = u.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, false /*isHttps*/, logger, useExternal)
		if err != nil {
			err := fmt.Errorf("cannot get hostname from node info %v. err=%w", nodeInfoMap, err)
			logger.Error(err.Error())
			return nil, err
		}

		if needHttps {
			hostHttpsAddr, err = u.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, true /*isHttps*/, logger, useExternal)
			if err != nil {
				errMsg := fmt.Sprintf("cannot get https hostname from node info err: %v nodeInfoMap: %v", err, nodeInfoMap)
				logger.Error(errMsg)
				return nil, errors.New(errMsg)
			}
		} else {
			hostHttpsAddr = ""
		}

		nodeAddressesList[index] = base.StringPair{hostAddr, hostHttpsAddr}
		index++
	}
	return nodeAddressesList, nil
}

func (u *Utilities) GetHttpsMgtPortFromNodeInfo(nodeInfo map[string]interface{}) (int, error) {
	portsObjRaw, portsObjExists := nodeInfo[base.PortsKey]
	if !portsObjExists {
		return -1, base.ErrorNoPortNumber
	}

	portsObj, ok := portsObjRaw.(map[string]interface{})
	if !ok {
		return -1, base.ErrorNoPortNumber
	}

	sslPort, ok := portsObj[base.SSLPortKey]
	if !ok {
		return -1, base.ErrorNoPortNumber
	}

	sslPortFloat, ok := sslPort.(float64)
	if !ok {
		return -1, base.ErrorNoPortNumber
	}
	return int(sslPortFloat), nil
}

func (u *Utilities) replacePortWithHttpsMgtPort(hostAddr string, nodeInfo map[string]interface{}) (string, error) {
	sslPort, err := u.GetHttpsMgtPortFromNodeInfo(nodeInfo)
	if err != nil {
		return "", err
	}
	hostName := base.GetHostName(hostAddr)
	hostAddr = base.GetHostAddr(hostName, uint16(sslPort))
	return hostAddr, nil
}

func (u *Utilities) GetHostAddrFromNodeInfo(connStr string, nodeInfo map[string]interface{}, isHttps bool, logger *log.CommonLogger, useExternal bool) (string, error) {
	var hostAddr string
	if useExternal {
		var foundExternal bool
		var foundExternalButNeedsPort bool
		// If external info exists, use it
		if externalAddr, externalMgtPort, externalErr := u.GetExternalMgtHostAndPort(nodeInfo, isHttps); externalErr == nil {
			hostAddr = base.GetHostAddr(externalAddr, (uint16)(externalMgtPort))
			foundExternal = true
		} else if externalErr == base.ErrorNoPortNumber {
			// Extract original internal node management port
			if hostname, exists := nodeInfo[base.HostNameKey].(string); exists {
				hostPort, portErr := base.GetPortNumber(hostname)
				if portErr == nil {
					// Combine externalHost:internalPort
					hostAddr = base.GetHostAddr(externalAddr, hostPort)
					if isHttps && hostPort == base.DefaultAdminPort {
						return u.replacePortWithHttpsMgtPort(hostAddr, nodeInfo)
					}
				} else {
					// Original internal address did not have port number, so continue to just have externalAddr[:noPort]
					hostAddr = externalAddr
					foundExternalButNeedsPort = true
				}
			}
			foundExternal = true
		}
		if foundExternal {
			// Verify that the hostAddr can be mapped if necessary
			// We are passing "false" for isTLS because we only use it to check for IPv4/IPv6 and not utilizing the
			// return value itself
			if _, err := base.MapToSupportedIpFamily(hostAddr, false); err != nil {
				return "", err
			}
			// We foundExternal the info from the external address
			if foundExternalButNeedsPort {
				if isHttps {
					// When external addressing is used, the external port could optionally be specified
					// If it's not specified, we'll use the original http or https port
					return u.replacePortWithHttpsMgtPort(hostAddr, nodeInfo)
				} else {
					// This is the case where external addressing section exists
					// within that section the hostname is specified, but no port
					// Using the internal hostnaame and port, we cannot extract the internal port
					// So we'll just use the default port
					hostAddr = base.GetHostAddr(hostAddr, base.DefaultAdminPort)
					return hostAddr, nil
				}
			} else {
				// hostAddr should already be compiled with properly externalMgmtport already
				return hostAddr, nil
			}
		}
	}

	// Get internal node information. If found, it already validated that the hostAddr can be mapped to IP if required
	hostAddr, err := u.getAdminHostAddrFromNodeInfo(connStr, nodeInfo, logger)
	if err != nil {
		err = fmt.Errorf("cannot get admin hostname from node info %v. err=%w", nodeInfo, err)
		logger.Error(err.Error())
		return "", err
	}

	if isHttps {
		// When not using external addressing, it is very highly chances are the admin port is 8091
		// as well as 18091 for secure, which this is going to be doing
		return u.replacePortWithHttpsMgtPort(hostAddr, nodeInfo)
	}
	return hostAddr, nil
}

// Returns:
// 1. External IP
// 2. External kv port (if applicable, -1 if not found)
// 3. Returns nil if port exists - ErrorNoPortNumber if kv (direct) port doesn't exist
// 4. External KvSSL port (if applicable, -1 if not found)
// 5. Returns nil if SSL port exists - ErrorNoPortNumber if SSL port doesn't exist
// Any other errors are considered bad op
func (u *Utilities) GetExternalAddressAndKvPortsFromNodeInfo(nodeInfo map[string]interface{}) (string, int, error, int, error) {
	var hostAddr string
	var portNumber int
	var sslPortNumber int
	var portErr error
	var sslPortErr error

	alternateObjRaw, alternateExists := nodeInfo[base.AlternateKey]
	if !alternateExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	alternateObj, ok := (alternateObjRaw).(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("GetExternalAddressAndKvPortsFromNodeInfo: Unable to convert alternateObj to map[string]interface{}")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	externalObjRaw, externalExists := alternateObj[base.ExternalKey]
	if !externalExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	externalObj, ok := (externalObjRaw).(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("GetExternalAddressAndKvPortsFromNodeInfo: Unable to convert externalObj to map[string]interface{}")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	hostAddrObjRaw, hostAddrObjExists := externalObj[base.HostNameKey]
	if !hostAddrObjExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	hostAddr, ok = (hostAddrObjRaw).(string)
	if !ok {
		u.logger_utils.Errorf("GetExternalAddressAndKvPortsFromNodeInfo: Unable to convert hostAddrObj to string")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	} else if len(hostAddr) == 0 {
		u.logger_utils.Errorf("GetExternalAddressAndKvPortsFromNodeInfo: Empty Hostname")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	portsObjRaw, portsObjExists := externalObj[base.PortsKey]
	if !portsObjExists {
		return hostAddr, -1, base.ErrorNoPortNumber, -1, base.ErrorNoPortNumber
	}

	portErr = base.ErrorNoPortNumber
	sslPortErr = base.ErrorNoPortNumber
	portNumber = -1
	sslPortNumber = -1
	portsObj, ok := portsObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Warnf("Unable to convert portsObj to map[string]interface{}")
	} else {
		// Get the External kv port (internally "direct") port if it's there
		// KV team wants clients to use nodeServices, which means that "direct" is used as an internal naming convention
		// The alternate address fields use "kv" as what "direct" means to traditional XDCR
		kvPortFloat, kvPortExists := portsObj[base.KVPortKey]
		if kvPortExists {
			kvPortIntCheck, ok := kvPortFloat.(float64)
			if ok {
				portNumber = (int)(kvPortIntCheck)
				portErr = nil
			}
		}
		// Get the SSL port if it is there
		sslPort, sslPortExists := portsObj[base.KVSSLPortKey]
		if sslPortExists {
			sslPortIntCheck, ok := sslPort.(float64)
			if ok {
				sslPortNumber = (int)(sslPortIntCheck)
				sslPortErr = nil
			}
		}
	}
	return hostAddr, portNumber, portErr, sslPortNumber, sslPortErr
}

func (u *Utilities) GetExternalMgtHostAndPort(nodeInfo map[string]interface{}, isHttps bool) (string, int, error) {
	var hostAddr string
	var portErr error = base.ErrorNoPortNumber
	var portNumber int = -1

	alternateObjRaw, alternateExists := nodeInfo[base.AlternateKey]
	if !alternateExists {
		return "", -1, base.ErrorResourceDoesNotExist
	}

	alternateObj, ok := alternateObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: unable to cast alternateObj to map[string]interface{}")
		fmt.Printf("GetExternalMgtHostAndPort: unable to cast alternateObj to map[string]interface{}\n")
		return "", -1, base.ErrorResourceDoesNotExist
	}

	externalObjRaw, externalExists := alternateObj[base.ExternalKey]
	if !externalExists {
		fmt.Printf("externalObjRaw does not exist\n")
		return "", -1, base.ErrorResourceDoesNotExist
	}

	externalObj, ok := externalObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: unable to cast externalObj to map[string]interface{}")
		fmt.Printf("GetExternalMgtHostAndPort: unable to cast externalObj to map[string]interface{}\n")
		return "", -1, base.ErrorResourceDoesNotExist
	}

	hostAddrObj, hostAddrObjExists := externalObj[base.HostNameKey]
	if !hostAddrObjExists {
		return "", -1, base.ErrorResourceDoesNotExist
	}

	hostAddr, ok = hostAddrObj.(string)
	if !ok {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: unable to cast hostAddr to string")
		return "", -1, base.ErrorResourceDoesNotExist
	} else if len(hostAddr) == 0 {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: empty hostAddr")
		return "", -1, base.ErrorResourceDoesNotExist
	}

	portsObjRaw, portsObjExists := externalObj[base.PortsKey]
	if !portsObjExists {
		return hostAddr, portNumber, portErr
	}

	portsObj, ok := portsObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: unable to cast portsObj to map[string]interface{}")
		return hostAddr, portNumber, portErr
	}

	var portKey string
	if isHttps {
		portKey = base.SSLMgtPortKey
	} else {
		portKey = base.MgtPortKey
	}

	mgmtObjRaw, mgmtObjExists := portsObj[portKey]
	if !mgmtObjExists {
		return hostAddr, portNumber, portErr
	}

	mgmtObj, ok := mgmtObjRaw.(float64)
	if !ok {
		u.logger_utils.Errorf("GetExternalMgtHostAndPort: unable to cast mgmtObj to float64")
		return hostAddr, portNumber, portErr
	}

	portNumber = (int)(mgmtObj)
	portErr = nil
	return hostAddr, portNumber, portErr
}

// Returns remote node's SSL management port if it exists
func (u *Utilities) getExternalSSLMgtPort(nodeInfo map[string]interface{}) (int, error) {
	alternateObjRaw, alternateExists := nodeInfo[base.AlternateKey]
	if !alternateExists {
		return -1, base.ErrorResourceDoesNotExist
	}

	alternateObj, ok := alternateObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("getExternalSSLMgtPort: unable to cast alternateObj to map[string]interface{}")
		return -1, base.ErrorResourceDoesNotExist
	}

	externalObjRaw, externalExists := alternateObj[base.ExternalKey]
	if !externalExists {
		return -1, base.ErrorResourceDoesNotExist
	}

	externalObj, ok := externalObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("getExternalSSLMgtPort: unable to cast externalObj to map[string]interface{}")
		return -1, base.ErrorResourceDoesNotExist
	}

	portsObjRaw, portsObjExists := externalObj[base.PortsKey]
	if !portsObjExists {
		u.logger_utils.Warnf("Unable to convert portsObj to map[string]interface{}")
		return -1, base.ErrorNoPortNumber
	}

	portsObj, ok := portsObjRaw.(map[string]interface{})
	if !ok {
		return -1, base.ErrorNoPortNumber
	}

	mgmtSSLObjRaw, mgmtSSLExists := portsObj[base.SSLMgtPortKey]
	if !mgmtSSLExists {
		return -1, base.ErrorNoPortNumber
	}

	mgmtSSLObj, ok := mgmtSSLObjRaw.(float64)
	if !ok {
		u.logger_utils.Warnf("Unable to convert portsObj to float64")
		return -1, base.ErrorNoPortNumber
	}

	return (int)(mgmtSSLObj), nil
}

// Returns:
// 1. External Hostname
// 2. capi port
// 3. capi port error
// 4. capi SSL port
// 5. capi SSL port error
func (u *Utilities) getExternalHostAndCapiPorts(nodeInfo map[string]interface{}) (string, int, error, int, error) {
	var hostAddr string
	var capiPort int = -1
	var capiSSLPort int = -1
	var capiPortErr error = base.ErrorNoPortNumber
	var capiSSLPortErr error = base.ErrorNoPortNumber

	alternateObjRaw, alternateExists := nodeInfo[base.AlternateKey]
	if !alternateExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	alternateObj, ok := (alternateObjRaw).(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("getExternalHostAndCapiPorts: Unable to convert alternateObj to map[string]interface{}")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	externalObjRaw, externalExists := alternateObj[base.ExternalKey]
	if !externalExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	externalObj, ok := (externalObjRaw).(map[string]interface{})
	if !ok {
		u.logger_utils.Errorf("getExternalHostAndCapiPorts: Unable to convert externalObj to map[string]interface{}")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	hostAddrObjRaw, hostAddrObjExists := externalObj[base.HostNameKey]
	if !hostAddrObjExists {
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	hostAddr, ok = (hostAddrObjRaw).(string)
	if !ok {
		u.logger_utils.Errorf("getExternalHostAndCapiPorts: Unable to convert hostAddrObj to string")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	} else if len(hostAddr) == 0 {
		u.logger_utils.Errorf("getExternalHostAndCapiPorts: Empty Hostname")
		return "", -1, base.ErrorResourceDoesNotExist, -1, base.ErrorResourceDoesNotExist
	}

	portsObjRaw, portsObjExists := externalObj[base.PortsKey]
	if !portsObjExists {
		return "", -1, base.ErrorNoPortNumber, -1, base.ErrorNoPortNumber
	}

	portsObj, ok := portsObjRaw.(map[string]interface{})
	if !ok {
		u.logger_utils.Warnf("Unable to convert portsObj to map[string]interface{}")
	} else {
		capiPortRaw, capiPortExists := portsObj[base.CapiPortKey]
		if capiPortExists {
			portNumberFloat, ok := (capiPortRaw).(float64)
			if !ok {
				u.logger_utils.Warnf("Unable to convert capiPort to float64")
			} else {
				capiPort = (int)(portNumberFloat)
				capiPortErr = nil
			}
		}
		// Get the SSL port if it is there
		if sslPort, sslPortExists := portsObj[base.CapiSSLPortKey]; sslPortExists {
			sslPortNumberFloat, ok := sslPort.(float64)
			if !ok {
				u.logger_utils.Warnf("Unable to convert capiSSLPort to float64")
			} else {
				capiSSLPort = (int)(sslPortNumberFloat)
				capiSSLPortErr = nil
			}
		}
	}
	return hostAddr, capiPort, capiPortErr, capiSSLPort, capiSSLPortErr
}

func (u *Utilities) getAdminHostAddrFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	hostAddr, err := u.getHostAddrFromNodeInfoInternal(adminHostAddr, nodeInfo, logger)
	if err == base.ErrorNoHostName {
		hostAddr = adminHostAddr
		err = nil
	}
	return hostAddr, err
}

func (u *Utilities) getHostAddrFromNodeInfoInternal(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	var hostAddr string
	var ok bool

	hostAddrObj, ok := nodeInfo[base.HostNameKey]
	if !ok {
		logger.Infof("hostname is missing from node info %v. Host name in remote cluster reference, %v, will be used.\n", nodeInfo, adminHostAddr)
		return "", base.ErrorNoHostName
	} else {
		hostAddr, ok = hostAddrObj.(string)
		if !ok {
			return "", fmt.Errorf("Error getting host address from target cluster %v. host name, %v, is of wrong type\n", adminHostAddr, hostAddrObj)
		}
	}
	// We are passing "false" for isTLS because we only use it to check for IPv4/IPv6 and not utilizing the
	// return value itself
	_, err := base.MapToSupportedIpFamily(hostAddr, false)
	if err != nil {
		// The hostAddr cannot be mapped to the supported IP Address
		return "", err
	}
	return hostAddr, nil
}

// Checks if alternate address setup for the cluster is valid.
// For proper port forwarding based load balancing, it is expected that:
// Each node has an unique FQDN:mgmt port
// Example 1:
// If there are 3 nodes, each of the 3 nodes share the same FQDN, each node needs to have its own unique
// management port number.
// Example 2:
// If there are 3 nodes, each of the 3 nodes do not have unique mgmt port, each of it must has its own unique FQDN
func (u *Utilities) TargetHasSharedExternalHostnameAndMgmtPort(targetBucketInfo map[string]interface{}) (bool, error) {
	uniqueAddresses := make(map[string]bool)

	nodesList, err := u.GetNodeListFromInfoMap(targetBucketInfo, u.logger_utils)
	if err != nil {
		return false, err
	}

	for _, nodeInfoRaw := range nodesList {
		nodeInfo, ok := nodeInfoRaw.(map[string]interface{})
		if !ok {
			u.logger_utils.Warnf("TargetHasSharedExternalHostnameAndMgmtPort: unable to cast nodeInfo as map[string]interface{}, got: %v", nodeInfoRaw)
			return false, fmt.Errorf("expected nodeInfo to be map[string]interface{}, got %T", nodeInfoRaw)
		}

		externalAddr, mgmtPort, mgmtPortErr := u.GetExternalMgtHostAndPort(nodeInfo, false /*isHttps*/)
		switch mgmtPortErr {
		case base.ErrorResourceDoesNotExist:
			// In a proper alternate address setup, all nodes should have alternate address configured
			// If any node is missing an alternate address, return false
			// It is not XDCR's responsibility to ensure that the alternate address is set up properly
			return false, nil
		case base.ErrorNoPortNumber:
			// Use https
			_, mgmtPort, mgmtPortErr = u.GetExternalMgtHostAndPort(nodeInfo, true /*isHttps*/)
			if mgmtPortErr == base.ErrorNoPortNumber {
				// Alternate address behavior is such that if the port is not specified,
				// use the original port number
				mgmtPort = int(base.DefaultAdminPort)
			} else if mgmtPortErr != nil {
				u.logger_utils.Warnf("TargetHasSharedExternalHostnameAndMgmtPort: unable to get external management port from nodeInfo %v, err=%v", nodeInfo, mgmtPortErr)
				return false, mgmtPortErr
			}
		}

		externalAddr = base.GetHostAddr(externalAddr, (uint16)(mgmtPort))
		uniqueAddresses[externalAddr] = true
	}

	// If all nodes share the same alternate address with management port, this means
	// that the load balancing is not properly set up for port forwarding
	if len(uniqueAddresses) == 1 && len(nodesList) > 1 {
		return true, nil
	}
	return false, nil
}

func (u *Utilities) getInternalToExternalMapping(nodesList []interface{}, isTerseInfo bool) (map[string]string, error) {
	internalExternalNodesMap := make(map[string]string)
	var directPort int

	for _, nodeInfoRaw := range nodesList {
		nodeInfo, ok := nodeInfoRaw.(map[string]interface{})
		if !ok {
			u.logger_utils.Warnf("GetIntExtHostNameKVPortTranslationMap unable to cast nodeInfo as map[string]interface{} from: %v", nodeInfoRaw)
			// skip this node
			continue
		}

		internalAddressAndPortRaw, internalAddressOk := nodeInfo[base.HostNameKey]
		if !internalAddressOk {
			u.logger_utils.Warnf("GetIntExtHostNameKVPortTranslationMap unable to retrieve internal host name from %v", nodeInfo)
			// skip this node
			continue
		}

		internalAddressAndPort, ok := (internalAddressAndPortRaw).(string)
		if !ok {
			u.logger_utils.Warnf("GetIntExtHostNameKVPortTranslationMap unable to cast internalAddressAndPort as string: %v", internalAddressAndPortRaw)
			// skip this node
			continue
		}

		internalAddress := base.GetHostName(internalAddressAndPort)

		portsKey := base.PortsKey
		if isTerseInfo {
			portsKey = base.ServicesKey
		}

		// Internally, we care about "direct" (or "kv" for terse bucket info) field
		portsObjRaw, portsExists := nodeInfo[portsKey]
		if !portsExists {
			u.logger_utils.Warnf("Unable to get port for %v (%s)", internalAddress, portsKey)
			// skip this node
			continue
		}
		portsObj, ok := portsObjRaw.(map[string]interface{})
		if !ok {
			u.logger_utils.Warnf("GetIntExtHostNameKVPortTranslationMap unable to cast portsObj as map[string]interface{} from (%s): %v", portsKey, portsObjRaw)
			// skip this node
			continue
		}

		kvPortKey := base.DirectPortKey
		if isTerseInfo {
			kvPortKey = base.KVPortKey
		}

		directPortIface, directPortExists := portsObj[kvPortKey]
		if !directPortExists {
			u.logger_utils.Warnf("Unable to get direct port for %v (%s)", internalAddress, kvPortKey)
			// skip this node
			continue
		}
		directPortFloat, ok := directPortIface.(float64)
		if !ok {
			u.logger_utils.Warnf("GetIntExtHostNameKVPortTranslationMap unable to cast directPort as float (%s)", directPortIface, kvPortKey)
			// skip this node
			continue
		}

		directPort = (int)(directPortFloat)

		internalAddressAndDirectPort := base.GetHostAddr(internalAddress, (uint16)(directPort))

		externalAddress, externalDirectPort, externalErr, externalKvSSLPort, externalKvSSLErr := u.GetExternalAddressAndKvPortsFromNodeInfo(nodeInfo)
		if len(externalAddress) == 0 {
			// no mapping to create
			continue
		}

		mappedPort := -1
		if isTerseInfo {
			switch {
			case externalKvSSLErr == nil:
				// External address and KV SSL port both exist
				mappedPort = externalKvSSLPort
			case errors.Is(externalKvSSLErr, base.ErrorNoPortNumber):
				// External address exists, but KV SSL port does not. Use internal host's port number just like
				// non-terse bucket info behaviour.
				mappedPort = directPort
			}
		} else {
			switch {
			case externalErr == nil:
				// External address and port both exist
				mappedPort = externalDirectPort
			case errors.Is(externalErr, base.ErrorNoPortNumber):
				// External address exists, but port does not. Use internal host's port number
				mappedPort = directPort
			}
		}

		if mappedPort < 0 {
			// there was no mapped port for external address.
			continue
		}

		// there was no mapped port for external address.
		internalExternalNodesMap[internalAddressAndDirectPort] = base.GetHostAddr(externalAddress, (uint16)(mappedPort))
	}

	return internalExternalNodesMap, nil
}

// Note - the translated map should be in the k->v form of:
// 1. internalNodeAddress:directPort -> externalNodeAddress:kvPort for non-terse bucket info
// OR
// 2. internalNodeAddress:directPort -> externalNodeAddress:kvSSLPort for non-terse bucket info
// (2) will be used when externalNodeAddress:kvPort entries of (1) have the same values for all nodes.
func (u *Utilities) GetIntExtHostNameKVPortTranslationMap(bucketInfo map[string]interface{}) (map[string]string, error) {
	if len(bucketInfo) == 0 {
		return nil, base.ErrorInvalidInput
	}

	var nodesList []interface{}

	useTerseInfo, err := u.IsTerseBucketInfo(bucketInfo)
	if err != nil {
		u.logger_utils.Errorf("GetIntExtHostNameKVPortTranslationMap unable to check for terse info intent: %v", err)
		return nil, err
	}

	if useTerseInfo {
		// use the 'nodesExt' key as we need kvSSL port.
		nodesList, err = u.GetNodeExtListFromInfoMap(bucketInfo, u.logger_utils)
		if err != nil {
			return nil, err
		}
	} else {
		// use the 'nodes' key as we do not need kvSSL port.
		nodesList, err = u.GetNodeListFromInfoMap(bucketInfo, u.logger_utils)
		if err != nil {
			return nil, err
		}
	}

	internalExternalNodesMap, err := u.getInternalToExternalMapping(nodesList, useTerseInfo)
	if err != nil {
		return nil, err
	}

	if len(internalExternalNodesMap) == 0 {
		return nil, base.ErrorResourceDoesNotExist
	}

	return internalExternalNodesMap, nil
}

func (u *Utilities) GetHostNameFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, error) {
	hostAddr, err := u.getAdminHostAddrFromNodeInfo(adminHostAddr, nodeInfo, logger)
	if err != nil {
		return "", err
	}
	return base.GetHostName(hostAddr), nil
}

// this method is called when nodeInfo came from the terse bucket call, pools/default/b/[bucketName]
// where hostname in nodeInfo is a host name without port rather than a host address with port
func (u *Utilities) getHostNameWithoutPortFromNodeInfo(adminHostAddr string, nodeInfo map[string]interface{}, logger *log.CommonLogger) (string, bool, error) {
	var missingHostname bool
	hostName, err := u.getHostAddrFromNodeInfoInternal(adminHostAddr, nodeInfo, logger)
	if err == base.ErrorNoHostName {
		missingHostname = true
		hostName = base.GetHostName(adminHostAddr)
		err = nil
	}

	return hostName, missingHostname, err
}

// convenient api for rest calls to local cluster
func (u *Utilities) QueryRestApi(baseURL string,
	path string,
	preservePathEncoding bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	logger *log.CommonLogger) (error, int) {
	return u.QueryRestApiWithAuth(baseURL, path, preservePathEncoding, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, httpCommand, contentType, body, timeout, out, nil, false, logger, nil)
}

func (u *Utilities) EnforcePrefix(prefix string, str string) string {
	var ret_str string = str
	if !strings.HasPrefix(str, prefix) {
		ret_str = prefix + str
	}
	return ret_str
}

func (u *Utilities) RemovePrefix(prefix string, str string) string {
	ret_str := strings.Replace(str, prefix, "", 1)
	return ret_str
}

// this expects the baseURL doesn't contain username and password.
// The same routine QueryRestApiWithAuth is used to contact source and target clusters. The caller should ensure that the right
// params are passed based on if the caller intends to connect to source or target cluster.
// 1. To contact the source cluster: Credentials are fetched from securitySvc (i.e. cbauth).
// The caller should pass in clientCertKeyPair stored in the security service and pass in empty clientCertificate and clientKey. This is
// because there client certificates could be encrypted using passphrase.
// 2. To contact the target cluster: Credentials are most likely fetched from remote cluster reference. clientCertificate and clientKey should be passed
// from remote cluster reference and need to skip clientCertKeyPair.
// In short, the caller has to either pass-in clientCertificate and clientKey OR clientCertKeyPair. But not both.
func (u *Utilities) QueryRestApiWithAuth(
	baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	authMech base.HttpAuthMech,
	certificate []byte,
	san_in_certificate bool,
	clientCertificate []byte,
	clientKey []byte,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger,
	clientCertKeyPair []tls.Certificate,
) (err error, statusCode int) {
	var http_client *http.Client
	if authMech != base.HttpAuthMechScramSha {
		var req *http.Request
		http_client, req, err = u.prepareForRestCall(baseURL, path, preservePathEncoding, username, password, authMech, certificate, san_in_certificate, clientCertificate, clientKey, httpCommand, contentType, body, client, logger, clientCertKeyPair)
		if err != nil {
			return
		}
		err, statusCode = u.doRestCall(req, timeout, out, http_client, logger)
	} else {
		err, statusCode, http_client = u.queryRestApiWithScramShaAuth(baseURL, path, preservePathEncoding, username, password, httpCommand, contentType, body, timeout, out, client, logger)

	}
	u.cleanupAfterRestCall(keep_client_alive, err, statusCode, http_client, logger)
	return
}

func (u *Utilities) queryRestApiWithScramShaAuth(
	baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	logger *log.CommonLogger) (error, int, *http.Client) {

	logger.Debugf("SCRAM-SHA authentication for user %v%v%v, baseURL=%v, path=%v\n", base.UdTagBegin, username, base.UdTagEnd, baseURL, path)

	URL, err := u.constructURL(baseURL, path, preservePathEncoding, base.HttpAuthMechScramSha)
	if err != nil {
		return err, 0, nil
	}

	req, err := scramsha.NewRequest(httpCommand,
		// URL.String() is adequate since scramSha is always called with preservePathEncoding set to false
		URL.String(),
		strings.NewReader(string(body)))
	if err != nil {
		return err, 0, nil
	}

	if timeout == 0 {
		timeout = base.DefaultHttpTimeout
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	} else {
		client.Timeout = timeout
	}

	res, err := scramsha.DoScramSha(req, username, password, client)
	statusCode := 0
	if res != nil {
		statusCode = res.StatusCode
	}
	if err != nil {
		return fmt.Errorf("Received error when making SCRAM-SHA connection. baseURL=%v, path=%v, err=%v", baseURL, path, err), statusCode, client
	}

	err = u.parseResponseBody(res, out, logger)
	return err, statusCode, client

}

// The caller is expected to pass one of clientCertificate or clientCertKeyPair.
// clientCertKeyPair is the "processed" form of client cert + client key.
func (u *Utilities) GetAuthMode(username string, clientCertificate []byte, path string, authMech base.HttpAuthMech, clientCertKeyPair []tls.Certificate) base.UserAuthMode {
	userAuthMode := base.UserAuthModeNone

	noClientCert := len(clientCertificate) == 0 && len(clientCertKeyPair) == 0
	if len(username) == 0 && noClientCert && path != base.SSLPortsPath {
		// username and clientCertificate can be both empty only when
		// 1. this is a local http call to the same node
		// or 2. this is a call to /nodes/self/xdcrSSLPorts on target to retrieve ssl port for subsequent https calls
		// treat case 1 separately, since we will need to set local user auth in http request
		userAuthMode = base.UserAuthModeLocal
	} else {
		// for http calls to remote target, set username and password in http request header if
		// 1. username has been provided
		// and 2. scram sha authentication is not used
		if len(username) != 0 && authMech != base.HttpAuthMechScramSha {
			userAuthMode = base.UserAuthModeBasic
		}
	}
	return userAuthMode
}

func (u *Utilities) prepareForRestCall(baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	authMech base.HttpAuthMech,
	certificate []byte,
	san_in_certificate bool,
	clientCertificate []byte,
	clientKey []byte,
	httpCommand string,
	contentType string,
	body []byte,
	client *http.Client,
	logger *log.CommonLogger,
	clientCertKeyPair []tls.Certificate,
) (*http.Client, *http.Request, error) {
	var l *log.CommonLogger = u.loggerForFunc(logger)
	var ret_client *http.Client = client

	userAuthMode := u.GetAuthMode(username, clientCertificate, path, authMech, clientCertKeyPair)

	req, host, err := u.ConstructHttpRequest(baseURL, path, preservePathEncoding, username, password, authMech, userAuthMode, httpCommand, contentType, body, l)
	if err != nil {
		return nil, nil, err
	}

	if ret_client == nil {
		ret_client, err = u.GetHttpClient(username, authMech, certificate, san_in_certificate, clientCertificate, clientKey, host, l, clientCertKeyPair)
		if err != nil {
			// req body could be long and unreadable... print just the header
			redactedReq := base.CloneAndTagHttpRequest(req)
			l.Errorf("Failed to get client for request, err=%v, req=%v\n", err, redactedReq.Header)
			return nil, nil, err
		}
	}
	return ret_client, req, nil
}

func (u *Utilities) cleanupAfterRestCall(keep_client_alive bool, err error, statusCode int, client *http.Client, logger *log.CommonLogger) {
	if !keep_client_alive || u.IsSeriousNetError(err) || u.isFatalStatusCode(statusCode) {
		if client != nil && client.Transport != nil {
			transport, ok := client.Transport.(*http.Transport)
			if ok {
				if u.IsSeriousNetError(err) {
					logger.Debugf("Encountered %v, close all idle connections for this http client.\n", err)
				}
				transport.CloseIdleConnections()
			}
		}
	}
}

func (u *Utilities) doRestCall(req *http.Request,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	logger *log.CommonLogger) (error, int) {
	if timeout > 0 {
		client.Timeout = timeout
	} else if client.Timeout != base.DefaultHttpTimeout {
		client.Timeout = base.DefaultHttpTimeout
	}

	res, err := client.Do(req)
	if err == nil && res != nil {
		err = u.parseResponseBody(res, out, logger)
		return err, res.StatusCode
	}

	return err, 0

}

func (u *Utilities) parseResponseBody(res *http.Response, out interface{}, logger *log.CommonLogger) (err error) {
	var l *log.CommonLogger = u.loggerForFunc(logger)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
		var bod []byte
		if res.ContentLength == 0 {
			// If res.Body is empty, json.Unmarshal on an empty Body will return the error "unexpected end of JSON input"
			// Return a more specific error here so upstream callers can handle it
			err = base.ErrorResourceDoesNotExist
			return
		}
		bod, err = ioutil.ReadAll(io.LimitReader(res.Body, res.ContentLength))
		if err != nil {
			l.Errorf("Failed to read response body, err=%v\n res=%v\n", err, res)
			return
		}
		if out != nil {
			err = json.Unmarshal(bod, out)
			if err != nil {
				if strings.Contains(string(bod), base.RESTNsServerNotFound) ||
					strings.Contains(string(bod), strings.ToLower(base.RESTNsServerNotFound)) ||
					res.StatusCode == http.StatusNotFound {
					l.Errorf("Original REST request (%v) received %v response. The URL may be incorrect or requested resource no longer exists", res.Request.URL, string(bod))
				} else {
					l.Errorf("Failed to unmarshal the response as json, err=%v, bod=%v\n res=%v\n", err, string(bod), res)
				}
				out = bod
				return
			}
		}
	}
	return
}

// convenient api for rest calls to local cluster
func (u *Utilities) InvokeRestWithRetry(baseURL string,
	path string,
	preservePathEncoding bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger, num_retry int) (error, int) {
	return u.InvokeRestWithRetryWithAuth(baseURL, path, preservePathEncoding, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, true, httpCommand, contentType, body, timeout, out, client, keep_client_alive, logger, num_retry)
}

func (u *Utilities) InvokeRestWithRetryWithAuth(baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	authMech base.HttpAuthMech,
	certificate []byte,
	san_in_certificate bool,
	clientCertificate []byte,
	clientKey []byte,
	insecureSkipVerify bool,
	httpCommand string,
	contentType string,
	body []byte,
	timeout time.Duration,
	out interface{},
	client *http.Client,
	keep_client_alive bool,
	logger *log.CommonLogger, num_retry int) (err error, statusCode int) {

	var http_client *http.Client = nil
	var req *http.Request = nil
	backoff_time := 500 * time.Millisecond

	for i := 0; i < num_retry; i++ {
		if authMech != base.HttpAuthMechScramSha {
			http_client, req, err = u.prepareForRestCall(baseURL, path, preservePathEncoding, username, password, authMech, certificate, san_in_certificate, clientCertificate, clientKey, httpCommand, contentType, body, client, logger, nil)
			if err == nil {
				err, statusCode = u.doRestCall(req, timeout, out, http_client, logger)
			}

			if err == nil {
				break
			}
		} else {
			err, statusCode, http_client = u.queryRestApiWithScramShaAuth(baseURL, path, preservePathEncoding, username, password, httpCommand, contentType, body, timeout, out, client, logger)
			if err == nil {
				break
			}
		}

		logger.Errorf("Received error when making rest call or unmarshalling data. baseURL=%v, path=%v, err=%v, statusCode=%v, num_retry=%v\n", baseURL, path, err, statusCode, i)

		//cleanup the idle connection if the error is serious network error
		u.cleanupAfterRestCall(true /*keep_client_alive*/, err, statusCode, http_client, logger)

		//backoff
		backoff_time = backoff_time + backoff_time
		time.Sleep(backoff_time)
	}

	return

}

func (u *Utilities) GetHttpClient(username string, authMech base.HttpAuthMech, certificate []byte, san_in_certificate bool, clientCertificate, clientKey []byte, ssl_con_str string, logger *log.CommonLogger, clientCertKeyPair []tls.Certificate) (*http.Client, error) {
	var client *http.Client
	if authMech == base.HttpAuthMechHttps {
		//using a separate tls connection to verify certificate
		//it can be changed in 1.4 when DialTLS is avaialbe in http.Transport
		conn, tlsConfig, err := base.MakeTLSConn(ssl_con_str, username, certificate, san_in_certificate, clientCertificate, clientKey, logger, clientCertKeyPair)
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

// this expect the baseURL doesn't contain username and password
func (u *Utilities) ConstructHttpRequest(
	baseURL string,
	path string,
	preservePathEncoding bool,
	username string,
	password string,
	authMech base.HttpAuthMech,
	userAuthMode base.UserAuthMode,
	httpCommand string,
	contentType string,
	body []byte,
	logger *log.CommonLogger) (*http.Request, string, error) {
	url, err := u.constructURL(baseURL, path, preservePathEncoding, authMech)
	if err != nil {
		return nil, "", err
	}

	var l *log.CommonLogger = u.loggerForFunc(logger)

	req, err := http.NewRequest(httpCommand, url.String(), bytes.NewBuffer(body))
	if err != nil {
		return nil, "", err
	}

	if preservePathEncoding {
		// get the original Opaque back
		req.URL.Opaque = url.Opaque
	}

	if contentType == "" {
		contentType = base.DefaultContentType
	}
	req.Header.Set(base.ContentType, contentType)

	req.Header.Set(base.UserAgent, base.GoxdcrUserAgent)

	switch userAuthMode {
	case base.UserAuthModeLocal:
		err := cbauth.SetRequestAuth(req)
		if err != nil {
			l.Errorf("Failed to set authentication to request. err=%v\n", err)
			l.Debugf("req=%v\n", req)
			return nil, "", err
		}
	case base.UserAuthModeBasic:
		req.SetBasicAuth(username, password)
	case base.UserAuthModeNone:
		// no op
	default:
		return nil, "", fmt.Errorf("Invalid userAuthMode %v", userAuthMode)
	}

	//TODO: log request would log password barely
	l.Debugf("http request=%v\n", req)

	return req, url.Host, nil
}

func (u *Utilities) constructURL(baseURL string,
	path string,
	preservePathEncoding bool,
	authMech base.HttpAuthMech) (*url.URL, error) {

	var baseURL_new string
	if authMech == base.HttpAuthMechHttps {
		baseURL_new = u.EnforcePrefix("https://", baseURL)
	} else {
		baseURL_new = u.EnforcePrefix("http://", baseURL)
	}
	url, err := couchbase.ParseURL(baseURL_new)
	if err != nil {
		return nil, err
	}

	if !preservePathEncoding {
		if q := strings.Index(path, "?"); q > 0 {
			url.Path = path[:q]
			url.RawQuery = path[q+1:]
		} else {
			url.Path = path
		}
	} else {
		// use url.Opaque to preserve encoding
		url.Opaque = "//"

		index := strings.Index(baseURL_new, "//")
		if index < len(baseURL_new)-2 {
			url.Opaque += baseURL_new[index+2:]
		}
		url.Opaque += path
	}
	return url, nil
}

// encode http request into wire format
// it differs from HttpRequest.Write() in that it preserves the Content-Length in the header,
// and ignores Body in request
func (u *Utilities) EncodeHttpRequest(req *http.Request) ([]byte, error) {
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
			reqBytes = u.EncodeHttpRequestHeader(reqBytes, key, value[0])
		} else {
			reqBytes = u.EncodeHttpRequestHeader(reqBytes, key, "")
		}
	}
	if !hasHost {
		// ensure that host name is in header
		reqBytes = u.EncodeHttpRequestHeader(reqBytes, "Host", req.Host)
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

func (u *Utilities) EncodeHttpRequestHeader(reqBytes []byte, key, value string) []byte {
	reqBytes = append(reqBytes, []byte(key)...)
	reqBytes = append(reqBytes, []byte(": ")...)
	reqBytes = append(reqBytes, []byte(value)...)
	reqBytes = append(reqBytes, []byte("\r\n")...)
	return reqBytes
}

func (u *Utilities) IsSeriousNetError(err error) bool {
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

// statusCode that requires connections to be dropped and recreated
// basically all statusCodes that are larger than 400 are fatal
func (u *Utilities) isFatalStatusCode(statusCode int) bool {
	return statusCode > http.StatusBadRequest
}

func (u *Utilities) NewTCPConn(hostName string) (*net.TCPConn, error) {
	conn, err := base.DialTCPWithTimeout(base.NetTCP, hostName)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("Failed to set up connection to %v", hostName)
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		// should never get here
		conn.Close()
		return nil, fmt.Errorf("The connection to %v returned is not TCP type", hostName)
	}

	// same settings as erlang xdcr
	err = tcpConn.SetKeepAlive(true)
	if err == nil {
		err = tcpConn.SetKeepAlivePeriod(base.KeepAlivePeriod)
	}
	if err == nil {
		err = tcpConn.SetNoDelay(false)
	}

	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf("Error setting options on the connection to %v. err=%v", hostName, err)
	}

	return tcpConn, nil
}

/**
 * Executes a anonymous function that returns an error. If the error is non nil, retry with exponential backoff.
 * Returns base.ErrorFailedAfterRetry + the last recorded error if operation times out, nil otherwise.
 * Max retries == the times to retry in additional to the initial try, should the initial try fail
 * initialWait == Initial time with which to start
 * Factor == exponential backoff factor based off of initialWait
 */
func (u *Utilities) ExponentialBackoffExecutor(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc) error {
	opErr := u.ExponentialBackoffExecutorWithOriginalError(name, initialWait, maxRetries, factor, op)
	if opErr == nil {
		return nil
	} else {
		opErr = fmt.Errorf("%v %v Last error: %v", name, base.ErrorFailedAfterRetry.Error(), opErr.Error())
		return opErr
	}
}

func (u *Utilities) ExponentialBackoffExecutorWithOriginalError(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc) (err error) {
	waitTime := initialWait
	for i := 0; i <= maxRetries; i++ {
		err = op()
		if err == nil {
			return nil
		} else if i != maxRetries {
			u.logger_utils.Warnf("ExponentialBackoffExecutor for %v encountered error (%v). Sleeping %v\n",
				name, err.Error(), waitTime)
			time.Sleep(waitTime)
			waitTime *= time.Duration(factor)
		}
	}
	return err
}

/*
 * This method has an additional parameter, finCh, than ExponentialBackoffExecutor. When finCh is closed,
 * this method can abort earlier.
 */
func (u *Utilities) ExponentialBackoffExecutorWithFinishSignal(name string, initialWait time.Duration, maxRetries int, factor int, op ExponentialOpFunc2, param interface{}, finCh chan bool) (interface{}, error) {
	waitTime := initialWait
	var result interface{}
	var err error
	for i := 0; i <= maxRetries; i++ {
		select {
		case <-finCh:
			err = fmt.Errorf("ExponentialBackoffExecutorWithFinishSignal for %v aborting %v\n", name, base.FinClosureStr)
			u.logger_utils.Warnf(err.Error())
			return nil, err
		default:
			result, err = op(param)
			if err == nil {
				return result, nil
			} else if i != maxRetries {
				u.logger_utils.Warnf("ExponentialBackoffExecutorWithFinishSignal for %v encountered error (%v). Sleeping %v\n",
					name, err.Error(), waitTime)
				base.WaitForTimeoutOrFinishSignal(waitTime, finCh)
				waitTime *= time.Duration(factor)
			}
		}
	}
	err = fmt.Errorf("%v %v Last error: %v", name, base.ErrorFailedAfterRetry.Error(), err.Error())
	return nil, err
}

// Get security related settings from target
// - authentication mechanism to use when making http[s] connections to target ns_server
// This method used ShortHttpTimeout because it is either called from remote cluster rest API,
// where a prompt response is required to keep the rest request from timing out,
// or called from remote cluster reference refresh code, where a pre-mature timeout can be tolerated
// This method also returns defaultPoolInfo of target for more flexibility
// CALLER BEWARE: defaultPoolInfo is returned ONLY when either scram sha or ssl is enabled, so as to avoid unnecessary work
func (u *Utilities) GetSecuritySettingsAndDefaultPoolInfo(hostAddr, hostHttpsAddr, username, password string, certificate, clientCertificate, clientKey []byte, scramShaEnabled bool, logger *log.CommonLogger) (httpAuthMech base.HttpAuthMech, defaultPoolInfo map[string]interface{}, statusCode int, err error) {
	if !scramShaEnabled && len(certificate) == 0 {
		// security settings are irrelevant if we are not using scram sha or ssl
		// note that a nil defaultPoolInfo is returned in this case
		return base.HttpAuthMechPlain, nil, statusCode, nil
	}

	if scramShaEnabled {
		// if scram sha is enabled, we will first try to connect to target ns_server using scram sha authentication
		// even if certificate/clientCert have been provided, we will not use them here because they are not needed by scram sha authentication
		defaultPoolInfo, statusCode, err = u.GetDefaultPoolInfoUsingScramSha(hostAddr, username, password, logger)
		if err == nil {
			httpAuthMech = base.HttpAuthMechScramSha
		} else if err != TargetMayNotSupportScramShaError {
			return base.HttpAuthMechPlain, nil, statusCode, err
		} else {
			if len(certificate) == 0 {
				// certificate not provided, cannot fall back to https. return error right away
				// Include statusCode in error message to get audit logged for http request
				auditStatusStr := fmt.Sprintf(base.AuditStatusFmt, statusCode)
				return base.HttpAuthMechPlain, nil, statusCode,
					fmt.Errorf("Cannot connect to target %v using \"half\" secure mode. Received unauthorized error (%v) when using Scram-Sha authentication. Cannot use https because server certificate has not been provided.",
						hostAddr, auditStatusStr)
			} else {
				// proceed to fall back to https
			}
		}
	}

	if defaultPoolInfo == nil {
		// if we get here, either scram sha is not enabled, or scram sha is enabled and target ns_server returned 401 error on our scram sha attempt
		// either way, it is implied that certificate has been provided. use https to connect to target
		defaultPoolInfo, statusCode, err = u.GetDefaultPoolInfoUsingHttps(hostHttpsAddr, username, password,
			certificate, clientCertificate, clientKey, logger)
		if err == nil {
			httpAuthMech = base.HttpAuthMechHttps
		} else {
			return base.HttpAuthMechPlain, nil, statusCode, err
		}
	}

	// at this point, we have a valid defaultPoolInfo, an httpAuthMech that worked for the host
	// derive httpAuthMech and certificate related settings from defaultPoolInfo

	nodeList, err := u.GetNodeListFromInfoMap(defaultPoolInfo, logger)
	if err != nil || len(nodeList) == 0 {
		err = fmt.Errorf("Can't get nodes information for cluster %v, err=%v", hostAddr, err)
		return base.HttpAuthMechPlain, nil, statusCode, err
	}

	clusterCompatibility, err := u.GetClusterCompatibilityFromNodeList(nodeList)
	if err != nil {
		return base.HttpAuthMechPlain, nil, statusCode, err
	}

	targetHasScramShaSupport := base.IsClusterCompatible(clusterCompatibility, base.VersionForHttpScramShaSupport)
	if scramShaEnabled && targetHasScramShaSupport && httpAuthMech != base.HttpAuthMechScramSha {
		// do not fall back to https if target is vulcan and up
		return base.HttpAuthMechPlain, nil, statusCode, fmt.Errorf("Failed to retrieve security settings from host=%v using SCRAM-SHA authentication. Please check whether SCRAM-SHA is enabled on target.", hostAddr)
	}

	if scramShaEnabled && !targetHasScramShaSupport && httpAuthMech == base.HttpAuthMechScramSha {
		// Cluster is not ScramSha compatible. We need to fallback to https.
		// Before doing so, we will get default pool using https again to make sure it works
		defaultPoolInfo, statusCode, err = u.GetDefaultPoolInfoUsingHttps(hostHttpsAddr, username, password,
			certificate, clientCertificate, clientKey, logger)
		if err == nil {
			httpAuthMech = base.HttpAuthMechHttps
		} else {
			return base.HttpAuthMechPlain, nil, statusCode, err
		}
	}
	return httpAuthMech, defaultPoolInfo, statusCode, nil
}

func (u *Utilities) GetDefaultPoolInfoUsingScramSha(hostAddr, username, password string, logger *log.CommonLogger) (map[string]interface{}, int, error) {
	defaultPoolInfo := make(map[string]interface{})
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.DefaultPoolPath, false, username, password, base.HttpAuthMechScramSha, nil /*certificate*/, false /*sanInCertificate*/, nil /*clientCertificate*/, nil /*clientKey*/, base.MethodGet, "", nil, base.ShortHttpTimeout, &defaultPoolInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusOK {
		// target supports scram sha
		return defaultPoolInfo, statusCode, nil
	} else if statusCode == http.StatusUnauthorized {
		// unauthorized error could be returned when target ns_server is pre-vulcan and does not support scram sha.
		// return a specific error to allow caller to fall back to https
		return nil, statusCode, TargetMayNotSupportScramShaError
	} else {
		return nil, statusCode, fmt.Errorf("Failed to retrieve security settings from host=%v using scram sha, err=%v, statusCode=%v", hostAddr, err, statusCode)
	}
}

func (u *Utilities) GetDefaultPoolInfoUsingHttps(hostHttpsAddr, username, password string,
	certificate []byte, clientCertificate, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, int, error) {
	defaultPoolInfo := make(map[string]interface{})

	// we do not know the correct values of sanInCertificate. set sanInCertificate set to true for better security
	err, statusCode := u.QueryRestApiWithAuth(hostHttpsAddr, base.DefaultPoolPath, false, username, password, base.HttpAuthMechHttps, certificate, true /*sanInCertificate*/, clientCertificate, clientKey, base.MethodGet, "", nil, base.ShortHttpTimeout, &defaultPoolInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusOK {
		return defaultPoolInfo, statusCode, nil
	} else {
		if err != nil && strings.Contains(err.Error(), base.NoIpSANErrMsg) {
			// if the error is about certificate not containing IP SANs, it could be that the target cluster is of an old version
			// make a second try with sanInCertificate set to false
			// after we retrieve target cluster version, we will then re-set sanInCertificate to the appropriate value
			logger.Debugf("Received certificate validation error from %v. Target may be an old version that does not support SAN in certificates. Retrying connection to target using sanInCertificate = false.", hostHttpsAddr)
			err, statusCode = u.QueryRestApiWithAuth(hostHttpsAddr, base.DefaultPoolPath, false, username, password, base.HttpAuthMechHttps, certificate, false /*sanInCertificate*/, clientCertificate, clientKey, base.MethodGet, "", nil, base.ShortHttpTimeout, &defaultPoolInfo, nil, false, logger, nil)
			if err == nil && statusCode == http.StatusOK {
				return defaultPoolInfo, statusCode, nil
			} else if statusCode == http.StatusUnauthorized {
				return nil, statusCode, u.getUnauthorizedError(username)
			} else {
				// if the second try still fails, return error
				return nil, statusCode, fmt.Errorf("Failed to retrieve security settings from host=%v, err=%v, statusCode=%v", hostHttpsAddr, err, statusCode)
			}
		} else if statusCode == http.StatusUnauthorized {
			return nil, statusCode, u.getUnauthorizedError(username)
		} else {
			return nil, statusCode, fmt.Errorf("Failed to retrieve security settings from host=%v, err=%v, statusCode=%v", hostHttpsAddr, err, statusCode)
		}
	}
}

// Given the KVVBMap, translate the map so that the server keys are replaced with external server keys, if applicable
func (u *Utilities) TranslateKvVbMap(kvVBMap base.KvVBMapType, targetBucketInfo map[string]interface{}) {
	translationMap, translationErr := u.GetIntExtHostNameKVPortTranslationMap(targetBucketInfo)
	if translationErr != nil && translationErr != base.ErrorResourceDoesNotExist {
		u.logger_utils.Warnf("Error constructing internal -> external address translation table. err=%v", translationErr)
	} else if translationErr == nil {
		(base.BucketKVVbMap)(kvVBMap).ReplaceInternalWithExternalHosts(translationMap)
	}
}

func (u *Utilities) ReplaceCouchApiBaseObjWithExternals(couchApiBase string, nodeInfo map[string]interface{}) string {
	if len(couchApiBase) == 0 {
		return couchApiBase
	}

	extHost, extCapi, extCapiErr, extCapiSSL, extCapiSSLErr := u.getExternalHostAndCapiPorts(nodeInfo)
	if len(extHost) > 0 {
		// "couchApiBaseHTTPS": "https://127.0.0.1:19502/b2%2B746a570d364cf609ac11572f8c8c2608",
		url, err := url.Parse(couchApiBase)
		if err != nil || !url.IsAbs() {
			u.logger_utils.Errorf("Unable to parse URL string for CouchApiBase: %v", err)
			return couchApiBase
		}
		var isHttps bool = strings.HasPrefix(couchApiBase, "https")
		var leadingHttpString string
		if isHttps {
			leadingHttpString = "https://"
		} else {
			leadingHttpString = "http://"
		}

		// Now strip out the http(s)://host:port/
		var leadingPrefix string
		leadingHostName := url.Hostname()
		leadingPort := url.Port()
		if len(leadingPort) > 0 {
			leadingPrefix = fmt.Sprintf("%s%s:%s/", leadingHttpString, leadingHostName, leadingPort)
		} else {
			leadingPrefix = fmt.Sprintf("%s%s/", leadingHttpString, leadingHostName)
		}
		strippedCouchApiBase := strings.TrimPrefix(couchApiBase, leadingPrefix)

		// Now recompile
		var recompiledUrl string
		var recompiledHostToUse string = extHost
		var recompiledPortToUse string
		if isHttps && extCapiSSLErr == nil {
			recompiledPortToUse = fmt.Sprintf("%v", extCapiSSL)
		} else if !isHttps && extCapiErr == nil {
			recompiledPortToUse = fmt.Sprintf("%v", extCapi)
		} else {
			recompiledPortToUse = leadingPort
		}
		if len(recompiledPortToUse) == 0 {
			recompiledUrl = fmt.Sprintf("%s%s/%s", leadingHttpString, recompiledHostToUse, strippedCouchApiBase)
		} else {
			recompiledUrl = fmt.Sprintf("%s%s:%s/%s", leadingHttpString, recompiledHostToUse, recompiledPortToUse, strippedCouchApiBase)
		}
		return recompiledUrl
	}
	return couchApiBase
}

func (u *Utilities) getUnauthorizedError(username string) error {
	errMsg := "Received unauthorized error from target. Please double check user credentials."
	// if username has not been specified [implying that client certificate has been provided and is being used]
	// unauthorized error could also be returned if target has client cert auth setting set to disable
	if len(username) == 0 {
		errMsg += " Since client certificate is being used, please ensure that target is version 5.5 and up and has client certificate authentication setting set to \"enable\" or \"mandatory\"."
	}

	return errors.New(errMsg)
}

// Verifies whether target bucket is still valid
func (u *Utilities) VerifyTargetBucket(targetBucketName, targetBucketUuid string, remoteClusterRef *metadata.RemoteClusterReference, logger *log.CommonLogger) error {
	connStr, err := remoteClusterRef.MyConnectionStr()
	if err != nil {
		return err
	}

	username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, err := remoteClusterRef.MyCredentials()
	if err != nil {
		return err
	}

	bucketInfo, err := u.GetBucketInfo(connStr, targetBucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, logger)
	if err != nil {
		return err
	}

	if targetBucketUuid == "" {
		return nil
	}

	extractedUuid, err := u.GetBucketUuidFromBucketInfo(targetBucketName, bucketInfo, logger)
	if err != nil {
		return err
	}

	if extractedUuid != targetBucketUuid {
		logger.Warnf("Bucket %v UUID has changed from %v to %v, indicating a bucket deletion and recreation", targetBucketName, targetBucketUuid, extractedUuid)
		return BucketRecreatedError
	}

	return nil
}

func (u *Utilities) GetHostNamesFromBucketInfo(bucketInfo map[string]interface{}) ([]string, error) {
	nodeListObj, ok := bucketInfo[base.NodesKey]
	if !ok {
		return nil, fmt.Errorf("Error getting %v from bucket info %v", base.NodesKey, bucketInfo)
	}
	nodeList, ok := nodeListObj.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Node list is of wrong type. nodeList=%v", nodeListObj)
	}
	var retList []string
	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Error getting nodeInfoMap from node list %v", nodeList)
		}
		hostNameObj, ok := nodeInfoMap[base.HostNameKey]
		if !ok {
			return nil, fmt.Errorf("Error getting %v from nodeInfoMap %v", base.HostNameKey, nodeInfoMap)
		}
		hostName, ok := hostNameObj.(string)
		if ok {
			retList = append(retList, hostName)
		}
	}
	return retList, nil
}

func (u *Utilities) GetCurrentHostnameFromBucketInfo(bucketInfo map[string]interface{}) (string, error) {
	nodeListObj, ok := bucketInfo[base.NodesKey]
	if !ok {
		return "", fmt.Errorf("Error getting %v from bucket info %v", base.NodesKey, bucketInfo)
	}
	nodeList, ok := nodeListObj.([]interface{})
	if !ok {
		return "", fmt.Errorf("Node list is of wrong type. nodeList=%v", nodeListObj)
	}
	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			return "", fmt.Errorf("Error getting nodeInfoMap from node list %v", nodeList)
		}
		thisNode, ok := nodeInfoMap[base.ThisNodeKey]
		if !ok || thisNode.(bool) != true {
			continue
		}
		hostNameObj, ok := nodeInfoMap[base.HostNameKey]
		if !ok {
			return "", fmt.Errorf("Error getting %v from nodeInfoMap %v", base.HostNameKey, nodeInfoMap)
		}
		hostName, ok := hostNameObj.(string)
		if !ok {
			return "", fmt.Errorf("Hostname %v should be string type.", hostNameObj)
		}
		return hostName, nil
	}
	return "", fmt.Errorf("Failed to get current server hostname from bucket info %v", bucketInfo)
}

func (u *Utilities) GetCollectionsManifest(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (*metadata.CollectionsManifest, error) {
	manifestInfo := make(map[string]interface{})
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.DefaultPoolBucketsPath+bucketName+base.CollectionsManifestPath, false, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, base.MethodGet, "", nil, 0, &manifestInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusOK {
		manifest, err := metadata.NewCollectionsManifestFromMap(manifestInfo)
		return &manifest, err
	}
	if statusCode == http.StatusNotFound {
		u.logger_utils.Warnf("Getting collection manifest from %v bucket %v resulted in statusNotFound", hostAddr, bucketName)
		return nil, u.GetNonExistentBucketError()
	} else {
		u.logger_utils.Errorf("Failed to get manifest for bucket '%v'. host=%v, err=%v, statusCode=%v", bucketName, hostAddr, err, statusCode)
		return nil, fmt.Errorf("Failed to get manifest info")
	}
}

func (u *Utilities) NewDataPool() base.DataPool {
	return base.NewDataPool()
}

func (u *Utilities) composeNsServerDocGetPath(bucketName string, ns *base.CollectionNamespace, docId string) string {
	var path = base.DefaultPoolBucketsPath + bucketName
	var docPath = base.DocsPath + docId
	if ns.IsDefault() {
		path += docPath
	} else {
		path += base.ScopesPath + ns.ScopeName + base.CollectionsPath + ns.CollectionName + docPath
	}
	return path
}

func (u *Utilities) StartDiagStopwatch(id string, threshold time.Duration) func() time.Duration {
	startTime := time.Now()
	return func() time.Duration {
		duration := time.Since(startTime)
		if duration > threshold {
			u.logger_utils.Warnf("%v took %v", id, duration)
		}
		return duration
	}
}

// Given an time threshold, execute a func if timer expires
// Returns a stopFunc to stop the timer from firing
func (u *Utilities) StartDebugExec(id string, threshold time.Duration, debugFunc func()) func() {
	finCh := make(chan bool)
	timer := time.NewTimer(threshold)
	var closeFinOnce sync.Once

	stopFunc := func() {
		closeFinOnce.Do(func() { close(finCh) })
	}

	go func() {
		select {
		case <-finCh:
			timer.Stop()
		case <-timer.C:
			stopFunc()
			debugFunc()
		}
	}()

	return stopFunc
}

func (u *Utilities) DumpStackTraceAfterThreshold(id string, threshold time.Duration, goroutines base.PprofLookupTypes) func() {
	dumpStackTrace := func() {
		pprof.Lookup(goroutines.String()).WriteTo(os.Stdout, 1)
	}
	return u.StartDebugExec(id, threshold, dumpStackTrace)
}

func (u *Utilities) GetHighSeqNos(vbnos []uint16, conn mcc.ClientIface, stats_map *map[string]string, collectionIds []uint32, recycledVbSeqnoMap *map[uint16]uint64) (*map[uint16]uint64, *map[string]string, []uint16, error) {
	if len(vbnos) == 0 {
		return nil, nil, nil, base.ErrorNoVbSpecified
	}

	var highseqno_map *map[uint16]uint64
	if recycledVbSeqnoMap != nil {
		highseqno_map = recycledVbSeqnoMap
	} else {
		newMap := make(map[uint16]uint64)
		highseqno_map = &newMap
	}

	var err error
	var unableToBeParsedVBs []uint16
	if len(collectionIds) == 0 {
		// Get all the seqno across everything in the bucket using traditional stats map
		if stats_map != nil && *stats_map != nil {
			sanitizeHighSeqnoStatsMap(vbnos, stats_map)
			// stats_map is not nill when GetHighSeqNos is called from per-replication stats manager, reuse stats_map to avoid memory over-allocation and re-allocation
			err = conn.StatsMapForSpecifiedStats(base.VBUCKET_SEQNO_STAT_NAME, *stats_map)
		} else {
			// stats_map is nill when GetHighSeqNos is called on paused replications. do not reuse stats_map
			*stats_map, err = conn.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		unableToBeParsedVBs, err = u.ParseHighSeqnoStat(vbnos, *stats_map, *highseqno_map)
	} else {
		// Need to get high sequence numbers across one or a set of collections only
		mccContext := &mcc.ClientContext{}
		var vbSeqnoMap map[uint16]uint64
		for _, collectionId := range collectionIds {
			mccContext.CollId = collectionId
			vbSeqnoMap, err = conn.GetAllVbSeqnos(vbSeqnoMap, mccContext)
			if err != nil {
				return nil, nil, nil, err
			}
			PopulateMaxVbSeqnoMap(vbSeqnoMap, *highseqno_map)
		}
		err = FilterVbSeqnoMap(vbnos, highseqno_map)
	}

	if err != nil {
		return nil, nil, nil, err
	} else {
		return highseqno_map, stats_map, unableToBeParsedVBs, nil
	}
}

// These are the actual max cas's from KV stats
func (u *Utilities) GetMaxCasStatsForVBs(vbnos []uint16, conn mcc.ClientIface, statsMap *map[string]string, vbMaxCasMap *map[uint16]uint64) (map[uint16]uint64, []uint16, error) {
	if len(vbnos) == 0 {
		return nil, nil, base.ErrorNoVbSpecified
	}

	var err error
	if statsMap != nil && *statsMap != nil {
		sanitizeHighSeqnoStatsMap(vbnos, statsMap)
		err = conn.StatsMapForSpecifiedStats(base.VBUCKET_DETAILS_NAME, *statsMap)
	} else {
		statsMap = &map[string]string{}
		*statsMap, err = conn.StatsMap(base.VBUCKET_DETAILS_NAME)
	}
	if err != nil {
		return nil, nil, err
	}
	unableToBeParsedVBs, err := u.ParseMaxCasStat(vbnos, *statsMap, *vbMaxCasMap)
	return *vbMaxCasMap, unableToBeParsedVBs, nil
}

// Ensure that only the VBs being requested are entries in the stats_map
// For speed, does not inspect any non high_seqno related stats (avoid map iteration)
func sanitizeHighSeqnoStatsMap(vbnos []uint16, stats_map *map[string]string) {
	unsortedVBs := base.CloneUint16List(vbnos)
	sortedVBs := base.SortUint16List(unsortedVBs)

	for vbno := uint16(0); vbno < base.NumberOfVbs; vbno++ {
		_, vbShouldExist := base.SearchUint16List(sortedVBs, vbno)
		if vbShouldExist {
			keyShouldExist := base.ComposeVBHighSeqnoStatsKey(vbno)
			if _, exists := (*stats_map)[keyShouldExist]; !exists {
				(*stats_map)[keyShouldExist] = ""
			}
		} else {
			keyShouldNotExist := base.ComposeVBHighSeqnoStatsKey(vbno)
			if _, exists := (*stats_map)[keyShouldNotExist]; exists {
				delete(*stats_map, keyShouldNotExist)
			}
		}
	}
}

func PopulateMaxVbSeqnoMap(latestVbSeqnoMap, maxVbSeqnoMap map[uint16]uint64) {
	for vb, latestSeqno := range latestVbSeqnoMap {
		maxSeqno, exists := maxVbSeqnoMap[vb]
		if !exists || latestSeqno > maxSeqno {
			maxVbSeqnoMap[vb] = latestSeqno
		}
	}
}

// Make sure vbSeqnoMap only contains entries in vbnos
func FilterVbSeqnoMap(vbnos []uint16, vbSeqnoMap *map[uint16]uint64) error {
	if len(vbnos) == 0 {
		if len(*vbSeqnoMap) > 0 {
			for k, _ := range *vbSeqnoMap {
				delete(*vbSeqnoMap, k)
			}
		}
		return nil
	}

	// Shouldn't be the case
	if len(vbnos) > len(*vbSeqnoMap) {
		return fmt.Errorf("Asking for vb's %v when seqnoMap only contains %v", vbnos, vbSeqnoMap)
	}

	sortedVbnos := base.SortUint16List(vbnos)
	sortedVbnosLen := len(sortedVbnos)
	var vbsToDelete []uint16
	for vbno, _ := range *vbSeqnoMap {
		i := sort.Search(sortedVbnosLen, func(i int) bool { return sortedVbnos[i] >= vbno })
		if i < sortedVbnosLen && sortedVbnos[i] == vbno {
			// Vbno entry of vbSeqnoMap exists in the vbnos list
		} else {
			// vbnos list did not indicate to include this vb in the vbSeqnoMap
			vbsToDelete = append(vbsToDelete, vbno)
		}
	}

	for _, vbno := range vbsToDelete {
		delete(*vbSeqnoMap, vbno)
	}
	return nil
}

// Given a bucketInfo and bucketName, figure out the VBs that this node is the master of
// Then return a list of replicas in order for each of the VB
// 1. Map of vbno -> memcached KV address:port
// 2. Map of memcached Address -> ns_server address
// 3. Number of replicas set for this bucket
// 4. List of VBs that this node is not the owner, but is a member as a replica
func (u *Utilities) GetReplicasInfo(bucketInfo map[string]interface{}, isClusterEncryptionStrictOrAll bool, recycledStringStringMap *base.StringStringMap, recycledVbHostMapGetter func(vbnos []uint16) *base.VbHostsMapType, recycledStringSliceGetter func() *[]string) (*base.VbHostsMapType, *base.StringStringMap, int, []uint16, error) {
	nodesList, ok := bucketInfo[base.NodesKey].([]interface{})
	if !ok {
		return nil, nil, 0, nil, fmt.Errorf("Unable to get %v from bucketInfo", base.NodesKey)
	}

	var kvToNsServerTranslateMap *base.StringStringMap
	if recycledStringStringMap != nil {
		kvToNsServerTranslateMap = recycledStringStringMap
	} else {
		newMap := make(base.StringStringMap)
		kvToNsServerTranslateMap = &newMap
	}

	var foundThisNode bool
	var thisNodeName string
	for _, nodeInfoRaw := range nodesList {
		nodeInfo, ok := nodeInfoRaw.(map[string]interface{})
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("Unable to convert nodeInfoRaw into the right type")
		}
		isThisNode, ok := nodeInfo[base.ThisNodeKey].(bool)
		if ok && isThisNode {
			foundThisNode = true
		}

		nsServerHostAndPort, ok := nodeInfo[base.HostNameKey].(string)
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("Unable to convert nodeInfo.hostname to string")
		}
		nodeNameWithoutPort := base.GetHostName(nsServerHostAndPort)

		portsMap, ok := nodeInfo[base.PortsKey].(map[string]interface{})
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("Unable to find ports data structure")
		}

		if isClusterEncryptionStrictOrAll {
			parsedSecureAdminPort, secureAdminPortErr := u.GetHttpsMgtPortFromNodeInfo(nodeInfo)
			if secureAdminPortErr != nil {
				parsedSecureAdminPort = int(base.DefaultAdminPortSSL)
			}
			nsServerHostAndPort = base.GetHostAddr(nodeNameWithoutPort, uint16(parsedSecureAdminPort))
		}

		for portName, portNumberRaw := range portsMap {
			portNumber, ok := portNumberRaw.(float64)
			if !ok {
				return nil, nil, 0, nil, fmt.Errorf("Wrong portNumberType: %v\n", reflect.TypeOf(portNumberRaw))
			}
			if portName == base.DirectPortKey {
				memcachedPort := uint16(portNumber)
				kvServerAndPort := base.GetHostAddr(nodeNameWithoutPort, memcachedPort)
				(*kvToNsServerTranslateMap)[kvServerAndPort] = nsServerHostAndPort
				if isThisNode {
					thisNodeName = kvServerAndPort
				}
			}
		}
	}
	if !foundThisNode {
		// If this node is not rebalanced into the cluster yet, this node won't be an owner of any VB's
		return nil, nil, 0, nil, base.ErrorNoSourceNozzle
	}

	vbServerMap, ok := bucketInfo[base.VBucketServerMapKey].(map[string]interface{})
	if !ok {
		return nil, nil, 0, nil, fmt.Errorf("Unable to find vbServerMap")
	}
	numOfReplicasFloat, ok := vbServerMap[base.NumberOfReplicas].(float64)
	if !ok {
		return nil, nil, 0, nil, fmt.Errorf("Unable to get number of replicas")
	}
	numOfReplicas := int(numOfReplicasFloat)
	if numOfReplicas == 0 {
		// Nothing to return
		return nil, nil, 0, nil, nil
	}
	serverList, ok := vbServerMap[base.ServerListKey].([]interface{})
	if !ok {
		return nil, nil, 0, nil, fmt.Errorf("Unable to find serverList")
	}

	var thisNodeIndex int = -1
	for i, serverNameRaw := range serverList {
		serverName, ok := serverNameRaw.(string)
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("Unable to parse serverName as string")
		}
		if serverName == thisNodeName {
			thisNodeIndex = i
			break
		}
	}
	if thisNodeIndex == -1 {
		return nil, nil, 0, nil, fmt.Errorf("Unable to find %v in list %v", thisNodeName, serverList)
	}

	// Once we have node name, index in the list, and number of replicas, we know what to compile and return
	vbucketListOfServerIdx, ok := vbServerMap[base.VBucketMapKey].([]interface{})
	if !ok {
		return nil, nil, 0, nil, fmt.Errorf("unable to find %v", base.VBucketMapKey)
	}

	var vbReplicaMap *base.VbHostsMapType
	var vbListForBeingAReplica []uint16

	// First pass - figure out the VBs if recycled map getter is present
	if recycledVbHostMapGetter != nil {
		var vbListForThisNode []uint16
		for vbno, serverIdxListRaw := range vbucketListOfServerIdx {
			serverIdxList, ok := serverIdxListRaw.([]interface{})
			if !ok {
				return nil, nil, 0, nil, fmt.Errorf("wrong type for serverIdxListRaw: %v", reflect.TypeOf(serverIdxListRaw))
			}
			// serverList contains the node itself (idx 0) + replicas
			if len(serverIdxList) != (numOfReplicas + 1) {
				return nil, nil, 0, nil, fmt.Errorf("for VB %v the list of nodes is of length %v - expected %v", vbno, len(serverIdxList), numOfReplicas+1)
			}
			firstNodeIdxFloat, ok := serverIdxList[0].(float64)
			if !ok {
				return nil, nil, 0, nil, fmt.Errorf("serverIndex is of wrong type: %v", reflect.TypeOf(serverIdxList[0]))
			}
			firstNodeIdx := int(firstNodeIdxFloat)
			if firstNodeIdx == thisNodeIndex {
				vbListForThisNode = append(vbListForThisNode, uint16(vbno))
			}
		}

		vbReplicaMap = recycledVbHostMapGetter(vbListForThisNode)
	} else {
		newVBReplicaMap := make(base.VbHostsMapType)
		vbReplicaMap = &newVBReplicaMap
	}

	// Second pass
	for vbno, serverIdxListRaw := range vbucketListOfServerIdx {
		serverIdxList, ok := serverIdxListRaw.([]interface{})
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("wrong type for serverIdxListRaw: %v", reflect.TypeOf(serverIdxListRaw))
		}
		// serverList contains the node itself (idx 0) + replicas
		if len(serverIdxList) != (numOfReplicas + 1) {
			return nil, nil, 0, nil, fmt.Errorf("for VB %v the list of nodes is of length %v - expected %v", vbno, len(serverIdxList), numOfReplicas+1)
		}
		firstNodeIdxFloat, ok := serverIdxList[0].(float64)
		if !ok {
			return nil, nil, 0, nil, fmt.Errorf("serverIndex is of wrong type: %v", reflect.TypeOf(serverIdxList[0]))
		}
		firstNodeIdx := int(firstNodeIdxFloat)
		if firstNodeIdx == thisNodeIndex {
			// This node is the VB master since it is first in the list - rest are replicas
			for i := 1; i < numOfReplicas+1; i++ {
				replicaIdxFloat := serverIdxList[i].(float64)
				replicaIdx := int(replicaIdxFloat)
				// replicaIdx is -1 if hasn't been assigned yet or is transient. Ignore -1
				if replicaIdx != -1 {
					replicaName := serverList[replicaIdx].(string)
					if (*vbReplicaMap)[uint16(vbno)] == nil {
						if recycledStringSliceGetter == nil {
							var newSlice []string
							(*vbReplicaMap)[uint16(vbno)] = &newSlice
						} else {
							(*vbReplicaMap)[uint16(vbno)] = recycledStringSliceGetter()
						}
					}
					*(*vbReplicaMap)[uint16(vbno)] = append(*(*vbReplicaMap)[uint16(vbno)], replicaName)
				}
			}
		} else {
			// See if this node's index belongs in any of the other VBs
			var thisNodeIdxIsReplica bool
			for _, nodeIdxRaw := range serverIdxList {
				nodeIdxFloat := nodeIdxRaw.(float64)
				nodeIdx := int(nodeIdxFloat)
				if nodeIdx == thisNodeIndex {
					thisNodeIdxIsReplica = true
					break
				}
			}
			if thisNodeIdxIsReplica {
				vbListForBeingAReplica = append(vbListForBeingAReplica, uint16(vbno))
			}
		}
	}
	return vbReplicaMap, kvToNsServerTranslateMap, numOfReplicas, vbListForBeingAReplica, nil
}

// ParseClientCertOutput takes the output of /settings/clientCertAuth endpoint and checks if client cert is 'mandatory' or 'hybrid'
func (u *Utilities) ParseClientCertOutput(clientCertInput map[string]interface{}) (isMandatoryOrHybrid bool, err error) {
	if clientCertInput == nil {
		err = fmt.Errorf("ClientCert input is empty")
		return
	}

	stateRaw, ok := clientCertInput[base.StateKey]
	if !ok {
		err = fmt.Errorf("unable to find state object")
		return
	}

	stateStr, ok := stateRaw.(string)
	if !ok {
		err = fmt.Errorf("expected state to be string, got %T", stateRaw)
		return
	}

	isMandatoryOrHybrid = (stateStr == base.MandatoryVal) || (stateStr == base.HybridVal)
	return
}

func (u *Utilities) GetTerseInfo(localConnStr string, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCert, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	clusterInfo, err := u.GetClusterInfo(localConnStr, base.TerseClusterInfoPath, username, password, authMech, certificate, sanInCertificate, clientCert, clientKey, logger)
	if err != nil {
		return nil, err
	}

	return clusterInfo, nil
}

// IsTerseBucketInfo returns bucketInfo using the pools/default/b/<bucketName> endpoint.
func (u *Utilities) IsTerseBucketInfo(bucketInfo map[string]interface{}) (bool, error) {
	if len(bucketInfo) == 0 {
		return false, base.ErrorInvalidInput
	}

	// Ensure that the input map is buckets info in the first place. Nodes info like response from
	// pools/default/nodeServices endpoint will also contain 'nodesExt' information.
	_, ok1 := bucketInfo[base.BucketTypeKey]

	// Non-terse bucket info doesn't contain 'nodesExt' information. It only contains 'nodes'.
	_, ok2 := bucketInfo[base.NodeExtKey]

	return ok1 && ok2, nil
}

// ShouldUseTerseBucketInfo returns whether kv_vb_map should be constructed using terse bucket info (pools/default/b/<bucketName>)
// instead of pools/default/buckets/<bucketName>.
// We should use terse bucket info when:
// 1. the user intent is 'external'
// 2. auth mech is 'Https'
// 3. all external hostnames are the same (like in a case of replicating to a LB or private links in Capella).
// If all these three conditions are met, the kv_vb_map generated using non-terse info will always contain one entry (or in general lesser
// entries than the actual number of active KV nodes) due to the absense of kvSSL port in non-terse endpoint. So we use the terse endpoint.
func (u *Utilities) ShouldUseTerseBucketInfo(bucketInfo map[string]interface{}, hostAddr, bucketName string, useExternal bool, isHttps bool) (bool, error) {
	if len(bucketInfo) == 0 {
		return false, base.ErrorInvalidInput
	}

	if !useExternal {
		return false, nil
	}

	if !isHttps {
		return false, nil
	}

	nodesList, err := u.GetNodeListFromInfoMap(bucketInfo, u.logger_utils)
	if err != nil {
		return false, err
	}

	if len(nodesList) == 1 {
		// single node target cluster will not have any problems with non-terse bucket info.
		return false, nil
	}

	kvVBMap, err := u.GetRemoteServerVBucketsMap(hostAddr, bucketName, bucketInfo, useExternal)
	if err != nil {
		return false, err
	}

	if len(kvVBMap) == len(nodesList) {
		// kvVBMap should have all the active KV nodes in the target cluster. If not, then it's because
		// non terse bucket info doesn't contain external kvSSL port. Terse bucket info should be used.
		return false, nil
	}

	return true, nil
}

// GetTerseBucketInfo returns the response of the pools/default/b/<bucketName> endpoint
func (u *Utilities) GetTerseBucketInfo(hostAddr, bucketName, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCert []byte, clientKey []byte, logger *log.CommonLogger) (map[string]interface{}, error) {
	if len(bucketName) == 0 {
		return nil, fmt.Errorf("bucket name cannot be empty")
	}
	bucketInfo := make(map[string]interface{})

	stopFunc := u.StartDiagStopwatch(fmt.Sprintf("GetTerseBucketInfo(%v, %v)", hostAddr, bucketName), base.DiagNetworkThreshold)
	defer stopFunc()
	err, statusCode := u.QueryRestApiWithAuth(hostAddr, base.BPath+bucketName, false, username, password, authMech, certificate, sanInCertificate, clientCert, clientKey, base.MethodGet, "", nil, 0, &bucketInfo, nil, false, logger, nil)
	if err == nil && statusCode == http.StatusOK {
		return bucketInfo, nil
	}

	switch statusCode {
	case http.StatusNotFound:
		return nil, u.GetNonExistentBucketError()
	case http.StatusUnauthorized:
		return nil, base.ErrorUnauthorized
	case http.StatusForbidden:
		return nil, base.ErrorForbidden
	default:
		logger.Errorf("Failed to get terse bucket info for bucket '%v'. host=%v, err=%v, statusCode=%v", bucketName, hostAddr, err, statusCode)
		return nil, fmt.Errorf("failed to get terse bucket info")
	}
}
