// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package replication_manager

import (
	"github.com/couchbase/goxdcr/log"
	base "github.com/couchbase/goxdcr/base"
	metadata "github.com/couchbase/goxdcr/metadata"
	utils "github.com/couchbase/goxdcr/utils"
	"strconv"
	"net/http"
	"net/url"
	"io/ioutil"
	"errors"
	"regexp"
	"encoding/json"
	"bytes"
	"strings"
)

// xdcr prefix for internal settings keys
var XDCRPrefix = "xdcr"
var ErrorsKey = "errors"

// http request related constants
const (
	ContentType = "Content-Type"
	DefaultContentType = "application/x-www-form-urlencoded"
)

// constants used for parsing url path
const (
	CreateReplicationPath    = "controller/createReplication"
	NotifySettingsChangePrefix  = "controller/notifySettingsChange"
	StatisticsPrefix         = "stats/buckets"
	InternalSettingsPath     = "internalSettings"
	AllReplicationsPath =      "pools/default/replications"
	DeleteReplicationPrefix  = "controller/cancelXDCR"
	SettingsReplicationsPath = "settings/replications" 
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path.
	// e.g., settings/replications/dynamic
	DynamicSuffix = "/dynamic"
)

// constants used for parsing replication settings
const (
	Type                           = "type"
	ReplicationType                = "replicationType"
	FilterExpression               = "filterExpression"
	PauseRequested                 = "pauseRequested"
	CheckpointInterval             = "checkpointInterval"
	BatchCount                     = "workerBatchSize"
	BatchSize                      = "docBatchSizeKb"
	FailureRestartInterval         = "failureRestartInterval"
	OptimisticReplicationThreshold = "optimisticReplicationThreshold"
	HttpConnection                 = "httpConnections"
	SourceNozzlePerNode            = "sourceNozzlePerNode"
	TargetNozzlePerNode            = "targetNozzlePerNode"
	MaxExpectedReplicationLag      = "maxExpectedReplicationLag"
	TimeoutPercentageCap           = "timeoutPercentageCap"
	LogLevel                       = "logLevel"
	StatsInterval				   = "statsInterval"

	ReplicationTypeValue           = "continuous"
)

// constants for parsing create replication request
const (
	FromBucket = "fromBucket"
	ToCluster = "toCluster"
	ToBucket = "toBucket"
	Forward = "forward"
)

// constants for parsing create replication response
const (
	ReplicationId = "id"
)

const (
	OldReplicationSettings = "OldReplicationSettings"
)

// constants for stats names
const (
	DocsWritten = "docs_written"
	DataReplicated = "data_replicated"
	ChangesLeft = "changes_left"
	DocsChecked = "docs_checked" 
	NumCheckpoints = "num_checkpoints"
	NumFailedCheckpoints = "num_failedckpts" 
	SizeRepQueue = "size_rep_queue"
	TimeCommiting = "time_committing"
	BandWidthUsage = "bandwidth_usage" 
	DocsLatencyAppr = "docs_latency_aggr" 
	DocsLatencyWt = "docs_latency_wt" 
	DocsRepQueue = "docs_req_queue" 
	MetaLatencyAggr = "meta_latency_aggr" 
	MetaLatencyWt = "meta_latency_wt" 
	RateReplication = "rate_replication" 
	DocsOptRepd = "docs_opt_repd" 
	ActiveVbreps = "active_vbreps"
	WaitingVbreps = "waiting_vbreps"
	TimeWorking = "time_working"
	TimeoutPercentageMap = "timeout_percentage_map" 
)

// errors
var MissingSettingsInRequest = errors.New("Invalid http request. No replication setting parameters have been supplied.")
var MissingOldSettingsInRequest = errors.New("Invalid http request. No old replication settings have been supplied.")

// replication settings key in rest api -> internal replication settings key
var RestKeyToSettingsKeyMap = map[string]string {
	Type:  metadata.ReplicationType,
	FilterExpression: metadata.FilterExpression,
	PauseRequested: metadata.Active,
	CheckpointInterval: metadata.CheckpointInterval,
	BatchCount: metadata.BatchCount,
	BatchSize: metadata.BatchSize,
	HttpConnection: metadata.HttpConnection,
	FailureRestartInterval: metadata.FailureRestartInterval,
	OptimisticReplicationThreshold: metadata.OptimisticReplicationThreshold,
	SourceNozzlePerNode: metadata.SourceNozzlePerNode,
	TargetNozzlePerNode: metadata.TargetNozzlePerNode,
	MaxExpectedReplicationLag: metadata.MaxExpectedReplicationLag,
	TimeoutPercentageCap: metadata.TimeoutPercentageCap,
	LogLevel: metadata.PipelineLogLevel,
	StatsInterval: metadata.PipelineStatsInterval,
} 

// internal replication settings key -> replication settings key in rest api
var SettingsKeyToRestKeyMap = map[string]string {
	metadata.ReplicationType:  Type,
	metadata.FilterExpression: FilterExpression,
	metadata.Active: PauseRequested,
	metadata.CheckpointInterval: CheckpointInterval,
	metadata.BatchCount: BatchCount,
	metadata.BatchSize: BatchSize,
	metadata.HttpConnection:  HttpConnection,
	metadata.FailureRestartInterval: FailureRestartInterval,
	metadata.OptimisticReplicationThreshold: OptimisticReplicationThreshold,
	metadata.SourceNozzlePerNode: SourceNozzlePerNode,
	metadata.TargetNozzlePerNode: TargetNozzlePerNode,
	metadata.MaxExpectedReplicationLag: MaxExpectedReplicationLag,
	metadata.TimeoutPercentageCap: TimeoutPercentageCap,
	metadata.PipelineLogLevel: LogLevel,
	metadata.PipelineStatsInterval: StatsInterval,
} 

//constants for outputing replication docs
const (
	RemoteClustersForReplicationDoc = "remoteClusters"
	BucketsPath = "buckets"
	
	ReplicationDocType = "type"
	ReplicationDocId = "id"
	ReplicationDocSource = "source"
	ReplicationDocTarget = "target"
	ReplicationDocContinuous = "continuous"
	ReplicationDocPauseRequested = "pauseRequested"
	
	ReplicationDocTypeXmem = "xdc-xmem"
	ReplicationDocTypeCapi = "xdc"
)

var logger_msgutil *log.CommonLogger = log.NewLogger("MessageUtils", log.DefaultLoggerContext)

func NewGetRemoteClustersResponse(remoteClusters map[string]*metadata.RemoteClusterReference) ([]byte, error) {
	remoteClusterArr := make([]map[string]interface{}, 0)
	for _, remoteCluster := range remoteClusters {
		remoteClusterArr = append(remoteClusterArr, remoteCluster.ToMap())
	}
	b, err := json.Marshal(remoteClusterArr)
	return b, err
}

func NewGetAllReplicationsResponse(replSpecs map[string]*metadata.ReplicationSpecification) ([]byte, error) {
	replArr := make([]map[string]interface{}, 0)
	for _, replSpec := range replSpecs {
		replArr = append(replArr, getReplicationDocMap(replSpec))
	}
	b, err := json.Marshal(replArr)
	return b, err
}

func getReplicationDocMap(replSpec *metadata.ReplicationSpecification) map[string]interface{} {
	replDocMap := make(map[string]interface{})
	if replSpec != nil {
		replDocMap[ReplicationDocId] = replSpec.Id
		replDocMap[ReplicationDocContinuous] = true
		replDocMap[ReplicationDocSource] = replSpec.SourceBucketName
		replDocMap[ReplicationDocTarget] = base.UrlDelimiter + RemoteClustersForReplicationDoc + base.UrlDelimiter + replSpec.TargetClusterUUID + base.UrlDelimiter + BucketsPath + base.UrlDelimiter + replSpec.TargetBucketName
		replDocMap[ReplicationDocPauseRequested] = !replSpec.Settings.Active
		if replSpec.Settings.RepType == metadata.ReplicationTypeXmem {
			replDocMap[ReplicationDocType] = ReplicationDocTypeXmem
		} else {
			replDocMap[ReplicationDocType] = ReplicationDocTypeCapi
		}
		// pass filter expression back, which is needed by running replications ui display
		replDocMap[FilterExpression] = replSpec.Settings.FilterExpression
	}
	return replDocMap
}

// this func assumes that the request.ParseForm() has already been called, which 
// should be the case since justValidate always come with some other required parameters
// As a result, the error returned by this func is always a validation error
func DecodeJustValidateFromRequest(request *http.Request) (bool, error) {	
	for key, valArr := range request.Form {		
		switch key {
		case base.JustValidate:
			justValidate, err := getBoolFromValArr(key, valArr, false)
			if err != nil {
				return false, err
			} else {
				return justValidate, nil
			}
		default:
			// ignore other parameters
		}
	}
	return false, nil
}

// decode parameters from create remote cluster request
func DecodeCreateRemoteClusterRequest(request *http.Request) (justValidate bool, remoteClusterRef *metadata.RemoteClusterReference, errorsMap map[string]error, err error) {	
	errorsMap = make(map[string]error)
	var uuid, name, hostName, userName, password string
	var demandEncryption bool
	var certificate []byte
	
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	justValidate, err = DecodeJustValidateFromRequest(request)
	if err != nil{
		errorsMap[base.JustValidate] = err
	}
		
	for key, valArr := range request.Form {		
		switch key {
		case base.RemoteClusterUuid:
			uuid = getStringFromValArr(key, valArr)
		case base.RemoteClusterName:
			name = getStringFromValArr(key, valArr)
		case base.RemoteClusterHostName:
			hostName = getStringFromValArr(key, valArr)
		case base.RemoteClusterUserName:
			userName = getStringFromValArr(key, valArr)
		case base.RemoteClusterPassword:
			password = getStringFromValArr(key, valArr)
		case base.RemoteClusterDemandEncryption:
			demandEncryption, err = getBoolFromValArr(key, valArr, false)
			if err != nil {
				errorsMap[base.RemoteClusterDemandEncryption] = err
			}
		case base.RemoteClusterCertificate:
			certificateStr := getStringFromValArr(key, valArr)
			certificate = []byte(certificateStr)
		default:
			// ignore other parameters
		}
	}
	
	// check required parameters
	if len(name) == 0 {
		errorsMap[base.RemoteClusterName] = utils.MissingParameterError("cluster name")
	}
	if len(hostName) == 0 {
		errorsMap[base.RemoteClusterHostName] = utils.MissingParameterError("hostname (ip)")
	}
	if len(userName) == 0 {
		errorsMap[base.RemoteClusterUserName] = utils.MissingParameterError("username")
	}
	if len(password) == 0 {
		errorsMap[base.RemoteClusterPassword] = utils.MissingParameterError("password")
	}
	
	// demandEncryption can be set only on enterprise editions
	isEnterprise, err := XDCRCompTopologyService().IsMyClusterEnterprise()
	if err != nil {
		return
	}		
	if demandEncryption && !isEnterprise {
		errorsMap[base.RemoteClusterDemandEncryption] = errors.New("Encryption can only be used in enterprise edition")
	}
	
	// certificate is required if demandEncryption is set to true
	if demandEncryption && len(certificate) == 0 {
		errorsMap[base.RemoteClusterCertificate] = errors.New("Certificate is required when encryption is enabled")
	}
	
	if len(errorsMap) == 0 {
		remoteClusterRef = metadata.NewRemoteClusterReference(uuid, name, hostName, userName, password, demandEncryption, certificate)
	}
	
	return
}

func NewCreateRemoteClusterResponse(remoteClusterRef *metadata.RemoteClusterReference) ([]byte, error) {
	return json.Marshal(remoteClusterRef.ToMap())
}

func NewDeleteRemoteClusterResponse() ([]byte, error) {
	// return "ok" in success case
	return json.Marshal("ok")
}

func NewDeleteReplicationResponse() ([]byte, error) {
	emptyArr := make([]string, 0)
	return json.Marshal(emptyArr)
}

// decode parameters from create replication request
func DecodeCreateReplicationRequest(request *http.Request) (fromBucket, toCluster, toBucket string, forward bool, settings map[string]interface{}, errorsMap map[string]error, err error) {	
	errorsMap = make(map[string]error)
	var replicationType string
	
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	// forward defaults to true if not specified
	forward = true

	for key, valArr := range request.Form {
		switch key {
		case ReplicationType:
			replicationType = getStringFromValArr(key, valArr)
			if replicationType != ReplicationTypeValue {
				errorsMap[ReplicationType] = utils.GenericInvalidValueError(ReplicationType)
			}
		case FromBucket:
			fromBucket = getStringFromValArr(key, valArr)
		case ToCluster:
			toCluster = getStringFromValArr(key, valArr)
		case ToBucket:
			toBucket = getStringFromValArr(key, valArr)
		case Forward:
			forward, err = getBoolFromValArr(key, valArr, forward)
			if err != nil {
				errorsMap[Forward] = err
			}
		default:
			// ignore other parameters
		}
	}
	
	if len(replicationType) == 0 {
		errorsMap[ReplicationType] = utils.MissingValueError("replication type")
	}
	
	if len(fromBucket) == 0 {
		errorsMap[FromBucket] = utils.MissingValueError("source bucket")
	}
	if len(toCluster) == 0 {
		errorsMap[ToCluster] = utils.MissingValueError("target cluster")
	}
	if len(toBucket) == 0 {
		errorsMap[ToBucket] = utils.MissingValueError("target bucket")
	}
	 
	settings, settingsErrorsMap, err := DecodeSettingsFromRequest(request)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}
	return
}

func DecodeChangeReplicationSettings(request *http.Request) (justValidate bool, settings map[string]interface{}, errorsMap map[string]error, err error) {	
	errorsMap = make(map[string]error)
	
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	justValidate, err = DecodeJustValidateFromRequest(request)
	if err != nil{
		errorsMap[base.JustValidate] = err
	}
	 
	settings, settingsErrorsMap, err := DecodeSettingsFromRequest(request)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}
	return
}

// create a notify replication settings change request and attach old settings so that receiver can tell what the actual changes are
func NewNotifySettingsChangeRequest(replicationId string, xdcrRestPort uint16, oldSettings *metadata.ReplicationSettings) (*http.Request, error) {
	// replicationId is cancatenated into the url 
	url := base.AdminportUrlPrefix + NotifySettingsChangePrefix + base.UrlDelimiter + replicationId
	paramsMap := make(map[string]interface{})
	settingsBytes, err := json.Marshal(oldSettings)
	if err != nil {
		return nil, err
	}
	paramsMap[OldReplicationSettings] = settingsBytes
	paramsBytes, _ := EncodeMapIntoByteArray(paramsMap)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	request, err := http.NewRequest(base.MethodPost, url, paramsBuf)
	request.Header.Set(ContentType, DefaultContentType)
	return request, err
}

// decode replicationId from create replication response
func DecodeCreateReplicationResponse(response *http.Response) (string, error) {
	defer response.Body.Close()

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	
	var paramsMap map[string]interface{}
	err = json.Unmarshal(bodyBytes, &paramsMap) 
	if err != nil {
		return "", err
	}
	
	replicationId, ok := paramsMap[ReplicationId]
	
	if !ok {
		return "", utils.MissingParameterInHttpResponseError(ReplicationId)
	}
	
	replicationIdStr, ok := replicationId.(string)
	if !ok {
		return "", utils.IncorrectValueTypeInHttpResponseError(ReplicationId, replicationId, "string")
	}
	
	return replicationIdStr, nil
	
}

func DecodeReplicationIdAndForwardFlagFromHttpRequest(request *http.Request, pathPrefix string) (replicationId string, forward bool, err error) {
	replicationId, err = DecodeDynamicParamInURL(request, pathPrefix, "Replication Id")
	if err != nil {
		return 
	}
	
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	// forward defaults to true if not specified
	forward = true

	// get forward flag from request body
	for key, valArr := range request.Form {
		switch key {
			case Forward:
				forward, err = getBoolFromValArr(key, valArr, forward)
			default:
				// ignore other parameters
		}
	}
	
	return
	
}

func DecodeOldSettingsFromRequest(request *http.Request) (*metadata.ReplicationSettings, error) {
	var oldSettings metadata.ReplicationSettings
	
	if err := request.ParseForm(); err != nil {
		return nil, err
	}
	
	bFound := false

	for key, valArr := range request.Form {		
		switch key {
			case OldReplicationSettings:
				settingsStr := getStringFromValArr(key, valArr)
				if settingsStr == "" {
					return nil, utils.MissingParameterError(OldReplicationSettings)
				}
				bFound = true
				err := json.Unmarshal([]byte(settingsStr), &oldSettings)
				if err != nil {
					return nil, err
				}
			default:
				// ignore all other params
		}
	}
	
	if !bFound {
		return nil, MissingOldSettingsInRequest
	}
	
	return &oldSettings, nil
}

// decode replication settings related parameters from http request
func DecodeSettingsFromRequest(request *http.Request) (map[string]interface{}, map[string]error, error) {
	settings := make(map[string]interface{})
	errorsMap := make(map[string]error)
	
	if err := request.ParseForm(); err != nil {
		return nil, nil, err
	}

	for key, valArr := range request.Form {
		err := processKey(key, valArr, &settings)
		if err != nil {
			errorsMap[key] = err
		}
	}
	
	if len(errorsMap) > 0{
		return nil, errorsMap, nil
	}
	
	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	return settings, nil, nil
}

// decode replication settings related parameters from /internalSettings http request
func DecodeSettingsFromInternalSettingsRequest(request *http.Request) (map[string]interface{}, map[string]error, error) {
	settings := make(map[string]interface{})
	errorsMap := make(map[string]error)
	
	if err := request.ParseForm(); err != nil {
		return nil, nil, err
	}

	for key, valArr := range request.Form {
		restKey, err := ConvertRestInternalKeyToRestKey(key)
		if err != nil {
			// ignore non-internal settings key
			continue
		}
		
		err = processKey(restKey, valArr, &settings)
		if err != nil {
			errorsMap[restKey] = err
		}
	}
	
	if len(errorsMap) > 0{
		return nil, errorsMap, nil
	}
	
	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	return settings, nil, nil
}

func NewCreateReplicationResponse(replicationId string) []byte {
	params := make(map[string]interface{})
	params[ReplicationId] = replicationId
	// this should not fail
	bytes, _ := json.Marshal(params)
	return bytes
}

func NewReplicationSettingsResponse(settings *metadata.ReplicationSettings) ([]byte, error) {
	if settings == nil {
		return nil, nil
	} else {
		return json.Marshal(convertSettingsToRestSettingsMap(settings))
	}
}

func NewInternalSettingsResponse(settings *metadata.ReplicationSettings) ([]byte, error) {
	if settings == nil {
		return nil, nil
	} else {
		return json.Marshal(convertSettingsToRestInternalSettingsMap(settings))
	}
}


// decode replication id from http request
func DecodeDynamicParamInURL(request *http.Request, pathPrefix string, partName string) (string, error) {
	// length of prefix preceding replicationId in request url path 
	prefixLength := len(base.AdminportUrlPrefix) + len(pathPrefix) + len(base.UrlDelimiter)
	
	if len(request.URL.Path) <= prefixLength {		
		return "", utils.MissingParameterInHttpRequestUrlError(partName, request.URL.Path)
	}

	replicationId := request.URL.Path[prefixLength:]
	unescapedReplId, err := url.QueryUnescape(replicationId)
	logger_msgutil.Debugf("replication id decoded from request: %v\n", replicationId)
	logger_msgutil.Debugf("unescaped replication id: %v\n", unescapedReplId)
	return unescapedReplId, err
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
				return nil, utils.IncorrectValueTypeInMapError(key, val, "string/int/bool/LogLevel/[]byte")
		}
		params.Add(key, strVal)
	}
	
	return []byte (params.Encode()), nil
}

func verifyFilterExpression(filterExpression string) error {
	_, err := regexp.Compile(filterExpression)
	return err
}

func convertSettingsToRestSettingsMap(settings *metadata.ReplicationSettings) map[string]interface{} {
	restSettingsMap := make(map[string]interface{})
	settingsMap := settings.ToMap()
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		if restKey == PauseRequested {
			// pauseRequested = !active
			valueBool := value.(bool)
			restSettingsMap[restKey] = !valueBool
		} else {
			restSettingsMap[restKey] = value
		}
	}
	return restSettingsMap
}

func convertSettingsToRestInternalSettingsMap(settings *metadata.ReplicationSettings) map[string]interface{} {
	internalSettingsMap := make(map[string]interface{})
	settingsMap := settings.ToMap()
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		internalSettingsKey := ConvertRestKeyToRestInternalKey(restKey)
		internalSettingsMap[internalSettingsKey] = value
	}
	return internalSettingsMap
}

// turns the first char in key to upper case and prefix with "xdcr"
// e.g., turns "checkpointInterval" into "xdcrCheckpointInterval"
func ConvertRestKeyToRestInternalKey(key string) string {
	if key == "" {
		return XDCRPrefix
	}
	return XDCRPrefix + strings.ToUpper(key[0:1]) + key[1:]
}

func convertInternalSettingsMapToSettingsMap(internalSettingsMap map[string]interface{}) map[string]interface{} {
	settingsMap := make(map[string]interface{})
	for key, value := range internalSettingsMap {
		settingsMap[ConvertRestKeyToRestInternalKey(key)] = value
	}
	return internalSettingsMap
}

// strip the "xdcr" prefix and then turns the first char to lower case
// e.g., turns "xdcrCheckpointInterval" into "checkpointInterval"
func ConvertRestInternalKeyToRestKey(key string) (string, error) {
	prefixLen := len(XDCRPrefix)
	if len(key) > prefixLen && strings.HasPrefix(key, XDCRPrefix) {
		return strings.ToLower(key[prefixLen:prefixLen+1]) + key[prefixLen+1:], nil	
	} else {
		return "", errors.New("Not a valid internal settings key")
	} 
}

func getStringFromValArr(key string, valArr []string) string {
	if len(valArr) == 0 {
		return ""
	} else {
		return valArr[0]
	}
}

func getBoolFromValArr(key string, valArr []string, defaultValue bool) (bool, error) {
	boolStr := getStringFromValArr(key, valArr)
	if boolStr != "" {
		result, err := strconv.ParseBool(boolStr)
		if err != nil {
			return defaultValue, utils.IncorrectValueTypeError("a boolean")
		}
		return result, nil
	}
	
	return defaultValue, nil
}

// encode a map of errors into a byte array, which can then be returned in a http response
func EncodeErrorsMapIntoByteArray(errorsMap map[string]error) ([]byte, error) {
	errorMsgMap := make(map[string]string)
	result := make(map[string]interface{})
	
	for key, err := range errorsMap {
		errorMsgMap[key] = err.Error()
	}
	result[ErrorsKey] = errorMsgMap
	
	return json.Marshal(result)
}

func processKey(restKey string, valArr []string, settingsPtr *map[string]interface{}) error{
	settingsKey, ok := RestKeyToSettingsKeyMap[restKey]
	if !ok {
		// ignore non-settings key
		return nil
	}
	if len(valArr) == 0 || valArr[0] == "" {
		return nil
	}
	convertedValue, err := metadata.ValidateAndConvertSettingsValue(settingsKey, valArr[0])
	if err == nil {
		(*settingsPtr)[settingsKey] = convertedValue
	}	
	return err
}	
