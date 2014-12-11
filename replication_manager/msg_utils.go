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
	"fmt"
	"encoding/json"
	"bytes"
	"strings"
)

// xdcr prefix for internal settings keys
const XDCRPrefix = "xdcr"

// http request related constants
const (
	ContentType = "Content-Type"
	DefaultContentType = "application/x-www-form-urlencoded"
)

// constants used for parsing url path
const (
	RemoteClustersPath  = "pools/default/remoteClusters"
	CreateReplicationPath    = "controller/createReplication"
	DeleteReplicationPrefix  = "controller/cancelXDCR"
	NotifySettingsChangePrefix  = "controller/notifySettingsChange"
	StatisticsPrefix         = "stats/buckets"
	InternalSettingsPath     = "internalSettings"
	SettingsReplicationsPath = "settings/replications"
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path.
	// e.g., settings/replications/dynamic
	DynamicSuffix = "/dynamic"
)

// constants used for remote cluster references
const (
	RemoteClusterUuid   = "uuid"
	RemoteClusterName  = "name"
	RemoteClusterHostName = "hostname"
	RemoteClusterUserName = "username"
	RemoteClusterPassword = "password"
	RemoteClusterDemandEncryption = "demandEncryption"
	RemoteClusterCertificate = "certificate"
)

var RequiredRemoteClusterParams = [4]string{RemoteClusterName, RemoteClusterHostName, RemoteClusterUserName, RemoteClusterPassword}

// constants used for parsing replication settings
const (
	Type                           = "type"
	ReplicationType                = "replicationType"
	FilterExpression               = "filterExpression"
	Paused                         = "pauseRequested"
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
)

// constants for parsing create replication request
const (
	FromBucket = "fromBucket"
	ToCluster = "toCluster"
	ToBucket = "toBucket"
	FilterName = "filterName"
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
	Paused: metadata.Active,
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
	metadata.Active: Paused,
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

var logger_msgutil *log.CommonLogger = log.NewLogger("MessageUtils", log.DefaultLoggerContext)

func NewGetRemoteClustersResponse(remoteClusters map[string]*metadata.RemoteClusterReference) ([]byte, error) {
	remoteClusterArr := make([]metadata.RemoteClusterReference, 0)
	for _, remoteCluster := range remoteClusters {
		remoteClusterArr = append(remoteClusterArr, *remoteCluster)
	}
	b, err := json.Marshal(remoteClusterArr)
	return b, err
}

// decode parameters from create remote cluster request
func DecodeCreateRemoteClusterRequest(request *http.Request) (uuid, name, hostName, userName, password string, demandEncryption bool, certificate []byte, err error) {	
	if err = request.ParseForm(); err != nil {
		return 
	}

	decodedParams := make(map[string]bool, 0)
	
	for key, valArr := range request.Form {
		if len(valArr) != 1 {
			err = utils.InvalidValueInHttpRequestError(key, valArr)
			return
		}
		val := valArr[0]
		
		switch key {
		case RemoteClusterUuid:
			uuid = val
			decodedParams[RemoteClusterUuid] = true
		case RemoteClusterName:
			name = val
			decodedParams[RemoteClusterName] = true
		case RemoteClusterHostName:
			hostName = val
			decodedParams[RemoteClusterHostName] = true
		case RemoteClusterUserName:
			userName = val
			decodedParams[RemoteClusterUserName] = true
		case RemoteClusterPassword:
			password = val
			decodedParams[RemoteClusterPassword] = true
		case RemoteClusterDemandEncryption:
			demandEncryption, err = strconv.ParseBool(val)
			if err != nil {
				err = utils.InvalidValueInHttpRequestError(key, val)
				return
			}
		case RemoteClusterCertificate:
			certificate = []byte(val)
		default:
			// ignore other parameters
		}
	}
	
	// check required parameters
	missingParams := make([]string, 0)
	for _, requiredParam := range RequiredRemoteClusterParams{
		if _, ok := decodedParams[requiredParam]; !ok {
			missingParams = append(missingParams, requiredParam)
		}
	}
	
	// certificate is required if demandEncryption is set to true
	if demandEncryption && len(certificate) == 0 {
		missingParams = append(missingParams, RemoteClusterCertificate)
	}
	
	if len(missingParams) > 0 {
		err = utils.MissingParametersInHttpRequestError(missingParams)
		return
	} 
	
	return
}

func NewCreateRemoteClusterResponse(remoteClusterRef *metadata.RemoteClusterReference) ([]byte, error) {
	return json.Marshal(remoteClusterRef)
}

func NewDeleteRemoteClusterResponse() ([]byte, error) {
	// return "ok" in success case
	return []byte("ok"), nil
}

// decode parameters from create replication request
func DecodeCreateReplicationRequest(request *http.Request) (fromBucket, toCluster, toBucket, filterName string, forward bool, settings map[string]interface{}, err error) {	
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	// forward defaults to true if not specified
	forward = true

	for key, valArr := range request.Form {
		switch key {
		case FromBucket:
			fromBucket, err = getStringFromValArr(key, valArr)
		case ToCluster:
			toCluster, err = getStringFromValArr(key, valArr)
		case ToBucket:
			toBucket, err = getStringFromValArr(key, valArr)
		case FilterName:
			filterName, err = getStringFromValArr(key, valArr)
		case Forward:
			var forwardStr string
			forwardStr, err = getStringFromValArr(key, valArr)
			if err != nil {
				return
			}
			forward, err = strconv.ParseBool(forwardStr)
			if err != nil {
				err = utils.InvalidValueInHttpRequestError(key, forwardStr)
				return
			}
		default:
			// ignore other parameters
		}
		if err != nil {
			return
		}
	}
	
	missingParams := make([]string, 0)
	if len(fromBucket) == 0 {
		missingParams = append(missingParams, FromBucket)
	}
	if len(toCluster) == 0 {
		missingParams = append(missingParams, ToCluster)
	}
	if len(toBucket) == 0 {
		missingParams = append(missingParams, ToBucket)
	}
	if len(missingParams) > 0 {
		err = utils.MissingParametersInHttpRequestError(missingParams)
		return
	} 

	settings, err = DecodeSettingsFromRequest(request, false/*throwInvalidKeyError*/)
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

	// get forward flag from request body
	
	// forward defaults to true if not specified
	forward = true
	for key, valArr := range request.Form {
		switch key {
			case Forward:
				var forwardStr string
				forwardStr, err = getStringFromValArr(key, valArr)
				if err != nil {
					return
				}
				forward, err = strconv.ParseBool(forwardStr)
				if err != nil {
					err = utils.InvalidValueInHttpRequestError(key, forwardStr)
					return
				}
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
				settingsStr, err := getStringFromValArr(key, valArr)
				if err != nil {
					return nil, err
				}
				bFound = true
				err = json.Unmarshal([]byte(settingsStr), &oldSettings)
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
// if throwError is true, throw error if no settings are defined or 
// keys in request do not match those in replication settings
// throwError is false only when decoding CreateReplication request,
// where settings are optional and non-settings keys are present
func DecodeSettingsFromRequest(request *http.Request, throwError bool) (map[string]interface{}, error) {
	settings := make(map[string]interface{})
	
	if err := request.ParseForm(); err != nil {
		return nil, err
	}

	for key, valArr := range request.Form {
		settingsKey, _ := RestKeyToSettingsKeyMap[key]
		err := processKey(key, settingsKey, valArr, &settings)
		if err != nil {
			return nil, err
		}
	}
	
	if len(settings) == 0 && throwError {
		return nil, MissingSettingsInRequest
	}
	
	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	
	return settings, nil
	
}

func DecodeInternalSettingsFromRequest(request *http.Request) (map[string]interface{}, error) {
	settings := make(map[string]interface{})
	var err error
	
	if err = request.ParseForm(); err != nil {
		return nil, err
	}

	for key, valArr := range request.Form {
		restKey, err := ConvertRestInternalKeyToRestKey(key)
		if err != nil {
			// ignore non-internal settings key
			continue
		}
		settingsKey, _ := RestKeyToSettingsKeyMap[restKey]
		
		err = processKey(restKey, settingsKey, valArr, &settings)
		if err != nil {
			return nil, err
		}
	}
	
	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	
	return settings, nil
	
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
		restSettingsMap[restKey] = value
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

func getStringFromValArr(key string, valArr []string) (string, error) {
	if len(valArr) != 1 {
		return "", utils.InvalidValueInHttpRequestError(key, valArr)
	} else {
		return valArr[0], nil
	}
}

func processKey(restKey, settingsKey string, valArr []string, settingsPtr *map[string]interface{}) error {	
	settings := *settingsPtr
	var err error
		switch restKey {
			case FilterExpression:
				val, err := getStringFromValArr(restKey, valArr)
				if err != nil {
					return err
				}
				err = verifyFilterExpression(val) 
				if err != nil {
					errMsg := fmt.Sprintf("Invalid value, %v, for parameter, %v, in http request. It needs to be a valid regular expression.", val, restKey)
					return utils.NewEnhancedError(errMsg, err)
				}
				settings[settingsKey] = val
			case Type:	
				fallthrough
			case LogLevel:
				settings[settingsKey], err = getStringFromValArr(restKey, valArr)
				if err != nil {
					return err
				}
			case ReplicationType:	
				// nothing to do
			case Paused:
				val, err := getStringFromValArr(restKey, valArr)
				if err != nil {
					return err
				}
				paused, err := strconv.ParseBool(val)
				if err != nil {
					return utils.InvalidValueInHttpRequestError(restKey, val)
				}
				settings[settingsKey] = !paused
			case CheckpointInterval:
				fallthrough
			case BatchCount:
				fallthrough
			case BatchSize:
				fallthrough
			case FailureRestartInterval:
				fallthrough
			case OptimisticReplicationThreshold:
				fallthrough
			case HttpConnection:
				fallthrough
			case SourceNozzlePerNode:
				fallthrough
			case TargetNozzlePerNode:
				fallthrough
			case MaxExpectedReplicationLag:
				fallthrough
			case TimeoutPercentageCap:
				val, err := getStringFromValArr(restKey, valArr)
				if err != nil {
					return err
				}
				intVal, err := strconv.ParseInt(val, base.ParseIntBase, base.ParseIntBitSize)
				if err != nil {
					return utils.InvalidValueInHttpRequestError(restKey, val)
				}
				settings[settingsKey] = int(intVal)
			default:
				// ignore all other params
		}
	return nil
}
