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
	"encoding/json"
	"errors"
	"fmt"
	ap "github.com/couchbase/goxdcr/adminport"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// xdcr prefix for internal settings keys
var XDCRPrefix = "xdcr"
var ErrorsKey = "errors"

const (
	DefaultAdminPort = "8091"
)

// constants used for parsing url path
const (
	CreateReplicationPath    = "controller/createReplication"
	StatisticsPrefix         = "stats/buckets"
	RegexpValidationPrefix   = "controller/regexpValidation"
	InternalSettingsPath     = "internalSettings"
	AllReplicationsPath      = "pools/default/replications"
	AllReplicationInfosPath  = "pools/default/replicationInfos"
	DeleteReplicationPrefix  = "controller/cancelXDCR"
	SettingsReplicationsPath = "settings/replications"
	MemStatsPath             = "stats/mem"
	BlockProfileStartPath    = "profile/block/start"
	BlockProfileStopPath     = "profile/block/stop"
	BucketSettingsPrefix     = "controller/bucketSettings"
	XDCRInternalSettingsPath = "xdcr/internalSettings"

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
	SourceNozzlePerNode            = "sourceNozzlePerNode"
	TargetNozzlePerNode            = "targetNozzlePerNode"
	MaxExpectedReplicationLag      = "maxExpectedReplicationLag"
	TimeoutPercentageCap           = "timeoutPercentageCap"
	LogLevel                       = "logLevel"
	StatsInterval                  = "statsInterval"
	ReplicationTypeValue           = "continuous"
	GoMaxProcs                     = "goMaxProcs"
)

// constants for parsing create replication response
const (
	ReplicationId = "id"
)

// constants for RegexpValidation request
const (
	Expression = "expression"
	Keys       = "keys"
	StartIndex = "startIndex"
	EndIndex   = "endIndex"
)

// constants used for parsing bucket setting changes
const (
	BucketName = "bucketName"
	LWWEnabled = "lwwEnabled"
)

// constants for stats names
const (
	DocsWritten          = "docs_written"
	DataReplicated       = "data_replicated"
	ChangesLeft          = "changes_left"
	DocsChecked          = "docs_checked"
	NumCheckpoints       = "num_checkpoints"
	NumFailedCheckpoints = "num_failedckpts"
	SizeRepQueue         = "size_rep_queue"
	TimeCommiting        = "time_committing"
	BandWidthUsage       = "bandwidth_usage"
	DocsLatencyAppr      = "docs_latency_aggr"
	DocsLatencyWt        = "docs_latency_wt"
	DocsRepQueue         = "docs_req_queue"
	MetaLatencyAggr      = "meta_latency_aggr"
	MetaLatencyWt        = "meta_latency_wt"
	RateReplication      = "rate_replication"
	DocsOptRepd          = "docs_opt_repd"
	ActiveVbreps         = "active_vbreps"
	WaitingVbreps        = "waiting_vbreps"
	TimeWorking          = "time_working"
	TimeoutPercentageMap = "timeout_percentage_map"
)

// errors
var ErrorParsingForm = errors.New("Error parsing http request")
var MissingSettingsInRequest = errors.New("Invalid http request. No replication setting parameters have been supplied.")
var MissingOldSettingsInRequest = errors.New("Invalid http request. No old replication settings have been supplied.")

// replication settings key in rest api -> internal replication settings key
var RestKeyToSettingsKeyMap = map[string]string{
	Type:                           metadata.ReplicationType,
	FilterExpression:               metadata.FilterExpression,
	PauseRequested:                 metadata.Active,
	CheckpointInterval:             metadata.CheckpointInterval,
	BatchCount:                     metadata.BatchCount,
	BatchSize:                      metadata.BatchSize,
	FailureRestartInterval:         metadata.FailureRestartInterval,
	OptimisticReplicationThreshold: metadata.OptimisticReplicationThreshold,
	SourceNozzlePerNode:            metadata.SourceNozzlePerNode,
	TargetNozzlePerNode:            metadata.TargetNozzlePerNode,
	/*MaxExpectedReplicationLag:      metadata.MaxExpectedReplicationLag,
	TimeoutPercentageCap:           metadata.TimeoutPercentageCap,*/
	LogLevel:      metadata.PipelineLogLevel,
	StatsInterval: metadata.PipelineStatsInterval,
	GoMaxProcs:    metadata.GoMaxProcs,
}

// internal replication settings key -> replication settings key in rest api
var SettingsKeyToRestKeyMap = map[string]string{
	metadata.ReplicationType:                Type,
	metadata.FilterExpression:               FilterExpression,
	metadata.Active:                         PauseRequested,
	metadata.CheckpointInterval:             CheckpointInterval,
	metadata.BatchCount:                     BatchCount,
	metadata.BatchSize:                      BatchSize,
	metadata.FailureRestartInterval:         FailureRestartInterval,
	metadata.OptimisticReplicationThreshold: OptimisticReplicationThreshold,
	metadata.SourceNozzlePerNode:            SourceNozzlePerNode,
	metadata.TargetNozzlePerNode:            TargetNozzlePerNode,
	/*metadata.MaxExpectedReplicationLag:      MaxExpectedReplicationLag,
	metadata.TimeoutPercentageCap:           TimeoutPercentageCap,*/
	metadata.PipelineLogLevel:      LogLevel,
	metadata.PipelineStatsInterval: StatsInterval,
	metadata.GoMaxProcs:            GoMaxProcs,
}

var logger_msgutil *log.CommonLogger = log.NewLogger("MessageUtils", log.DefaultLoggerContext)

func NewGetRemoteClustersResponse(remoteClusters map[string]*metadata.RemoteClusterReference) (*ap.Response, error) {
	remoteClusterArr := make([]map[string]interface{}, 0)
	for _, remoteCluster := range remoteClusters {
		remoteClusterArr = append(remoteClusterArr, remoteCluster.ToMap())
	}
	return EncodeObjectIntoResponse(remoteClusterArr)
}

func NewGetAllReplicationsResponse(replSpecs map[string]*metadata.ReplicationSpecification) (*ap.Response, error) {
	// UI requires that the specs are in sorted order to avoid flicking
	specIds := make([]string, 0)
	for specId, _ := range replSpecs {
		specIds = append(specIds, specId)
	}
	sort.Strings(specIds)

	replArr := make([]map[string]interface{}, 0)
	for _, specId := range specIds {
		replArr = append(replArr, getReplicationDocMap(replSpecs[specId]))
	}
	return EncodeObjectIntoResponse(replArr)
}

func NewGetAllReplicationInfosResponse(replInfos []base.ReplicationInfo) (*ap.Response, error) {
	return EncodeObjectIntoResponse(replInfos)
}

func getReplicationDocMap(replSpec *metadata.ReplicationSpecification) map[string]interface{} {
	replDocMap := make(map[string]interface{})
	if replSpec != nil {
		replDocMap[base.ReplicationDocId] = replSpec.Id
		replDocMap[base.ReplicationDocContinuous] = true
		replDocMap[base.ReplicationDocSource] = replSpec.SourceBucketName
		replDocMap[base.ReplicationDocTarget] = base.UrlDelimiter + base.RemoteClustersForReplicationDoc + base.UrlDelimiter + replSpec.TargetClusterUUID + base.UrlDelimiter + base.BucketsPath + base.UrlDelimiter + replSpec.TargetBucketName

		// special transformation for replication type and active flag
		replDocMap[base.ReplicationDocPauseRequestedOutput] = !replSpec.Settings.Active
		if replSpec.Settings.RepType == metadata.ReplicationTypeXmem {
			replDocMap[base.ReplicationDocType] = base.ReplicationDocTypeXmem
		} else {
			replDocMap[base.ReplicationDocType] = base.ReplicationDocTypeCapi
		}

		// copy other replication settings into replication doc
		for key, value := range replSpec.Settings.ToMap() {
			if key != metadata.ReplicationType && key != metadata.Active {
				replDocMap[key] = value
			}
		}
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
			justValidate, err := getBoolFromValArr(valArr, false)
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
	var name, hostName, userName, password string
	var certificate []byte

	// default to false if not passed in
	demandEncryption := false

	if err = request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		err = nil
		return
	}

	for key, valArr := range request.Form {
		switch key {
		case base.JustValidate:
			justValidate, err = getBoolFromValArr(valArr, false)
			if err != nil {
				errorsMap[base.JustValidate] = err
			}
		case base.RemoteClusterName:
			name = getStringFromValArr(valArr)
		case base.RemoteClusterHostName:
			hostName = getStringFromValArr(valArr)
		case base.RemoteClusterUserName:
			userName = getStringFromValArr(valArr)
		case base.RemoteClusterPassword:
			password = getStringFromValArr(valArr)
		case base.RemoteClusterDemandEncryption:
			demandEncryption = getDemandEncryptionFromValArr(valArr)
		case base.RemoteClusterCertificate:
			certificateStr := getStringFromValArr(valArr)
			certificate = []byte(certificateStr)
		default:
			// ignore other parameters
		}
	}

	// check required parameters
	if len(name) == 0 {
		errorsMap[base.RemoteClusterName] = simple_utils.MissingParameterError("cluster name")
	}
	if len(hostName) == 0 {
		errorsMap[base.RemoteClusterHostName] = simple_utils.MissingParameterError("hostname (ip)")
	}
	if len(userName) == 0 {
		errorsMap[base.RemoteClusterUserName] = simple_utils.MissingParameterError("username")
	}
	if len(password) == 0 {
		errorsMap[base.RemoteClusterPassword] = simple_utils.MissingParameterError("password")
	}

	// certificate is required if demandEncryption is set to true
	if demandEncryption && len(certificate) == 0 {
		errorsMap[base.RemoteClusterCertificate] = errors.New("certificate must be given if demand encryption is on")
	}

	//validate the format of hostName, if it doesn't contain port number, append default port number 8091
	if !strings.Contains(hostName, base.UrlPortNumberDelimiter) {
		hostName = hostName + base.UrlPortNumberDelimiter + DefaultAdminPort
	}
	if len(errorsMap) == 0 {
		remoteClusterRef, err = metadata.NewRemoteClusterReference("", name, hostName, userName, password, demandEncryption, certificate)
	}

	return
}

func NewCreateRemoteClusterResponse(remoteClusterRef *metadata.RemoteClusterReference) (*ap.Response, error) {
	return EncodeObjectIntoResponse(remoteClusterRef.ToMap())
}

func NewOKResponse() (*ap.Response, error) {
	// return "ok" in success case
	return EncodeByteArrayIntoResponse([]byte("\"ok\""))
}

func NewEmptyArrayResponse() (*ap.Response, error) {
	// return empty array in success case
	return EncodeObjectIntoResponse(make([]map[string]interface{}, 0))
}

// decode parameters from create replication request
func DecodeCreateReplicationRequest(request *http.Request) (justValidate bool, fromBucket, toCluster, toBucket string, settings map[string]interface{}, errorsMap map[string]error, err error) {
	errorsMap = make(map[string]error)
	var replicationType string

	if err = request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		err = nil
		return
	}

	for key, valArr := range request.Form {
		switch key {
		case ReplicationType:
			replicationType = getStringFromValArr(valArr)
			if replicationType != ReplicationTypeValue {
				errorsMap[ReplicationType] = simple_utils.GenericInvalidValueError(ReplicationType)
			}
		case base.FromBucket:
			fromBucket = getStringFromValArr(valArr)
		case base.ToCluster:
			toCluster = getStringFromValArr(valArr)
		case base.ToBucket:
			toBucket = getStringFromValArr(valArr)
		case base.JustValidate:
			justValidate, err = getBoolFromValArr(valArr, false)
			if err != nil {
				errorsMap[base.JustValidate] = err
			}
		default:
			// ignore other parameters
		}
	}

	if len(replicationType) == 0 {
		errorsMap[ReplicationType] = simple_utils.MissingValueError("replication type")
	}

	if len(fromBucket) == 0 {
		errorsMap[base.FromBucket] = simple_utils.MissingValueError("source bucket")
	}
	if len(toCluster) == 0 {
		errorsMap[base.ToCluster] = simple_utils.MissingValueError("target cluster")
	}
	if len(toBucket) == 0 {
		errorsMap[base.ToBucket] = simple_utils.MissingValueError("target bucket")
	}

	settings, settingsErrorsMap := DecodeSettingsFromRequest(request, false, false)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}

	isEnterprise, err := XDCRCompTopologyService().IsMyClusterEnterprise()
	if err != nil {
		return
	}

	if !isEnterprise {
		filterExpression, ok := settings[metadata.FilterExpression]
		if ok && len(filterExpression.(string)) > 0 {
			errorsMap[FilterExpression] = errors.New("Filter expression can be specified in Enterprise edition only")
		}
	}

	return
}

func DecodeChangeReplicationSettings(request *http.Request, isDefaultSettings bool) (justValidate bool, settings map[string]interface{}, errorsMap map[string]error) {
	errorsMap = make(map[string]error)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return
	}

	justValidate, err := DecodeJustValidateFromRequest(request)
	if err != nil {
		errorsMap[base.JustValidate] = err
	}

	settings, settingsErrorsMap := DecodeSettingsFromRequest(request, isDefaultSettings, true)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}

	return
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
		return "", simple_utils.MissingParameterInHttpResponseError(ReplicationId)
	}

	replicationIdStr, ok := replicationId.(string)
	if !ok {
		return "", simple_utils.IncorrectValueTypeInHttpResponseError(ReplicationId, replicationId, "string")
	}

	return replicationIdStr, nil

}

// decode replication settings related parameters from http request
func DecodeSettingsFromRequest(request *http.Request, isDefaultSettings bool, isUpdate bool) (map[string]interface{}, map[string]error) {
	settings := make(map[string]interface{})
	errorsMap := make(map[string]error)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return nil, errorsMap
	}

	for key, valArr := range request.Form {
		err := processKey(key, valArr, &settings, isDefaultSettings, isUpdate)
		if err != nil {
			errorsMap[key] = err
		}
	}

	if len(errorsMap) > 0 {
		return nil, errorsMap
	}

	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	return settings, nil
}

// decode replication settings related parameters from /internalSettings http request
func DecodeSettingsFromInternalSettingsRequest(request *http.Request) (map[string]interface{}, map[string]error) {
	settings := make(map[string]interface{})
	errorsMap := make(map[string]error)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return nil, errorsMap
	}

	for key, valArr := range request.Form {
		restKey, err := ConvertRestInternalKeyToRestKey(key)
		if err != nil {
			// ignore non-internal settings key
			continue
		}

		err = processKey(restKey, valArr, &settings, true /*isDefaultSettings*/, false /*isUpdate*/)
		if err != nil {
			errorsMap[restKey] = err
		}
	}

	if len(errorsMap) > 0 {
		return nil, errorsMap
	}

	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	return settings, nil
}

func DecodeSettingsFromXDCRInternalSettingsRequest(request *http.Request) (map[string]interface{}, map[string]error) {
	settings := make(map[string]interface{})
	errorsMap := make(map[string]error)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return nil, errorsMap
	}

	for key, valArr := range request.Form {
		convertedValue, err := metadata.ValidateAndConvertXDCRInternalSettingsValue(key, valArr[0])
		if err != nil {
			errorsMap[key] = err
		} else {
			settings[key] = convertedValue
		}
	}

	if len(errorsMap) > 0 {
		return nil, errorsMap
	}

	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	return settings, nil
}

func DecodeRegexpValidationRequest(request *http.Request) (string, []string, error) {
	var expression string
	var keys []string

	if err := request.ParseForm(); err != nil {
		return "", nil, err
	}

	for key, valArr := range request.Form {
		switch key {
		case Expression:
			expression = getStringFromValArr(valArr)
		case Keys:
			keysStr := getStringFromValArr(valArr)
			err := json.Unmarshal([]byte(keysStr), &keys)
			if err != nil {
				return "", nil, utils.NewEnhancedError(fmt.Sprintf("Error parsing keys=%v.", keysStr), err)
			}
		default:
			// ignore other parameters
		}
	}

	if len(expression) == 0 {
		return "", nil, simple_utils.MissingParameterError("expression")
	}

	return expression, keys, nil
}

func NewCreateReplicationResponse(replicationId string) (*ap.Response, error) {
	params := make(map[string]interface{})
	params[ReplicationId] = replicationId
	return EncodeObjectIntoResponse(params)
}

func NewReplicationSettingsResponse(settings *metadata.ReplicationSettings) (*ap.Response, error) {
	if settings == nil {
		return NewEmptyArrayResponse()

	} else {
		return EncodeObjectIntoResponse(convertSettingsToRestSettingsMap(settings, false))
	}
}

func NewDefaultReplicationSettingsResponse(settings *metadata.ReplicationSettings, globalSettings *metadata.GlobalSettings) (*ap.Response, error) {
	if settings == nil || globalSettings == nil {
		return NewEmptyArrayResponse()
	} else {
		globalSettingsMap := convertGlobalSettingsToRestSettingsMap(globalSettings)
		replicationSettingMap := convertSettingsToRestSettingsMap(settings, true)
		for key, value := range globalSettingsMap {
			replicationSettingMap[key] = value
		}

		return EncodeObjectIntoResponse(replicationSettingMap)
	}
}

func NewInternalSettingsResponse(settings *metadata.ReplicationSettings, globalSettings *metadata.GlobalSettings) (*ap.Response, error) {
	if settings == nil || globalSettings == nil {
		return NewEmptyArrayResponse()
	} else {
		globalSettingsMap := convertGlobalSettingsToRestInternalSettingsMap(globalSettings)
		replicationSettingMap := convertSettingsToRestInternalSettingsMap(settings)
		for key, value := range globalSettingsMap {
			replicationSettingMap[key] = value
		}
		return EncodeObjectIntoResponse(replicationSettingMap)
	}
}

func NewXDCRInternalSettingsResponse(settings *metadata.InternalSettings) (*ap.Response, error) {
	if settings == nil {
		return NewEmptyArrayResponse()
	} else {
		return EncodeObjectIntoResponse(settings.ToMap())
	}
}

func NewRegexpValidationResponse(matchesMap map[string][][]int) (*ap.Response, error) {
	returnMap := make(map[string]interface{})

	for key, matches := range matchesMap {
		convertedMatches := make([]map[string]int, 0)
		for _, match := range matches {
			convertedMatch := make(map[string]int)
			convertedMatch[StartIndex] = match[0]
			convertedMatch[EndIndex] = match[1]
			convertedMatches = append(convertedMatches, convertedMatch)
		}
		returnMap[key] = convertedMatches
	}

	return EncodeObjectIntoResponse(returnMap)
}

// decode dynamic paramater from the path of http request
func DecodeDynamicParamInURL(request *http.Request, pathPrefix string, paramName string) (string, error) {
	// length of prefix preceding replicationId in request url path
	prefixLength := len(base.AdminportUrlPrefix) + len(pathPrefix) + len(base.UrlDelimiter)

	if len(request.URL.Path) <= prefixLength {
		return "", simple_utils.MissingParameterInHttpRequestUrlError(paramName, request.URL.Path)
	}

	paramValue := request.URL.Path[prefixLength:]

	logger_msgutil.Debugf("param value decoded from request: %v\n", paramValue)
	return paramValue, nil
}

func verifyFilterExpression(filterExpression string) error {
	_, err := regexp.Compile(filterExpression)
	return err
}

func convertGlobalSettingsToRestSettingsMap(settings *metadata.GlobalSettings) map[string]interface{} {
	restSettingsMap := make(map[string]interface{})
	var settingsMap map[string]interface{}

	settingsMap = settings.ToMap()
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		restSettingsMap[restKey] = value
	}
	return restSettingsMap
}

func convertGlobalSettingsToRestInternalSettingsMap(settings *metadata.GlobalSettings) map[string]interface{} {
	internalSettingsMap := make(map[string]interface{})
	settingsMap := settings.ToMap()
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		internalSettingsKey := ConvertRestKeyToRestInternalKey(restKey)
		internalSettingsMap[internalSettingsKey] = value
	}
	return internalSettingsMap
}

func convertSettingsToRestSettingsMap(settings *metadata.ReplicationSettings, isDefaultSettings bool) map[string]interface{} {
	restSettingsMap := make(map[string]interface{})
	var settingsMap map[string]interface{}
	if isDefaultSettings {
		settingsMap = settings.ToDefaultSettingsMap()
	} else {
		settingsMap = settings.ToMap()
	}

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
	settingsMap := settings.ToDefaultSettingsMap()
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

func getStringFromValArr(valArr []string) string {
	if len(valArr) == 0 {
		return ""
	} else {
		return valArr[0]
	}
}

func getBoolFromValArr(valArr []string, defaultValue bool) (bool, error) {
	boolStr := getStringFromValArr(valArr)
	if boolStr != "" {
		result, err := strconv.ParseBool(boolStr)
		if err != nil {
			return defaultValue, simple_utils.IncorrectValueTypeError("a boolean")
		}
		return result, nil
	}

	return defaultValue, nil
}

func EncodeRemoteClusterErrorsMapIntoResponse(errorsMap map[string]error) (*ap.Response, error) {
	return EncodeErrorsMapIntoResponse(errorsMap, false)
}

// encode a map of validation errors into a Response object, which can then be returned in a http response
// when withErrorsWrapper is false, simply return a map of error messages -- this is needed by remote cluster rest APIs
// when withErrorsWrapper is true, wrap the map of error messages in an outer map -- this is needed by replication rest APIs
func EncodeErrorsMapIntoResponse(errorsMap map[string]error, withErrorsWrapper bool) (*ap.Response, error) {
	errorMsgMap := make(map[string]string)
	for key, err := range errorsMap {
		errorMsgMap[key] = err.Error()
	}

	if !withErrorsWrapper {
		// validation errors cause StatusBadRequest to be returned to client
		return EncodeObjectIntoResponseWithStatusCode(errorMsgMap, http.StatusBadRequest)
	}

	result := make(map[string]interface{})
	result[ErrorsKey] = errorMsgMap

	// validation errors cause StatusBadRequest to be returned to client
	return EncodeObjectIntoResponseWithStatusCode(result, http.StatusBadRequest)
}

func EncodeInternalSettingsErrorsMapIntoResponse(errorsMap map[string]error) (*ap.Response, error) {
	bytes := []byte{'{'}
	for key, _ := range errorsMap {
		// use the same format as other internal settings validation errors
		// i.e., use "error" as the key and "xxx is invalid" as the value.
		// e.g., {"error":"xdcrDocBatchSizeKb is invalid","error":"xdcrWorkerBatchSize is invalid"}
		// can not use map (since the keys are all the same) and have to manually encode the byte array to be returned
		internalSettingsKey := ConvertRestKeyToRestInternalKey(key)
		entry := "\"error\":\"" + simple_utils.GenericInvalidValueError(internalSettingsKey).Error() + "\","
		bytes = append(bytes, []byte(entry)...)
	}

	// replace the last, redundant ',', with '}' to complete the encoding
	// note that this works only when errorsMaps is not empty, which is always true
	bytes[len(bytes)-1] = '}'

	return EncodeByteArrayIntoResponseWithStatusCode(bytes, http.StatusBadRequest)
}

func EncodeReplicationValidationErrorIntoResponse(err error) (*ap.Response, error) {
	return EncodeValidationErrorIntoResponse(err, true)
}

func EncodeRemoteClusterValidationErrorIntoResponse(err error) (*ap.Response, error) {
	return EncodeValidationErrorIntoResponse(err, false)
}

// encode a validation error into Response object by wrapping it in an errorMap
func EncodeValidationErrorIntoResponse(err error, withErrorsWrapper bool) (*ap.Response, error) {
	errorsMap := make(map[string]error)
	errorsMap[base.PlaceHolderFieldKey] = err
	return EncodeErrorsMapIntoResponse(errorsMap, withErrorsWrapper)
}

// encode the error message of an error, without any wrapping, into Response object.
func EncodeErrorMessageIntoResponse(err error, statusCode int) (*ap.Response, error) {
	if err != nil {
		return EncodeByteArrayIntoResponseWithStatusCode([]byte(err.Error()), statusCode)
	} else {
		return NewEmptyArrayResponse()
	}
}

// encode a byte array into Response object with default status code of StatusOK
func EncodeByteArrayIntoResponse(data []byte) (*ap.Response, error) {
	return EncodeByteArrayIntoResponseWithStatusCode(data, http.StatusOK)
}

// encode a byte array into Response object with specified status code
func EncodeByteArrayIntoResponseWithStatusCode(data []byte, statusCode int) (*ap.Response, error) {
	return &ap.Response{statusCode, data}, nil
}

// encode an arbitrary object into Response object with default status code of StatusOK
func EncodeObjectIntoResponse(object interface{}) (*ap.Response, error) {
	return EncodeObjectIntoResponseWithStatusCode(object, http.StatusOK)
}

// encode an arbitrary object into Response object with specified status code
func EncodeObjectIntoResponseWithStatusCode(object interface{}, statusCode int) (*ap.Response, error) {
	var body []byte
	if object == nil {
		body = []byte{}
	} else {
		var err error
		body, err = json.Marshal(object)
		if err != nil {
			return nil, err
		}
	}
	return EncodeByteArrayIntoResponseWithStatusCode(body, statusCode)
}

func EncodeAuthorizationErrorMessageIntoResponse(permission string) (*ap.Response, error) {
	msgMap := make(map[string]interface{})
	// use the same error message as that produced by ns_server
	msgMap["message"] = "Forbidden. User needs one of the following permissions"
	msgMap["permissions"] = []string{permission}
	return EncodeObjectIntoResponseWithStatusCode(msgMap, http.StatusForbidden)
}

func EncodeAuthorizationErrorMessageIntoResponse2(permissions []string) (*ap.Response, error) {
	msgMap := make(map[string]interface{})
	msgMap["message"] = "Forbidden. User needs all of the following permissions"
	msgMap["permissions"] = permissions
	return EncodeObjectIntoResponseWithStatusCode(msgMap, http.StatusForbidden)
}

// Remote cluster related errors can be internal server error or less servere invalid/unknown remote cluster errors,
// return different Response for them
func EncodeRemoteClusterErrorIntoResponse(err error) (*ap.Response, error) {
	if err != nil {
		isValidationError, unwrapperError := RemoteClusterService().CheckAndUnwrapRemoteClusterError(err)
		if isValidationError {
			return EncodeRemoteClusterValidationErrorIntoResponse(unwrapperError)
		} else {
			return nil, err
		}
	} else {
		return NewEmptyArrayResponse()
	}
}

// Replication spec related errors can be internal server error or less servere replication spec not found/already exists errors,
// return different Response for them
func EncodeReplicationSpecErrorIntoResponse(err error) (*ap.Response, error) {
	if err != nil {
		if ReplicationSpecService().IsReplicationValidationError(err) {
			return EncodeReplicationValidationErrorIntoResponse(err)
		} else {
			return nil, err
		}
	} else {
		return NewEmptyArrayResponse()
	}

}

func processKey(restKey string, valArr []string, settingsPtr *map[string]interface{}, isDefaultSettings bool, isUpdate bool) error {
	settingsKey, ok := RestKeyToSettingsKeyMap[restKey]
	if !ok {
		// ignore non-settings key
		return nil
	}

	if isDefaultSettings && !metadata.IsSettingDefaultValueMutable(settingsKey) {
		// ignore settings whose default values cannot be changed
		return nil
	}

	if isUpdate && !metadata.IsSettingValueMutable(settingsKey) {
		return errors.New("Setting value cannot be modified after replication is created.")
	}

	convertedValue, err := validateAndConvertAllSettingValue(settingsKey, valArr[0], restKey)
	if err == nil {
		(*settingsPtr)[settingsKey] = convertedValue
	}
	return err
}

func validateAndConvertAllSettingValue(key, value, restKey string) (convertedValue interface{}, err error) {
	//check if value is replication specific setting
	convertedValue, err = metadata.ValidateAndConvertSettingsValue(key, value, restKey)
	//if we find converted value is null  than check if value is global process specific setting
	if convertedValue == nil {
		convertedValue, err = metadata.ValidateAndConvertGlobalSettingsValue(key, value, restKey)
	}
	return
}

// check if encryption is enabled from the valArr of demandEncryption parameter in http request
func getDemandEncryptionFromValArr(valArr []string) bool {
	demandEncryptionStr := getStringFromValArr(valArr)
	demandEncryptionInt, err := strconv.ParseInt(demandEncryptionStr, base.ParseIntBase, base.ParseIntBitSize)
	if err == nil && demandEncryptionInt == 0 {
		// int value of 0 indicates that encryption is not enabled
		return false
	} else {
		// any other value, e.g., "", 1, "on", "true", "false", etc., indicates that encryption is enabled
		return true
	}
}

func DecodeBucketSettingsChangeRequest(request *http.Request) (bool, error) {
	var lwwEnabled bool
	var err error

	if err = request.ParseForm(); err != nil {
		return false, err
	}

	lwwEnabledFound := false

	for key, valArr := range request.Form {
		switch key {
		case LWWEnabled:
			lwwEnabled, err = getBoolFromValArr(valArr, false)
			if err != nil {
				return false, err
			}
			lwwEnabledFound = true
		default:
			// ignore other parameters
		}
	}

	if !lwwEnabledFound {
		return false, simple_utils.MissingParameterError(LWWEnabled)
	}
	return lwwEnabled, nil
}
