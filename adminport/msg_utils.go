package adminport

import (
	"encoding/json"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	metadata "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	"strconv"
	"net/http"
	"io/ioutil"
	"errors"
)

// http request method types
const (
	MethodGet = "GET"
	MethodPost = "POST"
	MethodDelete = "DELETE"
)

// constants used for parsing url path
const (
	UrlDelimiter = "/"
	UrlPortNumberDelimiter = ":"
	

	CreateReplicationPath    = "controller/createReplication"
	InternalSettingsPath     = "internalSettings"
	SettingsReplicationsPath = "settings/replications"
	DeleteReplicationPrefix  = "controller/cancelXDCR"
	StatisticsPath         = "stats"
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path.
	// e.g., settings/replications/dynamic
	DynamicSuffix = UrlDelimiter + "dynamic"
)

// constants used for parsing internal settings
const (
	ReplicationType                = "xdcrReplicationType"
	FilterExpression               = "xdcrFilterExpression"
	Active                         = "xdcrActive"
	CheckpointInterval             = "xdcrCheckpointInterval"
	BatchCount                     = "xdcrWorkerBatchSize"
	BatchSize                      = "xdcrDocBatchSizeKb"
	FailureRestartInterval         = "xdcrFailureRestartInterval"
	OptimisticReplicationThreshold = "xdcrOptimisticReplicationThreshold"
	HttpConnection                 = "httpConnections"
	SourceNozzlePerNode            = "xdcrSourceNozzlePerNode"
	TargetNozzlePerNode            = "xdcrTargetNozzlePerNode"
	MaxExpectedReplicationLag      = "xdcrMaxExpectedReplicationLag"
	TimeoutPercentageCap           = "xdcrTimeoutPercentageCap"
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

// json content type for http request and response
var JsonType = "application/json"

// replication settings key in rest api -> internal replication settings key
var ReplSettingRestToInternalMap = map[string]string {
	ReplicationType: metadata.ReplicationType,
	FilterExpression: metadata.FilterExpression,
	Active: metadata.Active,
	CheckpointInterval: metadata.CheckpointInterval,
	BatchCount: metadata.BatchCount,
	BatchSize: metadata.BatchSize,
	FailureRestartInterval: metadata.FailureRestartInterval,
	OptimisticReplicationThreshold: metadata.OptimisticReplicationThreshold,
	SourceNozzlePerNode: metadata.SourceNozzlePerNode,
	TargetNozzlePerNode: metadata.TargetNozzlePerNode,
	MaxExpectedReplicationLag: metadata.MaxExpectedReplicationLag,
	TimeoutPercentageCap: metadata.TimeoutPercentageCap,
} 

// internal replication settings key -> replication settings key in rest api
var ReplSettingInternalToRestMap = map[string]string {
	metadata.ReplicationType: ReplicationType,
	metadata.FilterExpression: FilterExpression,
	metadata.Active: Active,
	metadata.CheckpointInterval: CheckpointInterval,
	metadata.BatchCount: BatchCount,
	metadata.BatchSize: BatchSize,
	metadata.FailureRestartInterval: FailureRestartInterval,
	metadata.OptimisticReplicationThreshold: OptimisticReplicationThreshold,
	metadata.SourceNozzlePerNode: SourceNozzlePerNode,
	metadata.TargetNozzlePerNode: TargetNozzlePerNode,
	metadata.MaxExpectedReplicationLag: MaxExpectedReplicationLag,
	metadata.TimeoutPercentageCap: TimeoutPercentageCap,
} 

var logger_msgutil *log.CommonLogger = log.NewLogger("MessageUtils", log.LogLevelInfo)

// decode parameters from create replication request
func DecodeCreateReplicationRequest(request *http.Request) (fromBucket, toCluster, toBucket, filterName string, forward bool, settings map[string]interface{}, err error) {
	if err = request.ParseForm(); err != nil {
		return 
	}
	
	// forward defaults to true if not specified
	forward = true

	for key, valArr := range request.Form {
		if len(valArr) != 1 {
			err = utils.InvalidValueInHttpRequestError(key, valArr)
			return
		}
		val := valArr[0]
		
		switch key {
		case FromBucket:
			fromBucket = val
		case ToCluster:
			toCluster = val
		case ToBucket:
			toBucket = val
		case FilterName:
			filterName = val
		case Forward:
			forward, err = strconv.ParseBool(val)
			if err != nil {
				err = utils.InvalidValueInHttpRequestError(key, val)
				return
			}
		default:
			// other keys must be for replication settings.
			_, ok := ReplSettingRestToInternalMap[key]
			if !ok {
				err = utils.InvalidParameterInHttpRequestError(key)
				return
			}
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

// create a new DeleteReplication request for specified replicationId and the specified node
func NewDeleteReplicationRequest(replicationId, nodeAddr string, port int) (*http.Request, error) {
	// replicatioId is cancatenated into the url 
	url := GetHostAddr(nodeAddr, port) + base.AdminportUrlPrefix + DeleteReplicationPrefix + UrlDelimiter + replicationId
	return http.NewRequest(MethodDelete, url, nil)
}

// return host address in the form of nodeAddr:port
func GetHostAddr(nodeAddr string, port int) string {
	return nodeAddr + UrlPortNumberDelimiter + strconv.FormatInt(int64(port), base.ParseIntBase)
}

// decode replicationId from create replication response
func DecodeCreateReplicationResponse(response *http.Response) (replicationId string, err error) {
	defer response.Body.Close()

	// create replication response is a map
	var resultMap = make(map[string]string)
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return
	}
	
	err = json.Unmarshal(bodyBytes, resultMap)
	if err != nil {
		return
	}
	replicationId, ok := resultMap[ReplicationId]
	if !ok {
		err = utils.MissingParameterInHttpResponseError(ReplicationId)
	}
	return
	
}

// decode parameters from delete replication request
func DecodeDeleteReplicationRequest(request *http.Request) (replicationId string, forward bool, err error) {
	replicationId, err = DecodeReplicationIdFromHttpRequest(request, DeleteReplicationPrefix)
	if err != nil {
		return 
	}
	
	if err = request.ParseForm(); err != nil {
		return 
	}

	// get forward flag from request body
	
	// forward defaults to true if not specified
	forward = true
	for key, val := range request.Form {
		switch key {
			case Forward:
				forward, err = strconv.ParseBool(val[0])
				if err != nil {
					err = utils.InvalidValueInHttpRequestError(key, val[0])
					return
				}
			default:
				err = utils.InvalidParameterInHttpRequestError(key)
				return
		}
	}
	
	return
	
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
		internalKey, ok := ReplSettingRestToInternalMap[key]
		if !ok {
			if throwError {
				err := utils.InvalidParameterInHttpRequestError(key)
				return nil, err
			} else {
				continue
			}
		}
		
		if len(valArr) != 1 {
			err := utils.InvalidValueInHttpRequestError(key, valArr)
			return nil, err
		}
		
		val := valArr[0]
		
		switch key {
			case ReplicationType:	
				fallthrough
			case FilterExpression:
				settings[internalKey] = val
			case Active:
				active, err := strconv.ParseBool(val)
				if err != nil {
					err = utils.InvalidValueInHttpRequestError(key, val)
					return nil, err
				}
				settings[internalKey] = active
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
				intVal, err := strconv.ParseInt(val, base.ParseIntBase, base.ParseIntBitSize)
				if err != nil {
					err = utils.InvalidValueInHttpRequestError(key, val)
					return nil, err
				}
				settings[internalKey] = int(intVal)
		}
	}
	
	if len(settings) == 0 && throwError {
		return nil, errors.New("Invalid http request. No replication setting parameters have been supplied.")
	}
	
	logger_msgutil.Debugf("settings decoded from request: %v\n", settings)
	
	return settings, nil
	
}

func NewCreateReplicationResponse(replicationId string) ([]byte, error) {
	resultMap := make(map[string]string)
	resultMap[ReplicationId] = replicationId
	return json.Marshal(resultMap)
}

func NewViewInternalSettingsResponse(settings *metadata.ReplicationSettings) ([]byte, error) {
	resultMap := make(map[string]interface{})
	for key, val := range settings.ToMap() {
		restKey := ReplSettingInternalToRestMap[key]
		resultMap[restKey] = val
	}
	return json.Marshal(resultMap)
}

// decode replication id from http request
func DecodeReplicationIdFromHttpRequest(request *http.Request, pathPrefix string) (string, error) {
	// length of prefix preceding replicationId in request url path 
	prefixLength := len(base.AdminportUrlPrefix) + len(pathPrefix) + len(UrlDelimiter)
	
	if len(request.URL.Path) <= prefixLength {		
		return "", utils.MissingReplicationIdInHttpRequestError(request.URL.Path)
	}

	replicationId := request.URL.Path[prefixLength:]
	logger_msgutil.Debugf("replication id decoded from request: %v\n", replicationId)
	return replicationId, nil
}

