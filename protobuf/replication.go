package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	"regexp"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
)

const(
	UrlDelimiter = "/"
	
	CreateReplicationPath = "/controller/createReplication"
	InternalSettingsPath = "/internalSettings"
	SettingsReplicationsPath = "/settings/replications/"
	DeleteReplicationPrefix = "/controller/cancelXDCR/"
	StatisticsPrefix = "/pools/default/buckets"
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path. 
	// e.g., settings/replications/dynamic
	DynamicSuffix = UrlDelimiter + "dynamic"
	// The same path, e.g.,SETTINGS_REPLICATION_PATH, may be used for two different APIs: look up and modify. 
	// The following suffixes are used to distinguish between these two cases
	GetSuffix = UrlDelimiter + "get"
	PostSuffix = UrlDelimiter + "post"
	
	StatsPathPattern = ".*pools/default/buckets/[^/]*/stats/replications"
	
)

var statsPathRegexp, _ = regexp.Compile(StatsPathPattern)

var logger_repmsg *log.CommonLogger = log.NewLogger("ReplicationMessages", log.LogLevelInfo)

// create new Settings message from ReplicationSettings object
func NewInternalSettings(replSettings *metadata.ReplicationSettings) *InternalSettings {
	checkpointInterval := uint32(replSettings.CheckpointInterval())
	batchCount :=  uint32(replSettings.BatchCount())
	batchSize :=           uint32(replSettings.BatchSize()) 
	failureRestartInterval :=   uint32(replSettings.FailureRestartInterval())
	optimisticReplicationThreshold := uint32(replSettings.OptimisticReplicationThreshold())
	httpConnections :=           uint32(replSettings.HttpConnection())
	sourceNozzlePerNode :=   uint32(replSettings.SourceNozzlesPerNode())
	targetNozzlePerNode :=   uint32(replSettings.TargetNozzlesPerNode())
	maxExpectedReplicationLag := uint32(replSettings.MaxExpectedReplicationLag())
	timeoutPercentageCap := uint32(replSettings.TimeoutPercentageCap())
	return &InternalSettings{                  
		XdcrCheckpointInterval:  &checkpointInterval,       
		XdcrWorkerBatchSize:       &batchCount,         
		XdcrDocBatchSizeKb:           &batchSize,        
		XdcrFailureRestartInterval:   &failureRestartInterval,        
		XdcrOptimisticReplicationThreshold: &optimisticReplicationThreshold,  
		HttpConnections:           &httpConnections,       
		XdcrSourceNozzlePerNode:   &sourceNozzlePerNode,           
		XdcrTargetNozzlePerNode:   &targetNozzlePerNode,           
		XdcrMaxExpectedReplicationLag: &maxExpectedReplicationLag,       
		XdcrTimeoutPercentageCap:  &timeoutPercentageCap,
	}
}

// convert InternalSettings to map with keys in sync with those in metadata.ReplicationSettings
func InternalSettingsToMap(settings *InternalSettings) map[string]interface{} {
	settings_map := make (map[string]interface{})
	checkpointInternal := settings.GetXdcrCheckpointInterval()
	if checkpointInternal != 0 {
		settings_map[metadata.CheckpointInterval] = checkpointInternal
	}
	batchCount := settings.GetXdcrWorkerBatchSize()
	if batchCount != 0 {
		settings_map[metadata.BatchCount] = batchCount
	}
	batchSize := settings.GetXdcrDocBatchSizeKb()
	if batchSize != 0 {
		settings_map[metadata.BatchSize] = batchSize
	}
	failureRestartInterval := settings.GetXdcrFailureRestartInterval()
	if failureRestartInterval != 0 {
		settings_map[metadata.FailureRestartInterval] = failureRestartInterval
	}
	optimisticReplicationThreshold := settings.GetXdcrOptimisticReplicationThreshold()
	if optimisticReplicationThreshold != 0 {
		settings_map[metadata.OptimisticReplicationThreshold] = optimisticReplicationThreshold
	}
	httpConnection := settings.GetHttpConnections()
	if httpConnection != 0 {
		settings_map[metadata.HttpConnection] = httpConnection
	}
	sourceNozzlePerNode := settings.GetXdcrSourceNozzlePerNode()
	if sourceNozzlePerNode != 0 {
		settings_map[metadata.SourceNozzlePerNode] = sourceNozzlePerNode
	}	
	targetNozzlePerNode := settings.GetXdcrTargetNozzlePerNode()
	if targetNozzlePerNode != 0 {
		settings_map[metadata.TargetNozzlePerNode] = targetNozzlePerNode
	}	
	maxExpectedReplicationLag := settings.GetXdcrMaxExpectedReplicationLag()
	if maxExpectedReplicationLag != 0 {
		settings_map[metadata.MaxExpectedReplicationLag] = maxExpectedReplicationLag
	}	
	timeoutPercentageCap := settings.GetXdcrTimeoutPercentageCap()
	if timeoutPercentageCap != 0 {
		settings_map[metadata.TimeoutPercentageCap] = timeoutPercentageCap
	}	
	
	return settings_map
}

// convert ReplicationSettings to map with keys in sync with those in metadata.ReplicationSettings
func ReplicationSettingsToMap(settings *ReplicationSettings) map[string]interface{} {
	settings_map := make (map[string]interface{})
	replicationType := settings.GetXdcrReplicationType()
	settings_map[metadata.ReplicationType] = replicationType
	filterExpression := settings.GetXdcrFilterExpression()
	if len(filterExpression) > 0 {
		settings_map[metadata.FilterExpression] = filterExpression
	}
	active := settings.GetActive()
	settings_map[metadata.Active] = active
	
	internalSettings_map := InternalSettingsToMap(settings.GetInternalSettings())
	for key, value := range internalSettings_map {
		settings_map[key] = value
	}
	
	return settings_map
}

// create a new DeleteReplication request for specified replicationId
func NewDeleteReplicationRequest(replicationId string) *DeleteReplicationRequest {
	forward := false
	return &DeleteReplicationRequest{
		Id: &replicationId,
		Forward: &forward,
	}
}

// create a new CreateReplication response
func NewCreateReplicationResponse(replicationId string) *CreateReplicationResponse {
	return &CreateReplicationResponse{
		Id: &replicationId,
	}
}

// EmptyMessage implement MessageMarshaller interface
func (res *EmptyMessage) Name() string {
	return "EmptyMessage"
}

func (res *EmptyMessage) ContentType() string {
	return "application/protobuf"
}

func (res *EmptyMessage) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *EmptyMessage) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, res)
}

// CreateReplicationRequest implement MessageMarshaller interface
func (req *CreateReplicationRequest) Name() string {
	return CreateReplicationPath + PostSuffix
}

func (req *CreateReplicationRequest) ContentType() string {
	return "application/protobuf"
}

func (req *CreateReplicationRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *CreateReplicationRequest) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, req)
}

// CreateReplicationResponse implement MessageMarshaller interface
func (res *CreateReplicationResponse) Name() string {
	return "CreateReplicationResponse"
}

func (res *CreateReplicationResponse) ContentType() string {
	return "application/protobuf"
}

func (res *CreateReplicationResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *CreateReplicationResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// DeleteReplicationRequest implement MessageMarshaller interface
func (req *DeleteReplicationRequest) Name() string {
	return DeleteReplicationPrefix + DynamicSuffix + PostSuffix
}

func (req *DeleteReplicationRequest) ContentType() string {
	return "application/protobuf"
}

func (req *DeleteReplicationRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *DeleteReplicationRequest) Decode(data []byte) (err error) {
	request, err := utils.DecodeHttpRequestFromByteArray(data)
	if err != nil {
		return err
	}
	
	err = utils.DecodeMessageFromHttpRequest(request, req)
	if err != nil {
		return err
	}
	
	// extract replication id from request and add it to message
	replicationId, err := utils.DecodeReplicationIdFromHttpRequest(request, DeleteReplicationPrefix)
	if err == nil {
		req.Id = &replicationId
	}
	return err
}

// ViewInternalSettingsRequest implement MessageMarshaller interface
func (req *ViewInternalSettingsRequest) Name() string {
	return InternalSettingsPath + GetSuffix
}

func (req *ViewInternalSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ViewInternalSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ViewInternalSettingsRequest) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, req)
}

// ViewInternalSettingsResponse implement MessageMarshaller interface
func (res *ViewInternalSettingsResponse) Name() string {
	return "ViewInternalSettingsResponse"
}

func (res *ViewInternalSettingsResponse) ContentType() string {
	return "application/protobuf"
}

func (res *ViewInternalSettingsResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ViewInternalSettingsResponse) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, res)
}

// InternalSettings implement MessageMarshaller interface
func (res *InternalSettings) Name() string {
	return "InternalSettings"
}

func (res *InternalSettings) ContentType() string {
	return "application/protobuf"
}

func (res *InternalSettings) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *InternalSettings) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// ChangeGlobalSettingsRequest implement MessageMarshaller interface
func (req *ChangeGlobalSettingsRequest) Name() string {
	return SettingsReplicationsPath + PostSuffix
}

func (req *ChangeGlobalSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ChangeGlobalSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ChangeGlobalSettingsRequest) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, req)
}

// ChangeReplicationSettingsRequest implement MessageMarshaller interface
func (req *ChangeReplicationSettingsRequest) Name() string {
	return SettingsReplicationsPath + DynamicSuffix + PostSuffix
}

func (req *ChangeReplicationSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ChangeReplicationSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ChangeReplicationSettingsRequest) Decode(data []byte) (err error) {
	request, err := utils.DecodeHttpRequestFromByteArray(data)
	if err != nil {
		return err
	}
	
	err = utils.DecodeMessageFromHttpRequest(request, req)
	if err != nil {
		return err
	}
	
	// extract replication id from request and add it to message
	replicationId, err := utils.DecodeReplicationIdFromHttpRequest(request, SettingsReplicationsPath)
	if err == nil {
		req.Id = &replicationId
	}
	return err
}

// ChangeInternalSettingsRequest implement MessageMarshaller interface
func (req *ChangeInternalSettingsRequest) Name() string {
	return InternalSettingsPath + PostSuffix
}

func (req *ChangeInternalSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ChangeInternalSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ChangeInternalSettingsRequest) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, req)
}


// GetStatisticsRequest implement MessageMarshaller interface
func (req *GetStatisticsRequest) Name() string {
	return StatisticsPrefix + GetSuffix
}

func (req *GetStatisticsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *GetStatisticsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *GetStatisticsRequest) Decode(data []byte) (err error) {
	request, err := utils.DecodeHttpRequestFromByteArray(data)
	if err != nil {
		return err
	}
	
	err = utils.DecodeMessageFromHttpRequest(request, req)
	if err != nil {
		return err
	}
	
	// extract extra parameters from request and add it to message
	loc := statsPathRegexp.FindStringIndex(request.URL.Path)
	if loc == nil {
		return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", request.URL.Path))
	}
	// get encoded parameters from request url
	encodedParams := request.URL.Path[loc[1]:]
	// decode params into a url, which is essentially a list of "/" separated values, 
	// i.e., /[UUID]/[source_bucket]/[destination_bucket]/[stat_name]
	// or /[UUID]/[source_bucket]/[destination_bucket]/[filter_name]/[stat_name]
	paramsStr, err := url.Parse(encodedParams)
	if err != nil {
		return err
	}
	
	paramsArr := strings.Split(paramsStr.String(), UrlDelimiter)
	numOfParams := len(paramsArr)
	if numOfParams != 4 && numOfParams != 5 {
		return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", request.URL.Path))
	}
	
	// first three elements in array are UUID, source_bucket, destination_bucket, respectively
	req.Uuid = &(paramsArr[0])
	req.FromBucket = &(paramsArr[1])
	req.ToBucket = &(paramsArr[2])
	
	// if filter name is specified, set it
	if numOfParams == 5 {
		req.FilterName = &(paramsArr[3])
	}
	
	// last element in array is stat name. 
	statName := paramsArr[numOfParams - 1]
	if stats, ok := GetStatisticsRequest_Stats_value[statName]; !ok {
		return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", request.URL.Path))
	} else {
		statsObj := GetStatisticsRequest_Stats(stats)
		req.Stats = &statsObj
	}
	
	return err
}
