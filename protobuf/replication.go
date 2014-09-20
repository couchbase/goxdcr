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
)

const(
	URL_DELIMITER = "/"
	
	CREATE_REPLICATION_PATH = "controller/createReplication"
	INTERNAL_SETTINGS_PATH = "internalSettings"
	SETTINGS_REPLICATIONS_PATH = "settings/replications/"
	DELETE_REPLICATION_PREFIX = "controller/cancelXDCR/"
	STATISTICS_PREFIX = "pools/default/buckets"
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path. 
	// e.g., settings/replications/dynamic
	DYNAMIC_SUFFIX = URL_DELIMITER + "dynamic"
	// The same path, e.g.,SETTINGS_REPLICATION_PATH, may be used for two different APIs: look up and modify. 
	// The following suffixes are used to distinguish between these two cases
	GET_SUFFIX = URL_DELIMITER + "get"
	POST_SUFFIX = URL_DELIMITER + "post"
	
	STATS_PATH_PATTERN = ".*pools/default/buckets/[^/]*/stats/replications"
	
)

var statsPathRegexp, _ = regexp.Compile(STATS_PATH_PATTERN)

var logger_repmsg *log.CommonLogger = log.NewLogger("ReplicationMessages", log.LogLevelInfo)

// CreateReplicationRequest implement MessageMarshaller interface
func (req *CreateReplicationRequest) Name() string {
	return CREATE_REPLICATION_PATH + POST_SUFFIX
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
	return DELETE_REPLICATION_PREFIX + DYNAMIC_SUFFIX + POST_SUFFIX
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
	replicationId, err := utils.DecodeReplicationIdFromHttpRequest(request, DELETE_REPLICATION_PREFIX)
	if err == nil {
		req.Id = &replicationId
	}
	return err
}

// ViewSettingsRequest implement MessageMarshaller interface
func (req *ViewSettingsRequest) Name() string {
	return INTERNAL_SETTINGS_PATH + GET_SUFFIX
}

func (req *ViewSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ViewSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ViewSettingsRequest) Decode(data []byte) (err error) {
	return utils.DecodeMessageFromByteArray(data, req)
}

// Settings implement MessageMarshaller interface
func (res *Settings) Name() string {
	return "Settings"
}

func (res *Settings) ContentType() string {
	return "application/protobuf"
}

func (res *Settings) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *Settings) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// ChangeGlobalSettingsRequest implement MessageMarshaller interface
func (req *ChangeGlobalSettingsRequest) Name() string {
	return SETTINGS_REPLICATIONS_PATH + POST_SUFFIX
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
	return SETTINGS_REPLICATIONS_PATH + DYNAMIC_SUFFIX + POST_SUFFIX
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
	replicationId, err := utils.DecodeReplicationIdFromHttpRequest(request, SETTINGS_REPLICATIONS_PATH)
	if err == nil {
		req.Id = &replicationId
	}
	return err
}

// ChangeInternalSettingsRequest implement MessageMarshaller interface
func (req *ChangeInternalSettingsRequest) Name() string {
	return INTERNAL_SETTINGS_PATH + POST_SUFFIX
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
	return STATISTICS_PREFIX + GET_SUFFIX
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
	
	paramsArr := strings.Split(paramsStr.String(), URL_DELIMITER)
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
