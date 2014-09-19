package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
)

const(
	CREATE_REPLICATION_PATH = "controller/createReplication"
	INTERNAL_SETTINGS_PATH = "internalSettings"
	SETTINGS_REPLICATIONS_PATH = "settings/replications"
	DELETE_REPLICATION_PREFIX = "controller/cancelXDCR"
	STATISTICS_PREFIX = "pools/default/buckets"
	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path. 
	// e.g., settings/replications/dynamic
	DYNAMIC_SUFFIX = "/dynamic"
	// The same path, e.g.,SETTINGS_REPLICATION_PATH, may be used for two different APIs: look up and modify. 
	// The following suffixes are used to distinguish between these two cases
	GET_SUFFIX = "/get"
	POST_SUFFIX = "/post"
)

// CreateReplicationRequest implement MessageMarshaller interface
func (res *CreateReplicationRequest) Name() string {
	return CREATE_REPLICATION_PATH + POST_SUFFIX
}

func (res *CreateReplicationRequest) ContentType() string {
	return "application/protobuf"
}

func (res *CreateReplicationRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *CreateReplicationRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
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
func (res *DeleteReplicationRequest) Name() string {
	return DELETE_REPLICATION_PREFIX + DYNAMIC_SUFFIX + POST_SUFFIX
}

func (res *DeleteReplicationRequest) ContentType() string {
	return "application/protobuf"
}

func (res *DeleteReplicationRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *DeleteReplicationRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
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
	return proto.Unmarshal(data, req)
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
func (res *ChangeGlobalSettingsRequest) Name() string {
	return SETTINGS_REPLICATIONS_PATH + POST_SUFFIX
}

func (res *ChangeGlobalSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (res *ChangeGlobalSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ChangeGlobalSettingsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// ChangeReplicationSettingsRequest implement MessageMarshaller interface
func (res *ChangeReplicationSettingsRequest) Name() string {
	return SETTINGS_REPLICATIONS_PATH + DYNAMIC_SUFFIX + POST_SUFFIX
}

func (res *ChangeReplicationSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (res *ChangeReplicationSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ChangeReplicationSettingsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// ChangeInternalSettingsRequest implement MessageMarshaller interface
func (res *ChangeInternalSettingsRequest) Name() string {
	return INTERNAL_SETTINGS_PATH + POST_SUFFIX
}

func (res *ChangeInternalSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (res *ChangeInternalSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ChangeInternalSettingsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}


// GetStatisticsRequest implement MessageMarshaller interface
func (res *GetStatisticsRequest) Name() string {
	return STATISTICS_PREFIX + GET_SUFFIX
}

func (res *GetStatisticsRequest) ContentType() string {
	return "application/protobuf"
}

func (res *GetStatisticsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *GetStatisticsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}