package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
)

// GetRemoteClustersRequest implement MessageMarshaller interface
func (req *GetRemoteClustersRequest) Name() string {
	return "GetRemoteClustersRequest"
}

func (req *GetRemoteClustersRequest) ContentType() string {
	return "application/protobuf"
}

func (req *GetRemoteClustersRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *GetRemoteClustersRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// GetRemoteClustersResponse implement MessageMarshaller interface
func (res *GetRemoteClustersResponse) Name() string {
	return "GetRemoteClustersResponse"
}

func (res *GetRemoteClustersResponse) ContentType() string {
	return "application/protobuf"
}

func (res *GetRemoteClustersResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *GetRemoteClustersResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// CreateRemoteClusterRequest implement MessageMarshaller interface
func (res *CreateRemoteClusterRequest) Name() string {
	return "CreateRemoteClusterRequest"
}

func (res *CreateRemoteClusterRequest) ContentType() string {
	return "application/protobuf"
}

func (res *CreateRemoteClusterRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *CreateRemoteClusterRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// RemoteClusterResponse implement MessageMarshaller interface
func (res *RemoteClusterResponse) Name() string {
	return "RemoteClusterResponse"
}

func (res *RemoteClusterResponse) ContentType() string {
	return "application/protobuf"
}

func (res *RemoteClusterResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *RemoteClusterResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// DeleteRemoteClusterRequest implement MessageMarshaller interface
func (res *DeleteRemoteClusterRequest) Name() string {
	return "DeleteRemoteClusterRequest"
}

func (res *DeleteRemoteClusterRequest) ContentType() string {
	return "application/protobuf"
}

func (res *DeleteRemoteClusterRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *DeleteRemoteClusterRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// CreateReplicationRequest implement MessageMarshaller interface
func (res *CreateReplicationRequest) Name() string {
	return "CreateReplicationRequest"
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
	return "DeleteReplicationRequest"
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
	return "ViewSettingsRequest"
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

// ViewSettingsResponse implement MessageMarshaller interface
func (res *ViewSettingsResponse) Name() string {
	return "ViewSettingsResponse"
}

func (res *ViewSettingsResponse) ContentType() string {
	return "application/protobuf"
}

func (res *ViewSettingsResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ViewSettingsResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// ChangeSettingsRequest implement MessageMarshaller interface
func (res *ChangeSettingsRequest) Name() string {
	return "ChangeSettingsRequest"
}

func (res *ChangeSettingsRequest) ContentType() string {
	return "application/protobuf"
}

func (res *ChangeSettingsRequest) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ChangeSettingsRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}

// GetStatisticsRequest implement MessageMarshaller interface
func (res *GetStatisticsRequest) Name() string {
	return "GetStatisticsRequest"
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