package protobuf

import (
	"code.google.com/p/goprotobuf/proto"
)

// NewReplicationResponse creates a new instance of ReplicationResponse
// that can be sent back to the client.
func NewReplicationResponse(req interface{}, err error) *ReplicationResponse {
	var m *ReplicationResponse
	var errMsg  = err.Error()
	switch val := req.(type) {
	case *ReplicationRequest:
		m = &ReplicationResponse{
			SourceCluster:      proto.String(val.GetSourceCluster()),
			SourceBucket:      proto.String(val.GetSourceBucket()),
			TargetCluster:      proto.String(val.GetTargetCluster()),
			TargetBucket:      proto.String(val.GetTargetBucket()),
			OpCode:       proto.Uint32(val.GetOpCode()),
		}
		if err != nil {
			m.Err = &Error{Error: &errMsg,}
		}
	default:
	}
	return m
}

// ReplicationRequest implement MessageMarshaller interface
func (req *ReplicationRequest) Name() string {
	return "ReplicationRequest"
}

func (req *ReplicationRequest) ContentType() string {
	return "application/protobuf"
}

func (req *ReplicationRequest) Encode() (data []byte, err error) {
	return proto.Marshal(req)
}

func (req *ReplicationRequest) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, req)
}

// ReplicationResponse implement MessageMarshaller interface
func (res *ReplicationResponse) Name() string {
	return "ReplicationResponse"
}

func (res *ReplicationResponse) ContentType() string {
	return "application/protobuf"
}

func (res *ReplicationResponse) Encode() (data []byte, err error) {
	return proto.Marshal(res)
}

func (res *ReplicationResponse) Decode(data []byte) (err error) {
	return proto.Unmarshal(data, res)
}