// metadata service implementation leveraging gometa
package services

import (
	"encoding/json"
	"net/rpc"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	"github.com/couchbase/gometa/server"
	"github.com/couchbase/gometa/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
)

var goMetadataServiceMethod = "RequestReceiver.NewRequest"

type MetadataSvc struct {
	hostAddr    string // host addr
	client      *rpc.Client  // client for gometa service
	logger      *log.CommonLogger
}

func NewMetadataSvc(hostAddr string, logger_ctx *log.LoggerContext,) (*MetadataSvc, error) {
	meta_svc := &MetadataSvc{
					hostAddr:  hostAddr,  
					logger:    log.NewLogger("MetadataService", logger_ctx),
					}
		
	client, err := rpc.DialHTTP("tcp", meta_svc.hostAddr)
	if err == nil {
		meta_svc.client = client
		meta_svc.logger.Infof("Metdata service started with host=%v\n", meta_svc.hostAddr)
		return meta_svc, nil
	} else {
		return nil, err
	}
}

func (meta_svc *MetadataSvc) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	opCode := common.GetOpCodeStr(common.OPCODE_GET)
	result, err := meta_svc.sendRequest(opCode, replicationId, nil)
	if err != nil {
		return nil, err
	}
	var spec = &metadata.ReplicationSpecification{}
	err = json.Unmarshal(result, spec) 
	return spec, err
}

func (meta_svc *MetadataSvc) AddReplicationSpec(spec metadata.ReplicationSpecification) error {
	opCode := common.GetOpCodeStr(common.OPCODE_ADD)
	key := spec.Id
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = meta_svc.sendRequest(opCode, key, value)
	return err
}

func (meta_svc *MetadataSvc) SetReplicationSpec(spec metadata.ReplicationSpecification) error {
	opCode := common.GetOpCodeStr(common.OPCODE_SET)
	key := spec.Id
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = meta_svc.sendRequest(opCode, key, value)
	return err
}

func (meta_svc *MetadataSvc) DelReplicationSpec(replicationId string) error {
	opCode := common.GetOpCodeStr(common.OPCODE_DELETE)
	_, err := meta_svc.sendRequest(opCode, replicationId, nil)
	return err
}

func (meta_svc *MetadataSvc) sendRequest(opCode, key string, value []byte) ([]byte, error) {
	request := &server.Request{OpCode: opCode, Key: key, Value: value}
	var reply *server.Reply
	err := meta_svc.client.Call(goMetadataServiceMethod, request, &reply)
	if reply == nil {
		return nil, err
	} else {
		return reply.Result, err
	}
}
