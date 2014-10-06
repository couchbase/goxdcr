// metadata service implementation leveraging gometa
package services

import (
	"encoding/json"
	"net/rpc"
	"os"
	"os/exec"
	"fmt"
	"time"
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

func DefaultMetadataSvc() (*MetadataSvc, error) {
	meta_svc := &MetadataSvc{
					hostAddr:  "localhost:5003",  
					logger:    log.NewLogger("MetadataService", nil),
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

func NewMetadataSvc(hostAddr string, logger_ctx *log.LoggerContext) (*MetadataSvc, error) {
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

// utility methods for starting and killing the gometa service which the metadata service depends on. 
// mostly for testing

// start the gometa service
func StartGometaService() (*exec.Cmd, error) {
	goPath := os.Getenv("GOPATH")
	
	objPath := goPath + "/bin/gometa"
	srcPath := goPath + "/src/github.com/couchbase/gometa/cmd/gometa/*.go"
	err := exec.Command("/bin/bash", "-c", "go build -o " + objPath + " " + srcPath).Run()
	if err != nil {
		fmt.Printf("Error executing command line - %v\n", exec.Command("/bin/bash", "-c", "go build -o " + objPath + " " + srcPath).Args)
		return nil, err
	}

	// run gometa executable to start server
	cmd := exec.Command(objPath, "-config", goPath + "/src/github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/services/metadata_svc_config")
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	
	//wait for gometa service to finish starting
	time.Sleep(time.Second * 3)
	
	fmt.Println("started gometa service.")
	return cmd, nil
}

// kill the gometa service
func KillGometaService(cmd *exec.Cmd) {
	if err := cmd.Process.Kill(); err != nil {
		fmt.Println("failed to kill gometa service. Please kill it manually")
	} else {
		fmt.Println("killed gometa service successfully")
	}
}
