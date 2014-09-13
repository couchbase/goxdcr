// replication manager.

package replicationmanager

import (
	"strings"
	"strconv"
	ap "github.com/couchbase/indexing/secondary/adminport"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	"github.com/ysui6888/indexing/secondary/common"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	couchbase "github.com/couchbaselabs/go-couchbase"
)

type ReplicationManager struct {
	adminport string                       // <host:port> for replication manager's admin-port
	logPrefix string
}

func (rm *ReplicationManager) requestReplication(request *protobuf.ReplicationRequest) error {
	return nil
}

func (rm *ReplicationManager) forwardReplicationRequest(request *protobuf.ReplicationRequest) error {
	if !request.GetForward() {
	    // do nothing if "forward" flag in request is false
	    // this would happen if the request itself is a forwarded request
		return nil;
	}
	
	// turn off "forward" flag to prevent the forwarded request from being forwarded again
	off := false
	request.Forward = &off
	
	curKVAddr, bucket, _, err := getSourceTopology(request.GetSourceCluster(), request.GetSourceBucket())
	if err != nil {
		return err
	}
	
	for _, kvaddr := range bucket.VBServerMap().ServerList {
		if kvaddr != curKVAddr {
			rm.forwardReplicationRequestToKV(request, kvaddr)
			// TODO what if forward fails. Do we cancel previously forwarded requests? 
		}
	}
	
	return nil
}

func (rm *ReplicationManager) forwardReplicationRequestToKV(request *protobuf.ReplicationRequest, kvaddr string) error {
	kvName := getKVNameFromKVAddr(kvaddr)
	baseURL, err := couchbase.ParseURL("http://" + kvName + ":" + strconv.Itoa(base.AdminportNumber))
	if err != nil {
		return err
	}
	path := request.String()
	
	var output ap.MessageMarshaller
	err = utils.QueryRestAPI(baseURL, path, "", "", "POST", output)
	return err
}

// extract kv node name from kvaddr, which is in the form of kvName:port
func getKVNameFromKVAddr(kvaddr string) string {
	kvName := kvaddr
	if index := strings.Index(kvaddr, ":"); index >= 0 {
		kvName = kvaddr[0:index]
	}
	return kvName
}

// implementation of call back funcs required by xdcrFactory
func getXDCRConfig(topic string) (*base.XDCRConfig, error) {
	// TODO get from metadata store
	return nil, nil
}

func getSourceTopology(sourceCluster, sourceBucketn string) (string, *couchbase.Bucket, []uint16, error) {
	bucket, err := common.ConnectBucket(sourceCluster, base.DefaultPoolName, sourceBucketn)
	if err != nil {
		return "", nil, nil, err
	}
	
	// TODO ns_server needs to expose API to get the kvaddr and vb list of the current kv node
	// one option is for ns_server to expose getActiveVbs() and we infer kvaddr from Vbs
	kvaddr := bucket.VBServerMap().ServerList[0]
	
	m, err := bucket.GetVBmap([]string{kvaddr})
	if err != nil {
		return "", nil, nil, err
	}
	
	vbList := m[kvaddr]
	
	return kvaddr, bucket, vbList, nil
}

func getTargetTopology(targetCluster, targetBucketn string) (map[string][]uint16, error) {
	bucket, err := common.ConnectBucket(targetCluster, base.DefaultPoolName, targetBucketn)
	if err != nil {
		return nil, err
	}
	
	return bucket.GetVBmap(bucket.VBServerMap().ServerList)
}
