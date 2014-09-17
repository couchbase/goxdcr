// replication manager.

package replicationmanager

import (
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	"github.com/ysui6888/indexing/secondary/common"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	couchbase "github.com/couchbaselabs/go-couchbase"
)

// call back function for getting xdcr deployment config
type Get_XDCR_Deploy_Config_Callback_Func func() (*base.XDCRDeployConfig, error)

type ReplicationManager struct {
	adminport string                       // <host:port> for replication manager's admin-port
	xdcr_deploy_config_callback *Get_XDCR_Deploy_Config_Callback_Func
	logPrefix string
}

func (rm *ReplicationManager) StartReplication(request *protobuf.ReplicationRequest) error {
	return nil
}

func (rm *ReplicationManager) StopReplication(request *protobuf.ReplicationRequest) error {
	return nil
}

func (rm *ReplicationManager) RestartReplication(request *protobuf.ReplicationRequest) error {
	return nil
}

func (rm *ReplicationManager) GetXDCRDeployConfig() (*base.XDCRDeployConfig, error) {
	return (*rm.xdcr_deploy_config_callback)()
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
