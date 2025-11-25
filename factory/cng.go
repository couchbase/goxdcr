package factory

import (
	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts/cng"
)

// constructCNGNozzle constructs a CNG nozzle for the given parameters
func (x *XDCRFactory) constructCNGNozzle(topic string, // Replication topic
	spec *metadata.ReplicationSpecification, // Replication specification
	sourceCRMode base.ConflictResolutionMode, // Source conflict resolution mode
	targetBucketInfo map[string]any, // Target bucket info
	targetClusterRef *metadata.RemoteClusterReference, // Target cluster reference
	sourceClusterUUID string) ( // Source cluster UUID
	outNozzles map[string]common.Nozzle, // Nozzle ID to Nozzle map
	vbNozzleMap map[uint16]string, // VBucket to Nozzle ID map
	kvVBMap map[string][]uint16, // KV Server to VBucket list map
	targetUserName string, // Target cluster username
	targetPassword string, // Target cluster password
	err error) {

	outNozzles = make(map[string]common.Nozzle)
	vbNozzleMap = make(map[uint16]string)

	targetUserName = targetClusterRef.UserName()
	targetPassword = targetClusterRef.Password()
	x.logger.Infof("%v username for target bucket access=%v%v%v\n", spec.Id, base.UdTagBegin, targetUserName, base.UdTagEnd)

	var rsp map[uint16]*internal_xdcr_v1.GetVbucketInfoResponse
	rsp, err = getVbucketInfoFromCng(x.logger, x.utils, targetClusterRef, spec.TargetBucketName, targetBucketInfo)
	if err != nil {
		x.logger.Errorf("Error getting vbucket info from CNG, err=%v\n", err)
		return nil, nil, nil, "", "", err
	}

	// For CNG, there is only one nozzle per target cluster
	id := x.partId(CNG_NOZZLE_NAME_PREFIX, topic, targetClusterRef.HostName(), 0)
	cfg := cng.Config{
		Replication: cng.ReplicationConfig{
			CRMode:            sourceCRMode,
			SourceBucketName:  spec.SourceBucketName,
			SourceClusterUUID: sourceClusterUUID,
			SourceBucketUUID:  spec.SourceBucketUUID,
			TargetClusterUUID: spec.TargetClusterUUID,
			TargetBucketName:  spec.TargetBucketName,
			TargetBucketUUID:  spec.TargetBucketUUID,
		},
		Services: cng.Services{
			RemoteClusterSvc: x.remote_cluster_svc,
			Utils:            x.utils,
		},
	}
	err = cfg.Replication.SetVBUUIDMap(rsp)
	if err != nil {
		x.logger.Errorf("Error setting VBUUID map, err=%v\n", err)
		return nil, nil, nil, "", "", err
	}

	var nozzle *cng.Nozzle
	nozzle, err = cng.New(id, x.logger.LoggerContext(), cfg)
	if err != nil {
		x.logger.Errorf("Error constructing CNG nozzle, err=%v\n", err)
		return nil, nil, nil, "", "", err
	}

	kvVBMap, err = x.utils.GetRemoteServerVBucketsMap(targetClusterRef.HostName(), spec.TargetBucketName, targetBucketInfo, false)
	if err != nil {
		x.logger.Errorf("Error getting server vbuckets map, err=%v\n", err)
		return nil, nil, nil, "", "", err
	}
	if len(kvVBMap) == 0 {
		err = base.ErrorNoTargetNozzle
		return nil, nil, nil, "", "", err
	}

	numVBs := 0
	for _, vbList := range kvVBMap {
		numVBs += len(vbList)
	}

	x.logger.Infof("constructed CNG nozzle %v for target cluster %v numVBs=%d\n", id, targetClusterRef.HostName(), numVBs)
	outNozzles[nozzle.Id()] = nozzle
	for i := uint16(0); i < uint16(numVBs); i++ {
		vbNozzleMap[i] = nozzle.Id()
	}

	return
}
