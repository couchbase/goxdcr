// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/capi_utils"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"net/url"
)

var NoSupportForXDCRCheckpointingError = errors.New("No xdcrcheckpointing support on older node")

type RemoteBucketInfo struct {
	RemoteClusterRefName     string
	BucketName               string
	RemoteClusterRef         *metadata.RemoteClusterReference
	Capabilities             []string
	UUID                     string
	VBServerMap              map[string][]uint16
	MemcachedAddrRestAddrMap map[string]string
	RestAddrHttpClientMap    map[string]*http.Client
	logger                   *log.CommonLogger
}

func NewRemoteBucketInfo(remoteClusterRefName string, bucketName string, remote_cluster_ref *metadata.RemoteClusterReference,
	remote_cluster_svc RemoteClusterSvc, cluster_info_svc ClusterInfoSvc, logger *log.CommonLogger) (*RemoteBucketInfo, error) {
	if remoteClusterRefName == "" || bucketName == "" {
		return nil, errors.New("remoteClusterRefName and bucketName are required")
	}

	remoteBucket := &RemoteBucketInfo{RemoteClusterRefName: remoteClusterRefName,
		BucketName:       bucketName,
		RemoteClusterRef: remote_cluster_ref,
		logger:           logger}

	err := remoteBucket.refresh_internal(remote_cluster_svc, false)
	return remoteBucket, err
}

func (remoteBucket *RemoteBucketInfo) Refresh(remote_cluster_svc RemoteClusterSvc) error {
	return remoteBucket.refresh_internal(remote_cluster_svc, true)
}

func (remoteBucket *RemoteBucketInfo) refresh_internal(remote_cluster_svc RemoteClusterSvc, full bool) error {
	if remoteBucket.RemoteClusterRef == nil && !full {
		remoteClusterRef, err := remote_cluster_svc.RemoteClusterByRefName(remoteBucket.RemoteClusterRefName, true)
		if err != nil {
			remoteBucket.logger.Errorf("Failed to get remote cluster reference with refName=%v, err=%v\n", remoteBucket.RemoteClusterRefName, err)
			return err
		}

		remoteBucket.RemoteClusterRef = remoteClusterRef
	}

	username, password, certificate, sanInCertificate, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return err
	}
	connStr, err := remoteBucket.RemoteClusterRef.MyConnectionStr()
	if err != nil {
		return err
	}

	targetBucketInfo, err := utils.GetBucketInfo(connStr, remoteBucket.BucketName, username, password, certificate, sanInCertificate, remoteBucket.logger)
	if err != nil {
		return err
	}

	uuidObj, ok := targetBucketInfo[base.BucketUUIDKey]
	if !ok {
		return fmt.Errorf("Failed to get uuid for remote bucket %v", remoteBucket.BucketName)
	}

	remoteBucket.UUID, ok = uuidObj.(string)
	if !ok {
		return fmt.Errorf("uuid for remote bucket %v is of wrong type", remoteBucket.BucketName)
	}

	capabilitiesObj, ok := targetBucketInfo[base.BucketCapabilitiesKey]
	if !ok {
		return fmt.Errorf("Failed to get bucket capabilities for remote bucket %v", remoteBucket.BucketName)
	}

	capabilitiesArr, ok := capabilitiesObj.([]interface{})
	if !ok {
		return fmt.Errorf("bucket capabilities for remote bucket %v is of wrong type", remoteBucket.BucketName)
	}

	remoteBucket.Capabilities = make([]string, 0)
	for _, capabilityObj := range capabilitiesArr {
		capability, ok := capabilityObj.(string)
		if !ok {
			return fmt.Errorf("bucket capability for remote bucket %v is of wrong type", remoteBucket.BucketName)
		}
		remoteBucket.Capabilities = append(remoteBucket.Capabilities, capability)
	}

	remoteBucket.VBServerMap, err = utils.GetServerVBucketsMap(connStr, remoteBucket.BucketName, targetBucketInfo)
	if err != nil {
		return fmt.Errorf("Failed to get VBServerMap for remote bucket %v", remoteBucket.BucketName)
	}

	remoteBucket.MemcachedAddrRestAddrMap = make(map[string]string)
	remoteBucket.RestAddrHttpClientMap = make(map[string]*http.Client)

	urlmap, err := capi_utils.ConstructServerCouchApiBaseMap(remoteBucket.BucketName, targetBucketInfo, remoteBucket.RemoteClusterRef)
	if err != nil {
		return err
	}

	//the url it returns http://127.0.0.1:9500/default%2B77aceaa5b49efbd92a261b8a1e72dab5
	//we only need the host part
	for serverAddr, urlstr := range urlmap {
		u, err := url.Parse(urlstr)
		if err != nil {
			return err
		}
		remoteBucket.MemcachedAddrRestAddrMap[serverAddr] = u.Host
		http_client, err := utils.GetHttpClient(certificate, sanInCertificate, u.Host, remoteBucket.logger)
		if err != nil {
			return err
		}
		remoteBucket.RestAddrHttpClientMap[u.Host] = http_client
	}
	remoteBucket.logger.Infof("remoteBucket.MemcachedAddrRestAddrMap=%v\n", remoteBucket.MemcachedAddrRestAddrMap)

	return nil
}

func (remoteBucket *RemoteBucketInfo) String() string {
	return fmt.Sprintf("%v - %v", remoteBucket.RemoteClusterRefName, remoteBucket.BucketName)
}

type RemoteVBReplicationStatus struct {
	VBOpaque metadata.TargetVBOpaque
	VBSeqno  uint64
	VBNo     uint16
}

func (rep_status *RemoteVBReplicationStatus) IsEmpty() bool {
	return rep_status.VBOpaque == nil
}

func NewEmptyRemoteVBReplicationStatus(vbno uint16) *RemoteVBReplicationStatus {
	return &RemoteVBReplicationStatus{VBNo: vbno}
}

//abstract capi apis needed for xdcr
type CAPIService interface {
	//call at the beginning of the replication to determin the startpoint
	//PrePrelicate (_pre_replicate)
	//Parameters: remoteBucket - the information about the remote bucket
	//			  knownRemoteVBStatus - the current replication status of a vbucket
	//			  xdcrCheckpointingCapbility
	//returns:
	//		  bMatch - true if the remote vbucket matches the current replication status
	//		  current_remoteVBUUID - new remote vb uuid might be retured if bMatch = false and there was a topology change on remote vb
	//		  err
	PreReplicate(remoteBucket *RemoteBucketInfo, knownRemoteVBStatus *RemoteVBReplicationStatus, xdcrCheckpointingCapbility bool) (bVBMatch bool, current_remoteVBOpaque metadata.TargetVBOpaque, err error)
	//call to do disk commit on the remote cluster, which ensure that the mutations replicated are durable
	//CommitForCheckpoint (_commit_for_checkpoint)
	//Parameters: remoteBucket - the information about the remote bucket
	//			  remoteVBUUID - the remote vb uuid on file
	//			  vbno		   - the vb number
	//returns:	  remote_seqno - the remote vbucket's high sequence number
	//			  vb_uuid	   - the new vb uuid if there was a topology change
	//			  err
	CommitForCheckpoint(remoteBucket *RemoteBucketInfo, remoteVBOpaque metadata.TargetVBOpaque, vbno uint16) (remote_seqno uint64, vbOpaque metadata.TargetVBOpaque, err error)
	//call to mass validate vb uuids on remote cluster
	//Parameters: remoteBucket - the information about the remote bucket
	//			  remoteVBUUIDs - the map of vbno and vbuuid
	//returns: matching - the list of vb numbers whose vbuuid matches
	//		   mismatching - the list of vb numbers whose vbuuid mismatches
	//		   missing	- the list of vb numbers whose vbuuid is not kept on file
	MassValidateVBUUIDs(remoteBucket *RemoteBucketInfo, remoteVBUUIDs map[uint16]metadata.TargetVBOpaque) (matching []uint16, mismatching []uint16, missing []uint16, err error)
}
