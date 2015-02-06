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
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
)

type RemoteBucketInfo struct {
	RemoteClusterRefName string
	BucketName           string

	RemoteClusterRef         *metadata.RemoteClusterReference
	Capabilities             []string
	UUID                     string
	VBServerMap              map[string][]uint16
	MemcachedAddrRestAddrMap map[string]string
	logger                   *log.CommonLogger
}

func NewRemoteBucketInfo(remoteClusterRefName string, bucketName string, remote_cluster_ref *metadata.RemoteClusterReference,
	remote_cluster_svc RemoteClusterSvc, logger *log.CommonLogger) (*RemoteBucketInfo, error) {
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
	
	username, password, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return err
	}
	connectionStr, err := remoteBucket.RemoteClusterRef.MyConnectionStr()
	if err != nil {
		return err
	}

	bucket, err := utils.RemoteBucket(connectionStr, remoteBucket.BucketName, username, password)
	if err != nil {
		return err
	}
	defer bucket.Close()

	remoteBucket.UUID = bucket.UUID
	remoteBucket.Capabilities = bucket.Capabilities

	remoteBucket.VBServerMap, err = bucket.GetVBmap(bucket.VBServerMap().ServerList)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to get VBServerMap for remote bucket %v", remoteBucket.BucketName))
	}

	remoteBucket.MemcachedAddrRestAddrMap = make(map[string]string)
	nodes := bucket.Nodes()
	for _, node := range nodes {
		restAddr := node.Hostname
		host := utils.GetHostName (restAddr)
		memcachedPort := node.Ports["direct"]
		memcachedAddr := utils.GetHostAddr (host, uint16(memcachedPort))
		remoteBucket.MemcachedAddrRestAddrMap[memcachedAddr] = restAddr		
	}
	
	remoteBucket.logger.Infof("remoteBucket.MemcachedAddrRestAddrMap=%v\n", remoteBucket.MemcachedAddrRestAddrMap)
	return nil
}

func (remoteBucket *RemoteBucketInfo) String() string {
	return fmt.Sprintf("%v - %v", remoteBucket.RemoteClusterRefName, remoteBucket.BucketName)
}

type RemoteVBReplicationStatus struct {
	VBUUID  uint64
	VBSeqno uint64
	VBNo    uint16
}

func (rep_status *RemoteVBReplicationStatus) IsEmpty() bool {
	return rep_status.VBUUID == 0
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
	//			  disableCkptBackwardsCompat
	//returns:
	//		  bMatch - true if the remote vbucket matches the current replication status
	//		  current_remoteVBUUID - new remote vb uuid might be retured if bMatch = false and there was a topology change on remote vb
	//		  err
	PreReplicate(remoteBucket *RemoteBucketInfo, knownRemoteVBStatus *RemoteVBReplicationStatus, disableCkptBackwardsCompat bool) (bVBMatch bool, current_remoteVBUUID uint64, err error)
	//call to do disk commit on the remote cluster, which ensure that the mutations replicated are durable
	//CommitForCheckpoint (_commit_for_checkpoint)
	//Parameters: remoteBucket - the information about the remote bucket
	//			  remoteVBUUID - the remote vb uuid on file
	//			  vbno		   - the vb number
	//returns:	  remote_seqno - the remote vbucket's high sequence number
	//			  vb_uuid	   - the new vb uuid if there was a topology change
	//			  err
	CommitForCheckpoint(remoteBucket *RemoteBucketInfo, remoteVBUUID uint64, vbno uint16) (remote_seqno uint64, vb_uuid uint64, err error)
	//call to mass validate vb uuids on remote cluster
	//Parameters: remoteBucket - the information about the remote bucket
	//			  remoteVBUUIDs - the map of vbno and vbuuid
	//returns: matching - the list of vb numbers whose vbuuid matches
	//		   mismatching - the list of vb numbers whose vbuuid mismatches
	//		   missing	- the list of vb numbers whose vbuuid is not kept on file
	MassValidateVBUUIDs(remoteBucket *RemoteBucketInfo, remoteVBUUIDs [][]uint64) (matching []interface{}, mismatching []interface{}, missing []interface{}, err error)
}
