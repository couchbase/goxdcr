// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/capi_utils"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	utilities "github.com/couchbase/goxdcr/utils"
)

var NoSupportForXDCRCheckpointingError = errors.New("No xdcrcheckpointing support on older node")
var VB_OPAQUE_MISMATCH_ERR error = errors.New("The remote vb opaque doesn't match with the one provided")

type RemoteBucketInfo struct {
	RemoteClusterRefName     string
	BucketName               string
	RemoteClusterRef         *metadata.RemoteClusterReference
	remoteClusterSvc         RemoteClusterSvc
	Capabilities             []string
	UUID                     string
	VBServerMap              map[string][]uint16
	MemcachedAddrRestAddrMap map[string]string
	RestAddrHttpClientMap    map[string]*http.Client
	// Whether to use couchApiBase or host address for capi service end point
	UseCouchApiBase bool
	logger          *log.CommonLogger
	utils           utilities.UtilsIface
}

func NewRemoteBucketInfo(remoteClusterRefName string, bucketName string, remote_cluster_ref *metadata.RemoteClusterReference,
	remote_cluster_svc RemoteClusterSvc, logger *log.CommonLogger, utilsIn utilities.UtilsIface) (*RemoteBucketInfo, error) {
	if remoteClusterRefName == "" || bucketName == "" {
		return nil, errors.New("remoteClusterRefName and bucketName are required")
	}

	remoteBucket := &RemoteBucketInfo{RemoteClusterRefName: remoteClusterRefName,
		BucketName:       bucketName,
		RemoteClusterRef: remote_cluster_ref,
		logger:           logger,
		utils:            utilsIn,
		remoteClusterSvc: remote_cluster_svc,
	}

	err := remoteBucket.refresh_internal(false)
	return remoteBucket, err
}

func (remoteBucket *RemoteBucketInfo) refresh_internal(full bool) error {
	if remoteBucket.RemoteClusterRef == nil && !full {
		remoteClusterRef, err := remoteBucket.remoteClusterSvc.RemoteClusterByRefName(remoteBucket.RemoteClusterRefName, false)
		if err != nil {
			remoteBucket.logger.Errorf("Failed to get remote cluster reference with refName=%v, err=%v\n", remoteBucket.RemoteClusterRefName, err)
			return err
		}

		remoteBucket.RemoteClusterRef = remoteClusterRef
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return err
	}
	connStr, err := remoteBucket.RemoteClusterRef.MyConnectionStr()
	if err != nil {
		return err
	}

	useExternal, err := remoteBucket.remoteClusterSvc.ShouldUseAlternateAddress(remoteBucket.RemoteClusterRef)
	if err != nil {
		return err
	}

	targetBucketInfo, err := remoteBucket.utils.GetBucketInfo(connStr, remoteBucket.BucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, remoteBucket.logger)
	if err != nil {
		return err
	}

	uuidObj, ok := targetBucketInfo[base.UUIDKey]
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

	ref := remoteBucket.RemoteClusterRef
	shouldUseTerseInfo, err := remoteBucket.utils.ShouldUseTerseBucketInfo(targetBucketInfo, connStr, remoteBucket.BucketName, useExternal, ref.IsHttps())
	if err != nil {
		const msg = "Error getting server vbuckets map from terse info for remote bucket"
		remoteBucket.logger.Errorf("%s, targetBucketInfo=%v, ref=%s, bucket=%s, terse=%v, err=%v", msg, targetBucketInfo, ref.Uuid(), remoteBucket.BucketName, shouldUseTerseInfo, err)
		return fmt.Errorf("%s: %w", msg, err)
	}

	bucketInfoForKvVBMap := targetBucketInfo
	if shouldUseTerseInfo {
		remoteBucket.logger.Infof("using terse bucket info for kv_vb_map construction for remote component, ref=%s, bucket=%s", ref.Uuid(), remoteBucket.BucketName)
		terseBucketInfo, err := remoteBucket.getTerseBucketInfo(ref, remoteBucket.BucketName)
		if err != nil {
			const msg = "Error checking for terse info necessity for remote bucket"
			remoteBucket.logger.Errorf("%s, ref=%s, bucket=%s, err=%v", msg, ref.Uuid(), remoteBucket.BucketName, err)
			return fmt.Errorf("%s: %w", msg, err)
		}

		bucketInfoForKvVBMap = terseBucketInfo
	}

	remoteBucket.VBServerMap, err = remoteBucket.utils.GetRemoteServerVBucketsMap(connStr, remoteBucket.BucketName, bucketInfoForKvVBMap, useExternal)
	if err != nil {
		const msg = "Failed to get VBServerMap for remote bucket"
		return fmt.Errorf("%s, bucket=%v, terse=%v: %w", msg, remoteBucket.BucketName, shouldUseTerseInfo, err)
	}

	remoteBucket.MemcachedAddrRestAddrMap = make(map[string]string)
	remoteBucket.RestAddrHttpClientMap = make(map[string]*http.Client)

	clusterCompatibility, err := remoteBucket.utils.GetClusterCompatibilityFromBucketInfo(targetBucketInfo, remoteBucket.logger)
	if err != nil {
		return err
	}
	nsServerScramShaSupport := base.IsClusterCompatible(clusterCompatibility, base.VersionForHttpScramShaSupport)
	remoteBucket.UseCouchApiBase = !nsServerScramShaSupport

	urlmap, err := capi_utils.ConstructCapiServiceEndPointMap(remoteBucket.BucketName, bucketInfoForKvVBMap, remoteBucket.RemoteClusterRef, remoteBucket.utils, remoteBucket.UseCouchApiBase, useExternal)
	if err != nil {
		return err
	}

	for serverAddr, urlstr := range urlmap {
		var hostAddr string
		if remoteBucket.UseCouchApiBase {
			// urlstr is couchApiBase, which looks like http://127.0.0.1:9500/default%2B77aceaa5b49efbd92a261b8a1e72dab5
			// we only need the host part
			u, err := url.Parse(urlstr)
			if err != nil {
				return err
			}
			hostAddr = u.Host
		} else {
			// urlstr is the host address, e.g., 127.0.0.1:9000, which can be used as is
			hostAddr = urlstr
		}

		remoteBucket.MemcachedAddrRestAddrMap[serverAddr] = hostAddr
		http_client, err := remoteBucket.utils.GetHttpClient(username, remoteBucket.RemoteClusterRef.HttpAuthMech(), certificate, sanInCertificate, clientCertificate, clientKey, hostAddr, remoteBucket.logger)
		if err != nil {
			return err
		}
		remoteBucket.RestAddrHttpClientMap[hostAddr] = http_client
	}
	remoteBucket.logger.Infof("remoteBucket.MemcachedAddrRestAddrMap=%v\n", remoteBucket.MemcachedAddrRestAddrMap)

	return nil
}

// getTerseBucketInfo returns results from pools/default/b/<bucketName> endpoint of the target reference.
func (remoteBucket *RemoteBucketInfo) getTerseBucketInfo(ref *metadata.RemoteClusterReference, bucketName string) (map[string]interface{}, error) {
	connStr, err := ref.MyConnectionStr()
	if err != nil {
		remoteBucket.logger.Errorf("Error getting connection string from reference for remote bucket, id=%v, bucket=%s, err=%v", ref.Uuid(), bucketName, err)
		return nil, err
	}

	terseTargetBucketInfo, err := remoteBucket.utils.GetTerseBucketInfo(connStr, bucketName, ref.UserName(), ref.Password(), ref.HttpAuthMech(), ref.Certificates(), ref.SANInCertificate(), ref.ClientCertificate(), ref.ClientKey(), remoteBucket.logger)
	if err != nil {
		remoteBucket.logger.Errorf("Error getting terse bucket info from reference for remote bucket, id=%s, bucket=%s, err=%v", ref.Uuid(), bucketName, err)
		return nil, err
	}

	return terseTargetBucketInfo, nil
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

func (rep_status *RemoteVBReplicationStatus) String() string {
	if rep_status == nil {
		return ""
	}

	vbOpaqueStr := "<>"
	if rep_status.VBOpaque != nil {
		vbOpaqueStr = fmt.Sprintf("%v", rep_status.VBOpaque.Value())
	}
	return fmt.Sprintf("%v.%v.%v", rep_status.VBNo, vbOpaqueStr, rep_status.VBSeqno)
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
}
