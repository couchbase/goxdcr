// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
	"time"
)

const (
	// parent dir of all Replication Specs
	ReplicationSpecsCatalogKey = "replicationSpec"
)

var ReplicationSpecAlreadyExistErrorMessage = "Replication to the same remote cluster and bucket already exists"
var ReplicationSpecNotFoundErrorMessage = "Requested resource not found"
var InvalidReplicationSpecError = errors.New("Invalid Replication spec")

//replication spec and its derived object
//This is what is put into the cache
type ReplicationSpecVal struct {
	spec       *metadata.ReplicationSpecification
	derivedObj interface{}
	cas        int64
}

func (rsv *ReplicationSpecVal) CAS(obj CacheableMetadataObj) bool {
	if rsv == nil || obj == nil {
		return true
	} else if rsv2, ok := obj.(*ReplicationSpecVal); ok {
		if rsv.cas == rsv2.cas {
			if !rsv.spec.SameSpec(rsv2.spec) {
				// increment cas only when the metadata portion of ReplicationSpecVal has been changed
				// in other words, concurrent updates to the metadata portion is not allowed -- later write fails
				// while concurrent updates to the runtime portion is allowed -- later write wins
				rsv.cas++
			}
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (rsv *ReplicationSpecVal) Clone() CacheableMetadataObj {
	if rsv != nil {
		clonedRsv := &ReplicationSpecVal{}
		*clonedRsv = *rsv
		if rsv.spec != nil {
			clonedRsv.spec = rsv.spec.Clone()
		}
		return clonedRsv
	}
	return rsv
}

func (rsv *ReplicationSpecVal) Redact() CacheableMetadataObj {
	if rsv != nil && rsv.spec != nil {
		rsv.spec.Redact()
	}
	return rsv
}

func (rsv *ReplicationSpecVal) CloneAndRedact() CacheableMetadataObj {
	if rsv != nil {
		return rsv.Clone().Redact()
	}
	return rsv
}

type ReplicationSpecService struct {
	xdcr_comp_topology_svc   service_def.XDCRCompTopologySvc
	metadata_svc             service_def.MetadataSvc
	uilog_svc                service_def.UILogSvc
	remote_cluster_svc       service_def.RemoteClusterSvc
	cluster_info_svc         service_def.ClusterInfoSvc
	cache                    *MetadataCache
	cache_lock               *sync.Mutex
	logger                   *log.CommonLogger
	metadata_change_callback base.MetadataChangeHandlerCallback
	utils                    utilities.UtilsIface
}

func NewReplicationSpecService(uilog_svc service_def.UILogSvc, remote_cluster_svc service_def.RemoteClusterSvc,
	metadata_svc service_def.MetadataSvc, xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext, utilities_in utilities.UtilsIface) (*ReplicationSpecService, error) {
	logger := log.NewLogger("ReplSpecSvc", logger_ctx)
	svc := &ReplicationSpecService{
		metadata_svc:           metadata_svc,
		uilog_svc:              uilog_svc,
		remote_cluster_svc:     remote_cluster_svc,
		xdcr_comp_topology_svc: xdcr_comp_topology_svc,
		cluster_info_svc:       cluster_info_svc,
		cache:                  nil,
		cache_lock:             &sync.Mutex{},
		logger:                 logger,
		utils:                  utilities_in,
	}

	return svc, svc.initCacheFromMetaKV()
}

func (service *ReplicationSpecService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

func (service *ReplicationSpecService) initCache() {
	service.cache = NewMetadataCache(service.logger)
	service.logger.Info("Cache has been initialized for ReplicationSpecService")
}

func (service *ReplicationSpecService) getCache() *MetadataCache {
	if service.cache == nil {
		panic("Cache has not been initialized for ReplicationSpecService")
	}
	return service.cache
}

func (service *ReplicationSpecService) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	spec, err := service.replicationSpec(replicationId)
	if err != nil {
		return nil, err
	}

	// return a clone so that modification to the spec returned won't affect the spec in cache
	return spec.Clone(), nil
}

// this method is cheaper than ReplicationSpec() and should be called only when the spec returned won't be modified or that the modifications do not matter.
func (service *ReplicationSpecService) replicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	val, ok := service.getCache().Get(replicationId)
	if !ok || val == nil || val.(*ReplicationSpecVal).spec == nil {
		return nil, errors.New(ReplicationSpecNotFoundErrorMessage)
	}

	return val.(*ReplicationSpecVal).spec, nil
}

//validate the existence of source bucket
func (service *ReplicationSpecService) validateSourceBucket(errorMap base.ErrorMap, sourceBucket, targetCluster, targetBucket string) (string, string, error) {
	var err_source error
	start_time := time.Now()
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		return "", "", errors.New("XDCRTopologySvc.MyConnectionStr() returned empty string")
	}

	_, sourceBucketType, sourceBucketUUID, sourceConflictResolutionType, sourceEvictionPolicy, _, err_source := service.utils.BucketValidationInfo(local_connStr, sourceBucket, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, service.logger)
	service.logger.Infof("Result from local bucket look up: bucketName=%v, err_source=%v, time taken=%v\n", sourceBucket, err_source, time.Since(start_time))
	service.validateBucket(sourceBucket, targetCluster, targetBucket, sourceBucketType, sourceEvictionPolicy, err_source, errorMap, true)

	return sourceBucketUUID, sourceConflictResolutionType, nil
}

// validate that the source bucket and target bucket are not the same bucket
// i.e., validate that the following are not both true:
// 1. sourceBucketName == targetBucketName
// 2. sourceClusterUuid == targetClusterUuid
func (service *ReplicationSpecService) validateSrcTargetNonIdenticalBucket(errorMap base.ErrorMap, sourceBucket, targetBucket string, targetClusterRef *metadata.RemoteClusterReference) error {
	if sourceBucket == targetBucket {
		start_time := time.Now()
		sourceClusterUuid, err := service.xdcr_comp_topology_svc.MyClusterUuid()
		if err != nil {
			return errors.New("cannot get local cluster uuid")
		}

		if sourceClusterUuid == targetClusterRef.Uuid {
			errorMap[base.PlaceHolderFieldKey] = errors.New("Replication from a bucket to the same bucket is not allowed")
		} else {
			service.logger.Infof("Validated that source bucket and target bucket are not the same. time taken=%v\n", time.Since(start_time))
		}
	}
	return nil
}

// validate remote cluster ref
func (service *ReplicationSpecService) getRemoteReference(errorMap base.ErrorMap, targetCluster string) (*metadata.RemoteClusterReference, string, string, string, base.HttpAuthMech, []byte, bool, []byte, []byte) {
	var remote_userName, remote_password string
	var httpAuthMech base.HttpAuthMech
	var certificate []byte
	var sanInCertificate bool
	var clientCertificate []byte
	var clientKey []byte
	var err error
	var remote_connStr string
	start_time := time.Now()
	// refresh remote cluster reference to ensure that it is up to date
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByRefName(targetCluster, true)
	if err != nil {
		errorMap[base.ToCluster] = service.utils.NewEnhancedError("cannot find remote cluster", err)
	} else {
		service.logger.Infof("Successfully retrieved target cluster reference %v. time taken=%v\n", targetClusterRef.Name, time.Since(start_time))
		remote_connStr, err = targetClusterRef.MyConnectionStr()
		if err != nil {
			errorMap[base.ToCluster] = service.utils.NewEnhancedError("Invalid remote cluster. MyConnectionStr() failed.", err)
			return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey
		}
		remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err = targetClusterRef.MyCredentials()
		if err != nil {
			errorMap[base.ToCluster] = service.utils.NewEnhancedError("Invalid remote cluster. MyCredentials() failed.", err)
			return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey
		}
	}

	return targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey
}

func (service *ReplicationSpecService) validateReplicationSpecDoesNotAlreadyExist(errorMap base.ErrorMap, sourceBucket string, targetClusterRef *metadata.RemoteClusterReference, targetBucket string) {
	repId := metadata.ReplicationId(sourceBucket, targetClusterRef.Uuid, targetBucket)
	_, err := service.replicationSpec(repId)
	if err == nil {
		errorMap[base.PlaceHolderFieldKey] = errors.New(ReplicationSpecAlreadyExistErrorMessage)
	}
}

func (service *ReplicationSpecService) populateConnectionCreds(targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16, targetBucket string, targetBucketInfo map[string]interface{}) (allKvConnStrs []string, username, password string, err error) {
	if len(targetKVVBMap) == 0 {
		err = errors.New("kv vb map is empty")
		return
	}

	// Pull all the kv connection strings to a slice
	i := 0
	allKvConnStrs = make([]string, len(targetKVVBMap), len(targetKVVBMap))
	for kvaddr, _ := range targetKVVBMap {
		allKvConnStrs[i] = kvaddr
		i++
	}

	hasRBACSupport := false
	isTargetES := service.utils.CheckWhetherClusterIsESBasedOnBucketInfo(targetBucketInfo)
	if !isTargetES {
		var targetClusterCompatibility int
		targetClusterCompatibility, err = service.utils.GetClusterCompatibilityFromBucketInfo(targetBucketInfo, service.logger)
		if err != nil {
			err = fmt.Errorf("Error retrieving cluster compatibility on bucket %v. err=%v", targetBucket, err)
			return
		}

		hasRBACSupport = base.IsClusterCompatible(targetClusterCompatibility, base.VersionForRBACAndXattrSupport)
	}

	if hasRBACSupport {
		username = targetClusterRef.UserName
		password = targetClusterRef.Password
	} else {
		username = targetBucket
		password, err = service.utils.GetBucketPasswordFromBucketInfo(targetBucket, targetBucketInfo, service.logger)
		if err != nil {
			err = fmt.Errorf("Error retrieving password on bucket %v. err=%v", targetBucket, err)
			return
		}
	}
	return
}

func (service *ReplicationSpecService) validateXmemSettings(errorMap base.ErrorMap, targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16, sourceBucket, targetBucket string, targetBucketInfo map[string]interface{}, kvConnStr, username, password string) {
	if targetClusterRef.IsHalfEncryption() {
		// for half-ssl ref, validate that target memcached supports SCRAM-SHA authentication
		client, err := service.utils.GetRemoteMemcachedConnection(kvConnStr, username, password, targetBucket,
			base.ComposeUserAgentWithBucketNames("Goxdcr ReplSpecSvc", sourceBucket, targetBucket),
			false /*plainAuth*/, 0 /*keepAlivePeriod*/, service.logger)
		if client != nil {
			client.Close()
		}
		if err != nil {
			errorMap[base.ToCluster] = fmt.Errorf("Error verifying SCRAM-SHA authentication support. err=%v", err)
		}
	}
}

func (service *ReplicationSpecService) validateES(errorMap base.ErrorMap, targetClusterRef *metadata.RemoteClusterReference, targetBucketInfo map[string]interface{}, repl_type interface{}, sourceConflictResolutionType, targetConflictResolutionType string) []string {
	warnings := make([]string, 0)
	isTargetES := service.utils.CheckWhetherClusterIsESBasedOnBucketInfo(targetBucketInfo)
	if repl_type != metadata.ReplicationTypeCapi {
		// for xmem replication, validate that target is not an elasticsearch cluster
		if isTargetES {
			errorMap[base.Type] = errors.New("Replication to an Elasticsearch target cluster using XDCR Version 2 (XMEM protocol) is not supported. Use XDCR Version 1 (CAPI protocol) instead.")
			return warnings
		}

		// for xmem replication, validate that source and target bucket have the same conflict resolution type metadata
		if sourceConflictResolutionType != targetConflictResolutionType {
			errorMap[base.PlaceHolderFieldKey] = errors.New("Replication between buckets with different ConflictResolutionType setting is not allowed")
			return warnings
		}
	} else {
		// for capi replication
		// if encryption is enabled, return error
		if targetClusterRef.IsEncryptionEnabled() {
			errorMap[base.Type] = errors.New("XDCR Version 1 (CAPI protocol) replication does not support secure connections")
			return warnings
		}

		// if target is not elastic search cluster, compose a warning to be displayed in the replication creation ui log
		if !isTargetES {
			warnings = append(warnings, fmt.Sprintf("XDCR Version 1 (CAPI protocol) replication has been deprecated and should be used only for an Elasticsearch target cluster. Since the current target cluster is a Couchbase Server cluster, use XDCR Version 2 (XMEM protocol) instead."))
		}
		// if source bucket has timestamp conflict resolution enabled,
		// compose a warning to be displayed in the replication creation ui log
		if sourceConflictResolutionType == base.ConflictResolutionType_Lww {
			warnings = append(warnings, fmt.Sprintf("Replication to an Elasticsearch target cluster uses XDCR Version 1 (CAPI protocol), which does not support Timestamp Based Conflict Resolution. Even though the replication source bucket has Timestamp Based Conflict Resolution enabled, the replication will use Sequence Number Based Conflict Resolution instead."))
		}
	}
	return warnings
}

/**
 * Main Validation routine, supplemented by multiple helper sub-routines.
 * Each sub-routine may be daisy chained by variables that would be helpful for further subroutines.
 */
func (service *ReplicationSpecService) ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, []string) {
	errorMap := make(base.ErrorMap)
	service.logger.Infof("Start ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v\n", sourceBucket, targetCluster, targetBucket)
	defer service.logger.Infof("Finished ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v, errorMap=%v\n", sourceBucket, targetCluster, targetBucket, errorMap)

	sourceBucketUUID, sourceConflictResolutionType, err := service.validateSourceBucket(errorMap, sourceBucket, targetCluster, targetBucket)
	if len(errorMap) > 0 || err != nil {
		return "", "", nil, errorMap, err, nil
	}

	targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey := service.getRemoteReference(errorMap, targetCluster)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	service.validateReplicationSpecDoesNotAlreadyExist(errorMap, sourceBucket, targetClusterRef, targetBucket)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	err = service.validateSrcTargetNonIdenticalBucket(errorMap, sourceBucket, targetBucket, targetClusterRef)
	if len(errorMap) > 0 || err != nil {
		return "", "", nil, errorMap, err, nil
	}

	targetBucketInfo, targetBucketUUID, targetConflictResolutionType, targetKVVBMap := service.validateTargetBucket(errorMap, remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, sourceBucket, targetCluster)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	err = service.validateReplicationSettingsInternal(errorMap, sourceBucket, targetCluster, targetBucket, settings,
		targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate,
		clientCertificate, clientKey, targetKVVBMap, targetBucketInfo)
	if len(errorMap) > 0 || err != nil {
		return "", "", nil, errorMap, err, nil
	}

	repl_type, ok := settings[metadata.ReplicationType]
	if !ok {
		repl_type = metadata.ReplicationTypeXmem
	}

	warnings := service.validateES(errorMap, targetClusterRef, targetBucketInfo, repl_type, sourceConflictResolutionType, targetConflictResolutionType)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	if warnings != nil {
		service.logger.Warnf("Warnings from ValidateAddReplicationSpec : %v\n", base.FlattenStringArray(warnings))
	}

	return sourceBucketUUID, targetBucketUUID, targetClusterRef, errorMap, nil, warnings
}

func (service *ReplicationSpecService) ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap) (base.ErrorMap, error) {
	var errorMap base.ErrorMap = make(base.ErrorMap)

	targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey := service.getRemoteReference(errorMap, targetCluster)
	if len(errorMap) > 0 {
		return errorMap, nil
	}

	targetBucketInfo, _, _, _, _, targetKVVBMap, err := service.utils.BucketValidationInfo(remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
	if err != nil {
		return nil, err
	}

	err = service.validateReplicationSettingsInternal(errorMap, sourceBucket, targetCluster, targetBucket, settings, targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, targetKVVBMap, targetBucketInfo)
	return errorMap, err
}

func (service *ReplicationSpecService) validateReplicationSettingsInternal(errorMap base.ErrorMap, sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap,
	targetClusterRef *metadata.RemoteClusterReference, remote_connStr, remote_userName, remote_password string, httpAuthMech base.HttpAuthMech, certificate []byte, sanInCertificate bool,
	clientCertificate, clientKey []byte, targetKVVBMap map[string][]uint16, targetBucketInfo map[string]interface{}) error {

	var populateErr error
	var err error
	allKvConnStrs, username, password, populateErr := service.populateConnectionCreds(targetClusterRef, targetKVVBMap, targetBucket, targetBucketInfo)
	if populateErr != nil {
		return populateErr
	}

	repl_type, ok := settings[metadata.ReplicationType]
	compressionType, compressionOk := settings[metadata.CompressionType]
	if !ok || repl_type == metadata.ReplicationTypeXmem {
		service.validateXmemSettings(errorMap, targetClusterRef, targetKVVBMap, sourceBucket, targetBucket, targetBucketInfo, allKvConnStrs[0], username, password)
		if len(errorMap) > 0 {
			return nil
		}

		// Compression is only allowed in XMEM mode
		if compressionOk && compressionType != base.CompressionTypeNone {
			err = service.validateCompression(errorMap, sourceBucket, targetClusterRef, targetKVVBMap, targetBucket, targetBucketInfo, compressionType.(int), allKvConnStrs, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey)
			if len(errorMap) > 0 || err != nil {
				return err
			}
		}
	} else if compressionOk && compressionType != base.CompressionTypeNone {
		errorMap[base.CompressionTypeREST] = fmt.Errorf("Compression feature is incompatible with XDCR Version 1 (CAPI Protocol)")
		return nil
	}
	return nil
}

// validate compression - should only be called if compression setting dictates that there is compression
func (service *ReplicationSpecService) validateCompression(errorMap base.ErrorMap, sourceBucket string, targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16, targetBucket string, targetBucketInfo map[string]interface{}, compressionType int, allKvConnStrs []string, username, password string,
	httpAuthMech base.HttpAuthMech, certificate []byte, SANInCertificate bool, clientCertificate, clientKey []byte) error {
	var requestedFeaturesSet utilities.HELOFeatures
	var err error
	requestedFeaturesSet.CompressionType = base.GetCompressionType(compressionType)
	errKey := base.CompressionTypeREST

	if compressionType == base.CompressionTypeNone {
		// Don't check anything
		return err
	}

	err = service.validateCompressionPreReq(errorMap, targetBucket, targetBucketInfo, compressionType, errKey)
	if len(errorMap) > 0 || err != nil {
		return err
	}

	err = service.validateCompressionLocal(errorMap, sourceBucket, targetBucket, errKey, requestedFeaturesSet)
	if len(errorMap) > 0 || err != nil {
		return err
	}

	// Need to validate each node of the target to make sure all of them can support compression
	for i := 0; i < len(allKvConnStrs); i++ {
		err = service.validateCompressionTarget(errorMap, sourceBucket, targetClusterRef, targetBucket, allKvConnStrs[i], username, password,
			httpAuthMech, certificate, SANInCertificate, clientCertificate, clientKey, errKey, requestedFeaturesSet)
		if len(errorMap) > 0 || err != nil {
			return err
		}
	}

	return err
}

func (service *ReplicationSpecService) validateCompressionPreReq(errorMap base.ErrorMap, targetBucket string, targetBucketInfo map[string]interface{}, compressionType int,
	errKey string) error {
	var err error
	isEnterprise, enterpriseErr := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()
	if enterpriseErr != nil {
		err = enterpriseErr
		return err
	} else if !isEnterprise {
		errMsg := fmt.Sprintf("Compression feature is available only on Enterprise Edition")
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
		return err
	}

	// Version check
	targetClusterCompatibility, err := service.utils.GetClusterCompatibilityFromBucketInfo(targetBucketInfo, service.logger)
	if err != nil {
		errorMap[base.ToCluster] = fmt.Errorf("Error retrieving cluster compatibility on bucket %v. err=%v", targetBucket, err)
		return err
	}
	hasCompressionSupport := base.IsClusterCompatible(targetClusterCompatibility, base.VersionForCompressionSupport)
	if !hasCompressionSupport {
		errorMap[base.ToCluster] = fmt.Errorf("The version of Couchbase software installed on the remote cluster does not support compression. Please upgrade the destination cluster to version %v.%v or above to enable this feature",
			base.VersionForCompressionSupport[0], base.VersionForCompressionSupport[1])
		return err
	}
	return err
}

func (service *ReplicationSpecService) validateCompressionLocal(errorMap base.ErrorMap, sourceBucket string, targetBucket string, errKey string,
	requestedFeaturesSet utilities.HELOFeatures) error {
	localAddr, err := service.xdcr_comp_topology_svc.MyMemcachedAddr()
	if err != nil {
		return err
	}
	localClient, respondedFeatures, err := service.utils.GetMemcachedConnectionWFeatures(localAddr, sourceBucket,
		base.ComposeUserAgentWithBucketNames("Goxdcr ReplSpecSvc", sourceBucket, targetBucket), base.KeepAlivePeriod, requestedFeaturesSet, service.logger)
	if localClient != nil {
		localClient.Close()
	}
	if err != nil {
		return err
	}
	if respondedFeatures.CompressionType != requestedFeaturesSet.CompressionType {
		errorMap[errKey] = fmt.Errorf("Source cluster %v does not support %v compression. Please verify the configuration on the source cluster for %v compression support, or disable compression for this replication.",
			localAddr, base.CompressionTypeStrings[requestedFeaturesSet.CompressionType], base.CompressionTypeStrings[requestedFeaturesSet.CompressionType])
		return err
	}
	return err
}

func (service *ReplicationSpecService) validateCompressionTarget(errorMap base.ErrorMap, sourceBucket string, targetClusterRef *metadata.RemoteClusterReference, targetBucket string, kvConnStr, username, password string,
	httpAuthMech base.HttpAuthMech, certificate []byte, SANInCertificate bool, clientCertificate, clientKey []byte, errKey string, requestedFeaturesSet utilities.HELOFeatures) error {
	var conn mcc.ClientIface
	sslPortMap := make(base.SSLPortMap)
	var err error

	if targetClusterRef.IsFullEncryption() {
		// Full encryption needs TLS connection with special inputs
		var connStr string
		connStr, err = targetClusterRef.MyConnectionStr()
		if err != nil {
			return err
		}
		sslPortMap, err = service.utils.GetMemcachedSSLPortMap(connStr, username, password, httpAuthMech, certificate, SANInCertificate, clientCertificate, clientKey, targetBucket, service.logger)
		if err != nil {
			return err
		}
		sslPort, ok := sslPortMap[kvConnStr]
		if !ok {
			err = errors.New(fmt.Sprintf("Unable to populate sslPort using kvConnStr: %v sslPortMap: %v", kvConnStr, sslPortMap))
			return err
		}
		hostName := base.GetHostName(kvConnStr)
		sslConStr := base.GetHostAddr(hostName, sslPort)
		conn, err = base.NewTLSConn(sslConStr, username, password, certificate, SANInCertificate, clientCertificate, clientKey, targetBucket, service.logger)
	} else {
		// Half-Encryption should have already been verified via SCRAM-SHA check in validateXmem...
		// Unencrypted is also fine here with RawConn
		conn, err = service.utils.GetMemcachedRawConn(kvConnStr, username, password, targetBucket, !targetClusterRef.IsEncryptionEnabled() /*plainAuth*/, 0 /*keepAliveeriod*/, service.logger)
	}
	connCloseFunc := func() {
		if conn != nil {
			conn.Close()
		}
	}
	defer connCloseFunc()
	if err != nil {
		return err
	}

	respondedFeatures, err := service.utils.SendHELOWithFeatures(conn, base.ComposeUserAgentWithBucketNames("Goxdcr ReplSpecSvc", sourceBucket, targetBucket), base.HELOTimeout, base.HELOTimeout, requestedFeaturesSet, service.logger)
	if err != nil {
		return err
	}

	if respondedFeatures.CompressionType != requestedFeaturesSet.CompressionType {
		errorMap[base.ToCluster] = fmt.Errorf("Target cluster (node %v) does not support %v compression. Please verify the configuration on the target cluster for %v compression support, or disable compression for this replication.",
			kvConnStr, base.CompressionTypeStrings[requestedFeaturesSet.CompressionType], base.CompressionTypeStrings[requestedFeaturesSet.CompressionType])
		return err
	}
	return err
}

//validate target bucket
func (service *ReplicationSpecService) validateTargetBucket(errorMap base.ErrorMap, remote_connStr, targetBucket, remote_userName, remote_password string, httpAuthMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte,
	sourceBucket string, targetCluster string) (targetBucketInfo map[string]interface{}, targetBucketUUID, targetConflictResolutionType string, targetKVVBMap map[string][]uint16) {
	start_time := time.Now()

	targetBucketInfo, targetBucketType, targetBucketUUID, targetConflictResolutionType, _, targetKVVBMap, err_target := service.utils.RemoteBucketValidationInfo(remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
	service.logger.Infof("Result from remote bucket look up: connStr=%v, bucketName=%v, targetBucketType=%v, err_target=%v, time taken=%v\n", remote_connStr, targetBucket, targetBucketType, err_target, time.Since(start_time))

	service.validateBucket(sourceBucket, targetCluster, targetBucket, targetBucketType, "", err_target, errorMap, false)

	return targetBucketInfo, targetBucketUUID, targetConflictResolutionType, targetKVVBMap
}

func (service *ReplicationSpecService) validateBucket(sourceBucket, targetCluster, targetBucket, bucketType string, evictionPolicy string, err error, errorMap map[string]error, isSourceBucket bool) {
	var qualifier, errKey, bucketName string

	// should always return nil for second argument (error)
	isEnterprise, _ := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()

	if isSourceBucket {
		qualifier = "source"
		errKey = base.FromBucket
		bucketName = sourceBucket
	} else {
		qualifier = "target"
		errKey = base.ToBucket
		bucketName = targetBucket
	}

	if err == service.utils.GetNonExistentBucketError() {
		service.logger.Errorf("Spec [sourceBucket=%v, targetCluster=%v, targetBucket=%v] refers to non-existent %v bucket\n", sourceBucket, targetCluster, targetBucket, qualifier)
		errorMap[errKey] = service.utils.BucketNotFoundError(bucketName)
	} else if err != nil {
		errMsg := fmt.Sprintf("Error validating %v bucket '%v'. err=%v", qualifier, bucketName, err)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	} else if bucketType != base.CouchbaseBucketType && bucketType != base.EphemeralBucketType {
		errMsg := fmt.Sprintf("Incompatible %v bucket '%v'", qualifier, bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	} else if isSourceBucket && bucketType == base.EphemeralBucketType && evictionPolicy == base.EvictionPolicyNRU {
		// source bucket cannot be ephemeral bucket with eviction
		errMsg := fmt.Sprintf("XDCR replication for ephemeral bucket with eviction '%v' is not allowed", bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	} else if bucketType == base.EphemeralBucketType && !isEnterprise {
		errMsg := fmt.Sprintf("XDCR replication for ephemeral bucket '%v' is available only on Enterprise Edition", bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	}
}

func (service *ReplicationSpecService) AddReplicationSpec(spec *metadata.ReplicationSpecification, additionalInfo string) error {
	service.logger.Infof("Start AddReplicationSpec, spec=%v, additionalInfo=%v\n", spec, additionalInfo)

	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	service.logger.Info("Adding it to metadata store...")

	key := getKeyFromReplicationId(spec.Id)
	err = service.metadata_svc.AddWithCatalog(ReplicationSpecsCatalogKey, key, value)
	if err != nil {
		return err
	}

	err = service.loadLatestMetakvRevisionIntoSpec(spec)
	if err != nil {
		return err
	}

	err = service.updateCache(spec.Id, spec)
	if err == nil {
		service.writeUiLogWithAdditionalInfo(spec, "created", additionalInfo)
	}
	return err
}

func (service *ReplicationSpecService) loadLatestMetakvRevisionIntoSpec(spec *metadata.ReplicationSpecification) error {
	key := getKeyFromReplicationId(spec.Id)
	_, rev, err := service.metadata_svc.Get(key)
	if err != nil {
		return err
	}
	spec.Revision = rev
	if spec.Settings != nil {
		// Use the same revision value with ReplicationSettings since they are considered one entity from
		// Replication Spec Service perspective
		spec.Settings.Revision = rev
	}
	return nil
}

func (service *ReplicationSpecService) SetReplicationSpec(spec *metadata.ReplicationSpecification) error {
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	key := getKeyFromReplicationId(spec.Id)

	err = service.metadata_svc.Set(key, value, spec.Revision)
	if err != nil {
		return err
	}

	err = service.loadLatestMetakvRevisionIntoSpec(spec)
	if err != nil {
		return err
	}

	err = service.updateCache(spec.Id, spec)
	if err == nil {
		service.logger.Infof("Replication spec %s has been updated, rev=%v\n", spec.Id, spec.Revision)
		return nil
	} else {
		return err
	}
}

func (service *ReplicationSpecService) DelReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	return service.DelReplicationSpecWithReason(replicationId, "")
}

func (service *ReplicationSpecService) DelReplicationSpecWithReason(replicationId string, reason string) (*metadata.ReplicationSpecification, error) {
	spec, err := service.replicationSpec(replicationId)
	if err != nil {
		return nil, errors.New(ReplicationSpecNotFoundErrorMessage)
	}

	key := getKeyFromReplicationId(replicationId)
	err = service.metadata_svc.DelWithCatalog(ReplicationSpecsCatalogKey, key, spec.Revision)
	if err != nil {
		service.logger.Errorf("Failed to delete replication spec, key=%v, err=%v\n", key, err)
		return nil, err
	}

	err = service.updateCache(replicationId, nil)
	if err == nil {
		service.writeUiLog(spec, "removed", reason)
		service.logger.Infof("Replication spec %v successfully deleted. \n", key)
		return spec, nil
	} else {
		service.logger.Errorf("Failed to delete replication spec, key=%v, err=%v\n", key, err)
		return nil, err
	}

}

// NOTE - this is an expensive operation that will force a clean resync of the cache from metaKV
func (service *ReplicationSpecService) initCacheFromMetaKV() (err error) {
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()
	var KVsFromMetaKV []*service_def.MetadataEntry
	var KVsFromMetaKVErr error

	// Clears all from cache before reloading
	service.initCache()

	//	GetAllMetadataFromCatalog(catalogKey string) ([]*MetadataEntry, error)
	getAllKVsOpFunc := func() error {
		KVsFromMetaKV, KVsFromMetaKVErr = service.metadata_svc.GetAllMetadataFromCatalog(ReplicationSpecsCatalogKey)
		return KVsFromMetaKVErr
	}
	err = service.utils.ExponentialBackoffExecutor("GetAllMetadataFromCatalogReplicationSpec", base.RetryIntervalMetakv,
		base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, getAllKVsOpFunc)
	if err != nil {
		service.logger.Errorf("Unable to get all the KVs from metakv: %v", err)
		return
	}

	// One by one update the specs
	for _, KVentry := range KVsFromMetaKV {
		key := KVentry.Key
		marshalledSpec := KVentry.Value
		rev := KVentry.Rev

		replicationId := service.getReplicationIdFromKey(key)
		replSpec, err := constructReplicationSpec(marshalledSpec, rev)
		if err != nil {
			service.logger.Errorf("Unable to construct spec %v from metaKV's data. err: %v", key, err)
			continue
		}
		service.updateCacheInternalNoLock(replicationId, replSpec)
	}
	return

}

func (service *ReplicationSpecService) AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		if val.(*ReplicationSpecVal).spec != nil {
			specs[key] = val.(*ReplicationSpecVal).spec.Clone()
		}
	}
	return specs, nil
}

func (service *ReplicationSpecService) AllReplicationSpecIds() ([]string, error) {
	repIds := []string{}
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		if val.(*ReplicationSpecVal).spec != nil {
			repIds = append(repIds, key)
		}
	}
	return repIds, nil
}

func (service *ReplicationSpecService) AllReplicationSpecIdsForBucket(bucket string) ([]string, error) {
	var repIds []string
	allRepIds, err := service.AllReplicationSpecIds()
	if err != nil {
		return nil, err
	}

	if allRepIds != nil {
		for _, repId := range allRepIds {
			// there should not be any errors since allRepIds should contain valid replication ids
			match, _ := metadata.IsReplicationIdForSourceBucket(repId, bucket)
			if match {
				repIds = append(repIds, repId)
			}
		}
	}
	return repIds, nil
}

func (service *ReplicationSpecService) removeSpecFromCache(specId string) error {
	//soft remove it from cache by setting SpecVal.spec = nil, but keep the key there
	//so that the derived object can still be retrieved and be acted on for cleaning-up.
	val, ok := service.getCache().Get(specId)
	if ok && val != nil {
		specVal, ok1 := val.(*ReplicationSpecVal)
		if ok1 {
			specVal.spec = nil
		}
	}

	return nil
}

func constructReplicationSpec(value []byte, rev interface{}) (*metadata.ReplicationSpecification, error) {
	if value == nil {
		return nil, nil
	}

	spec := &metadata.ReplicationSpecification{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}
	spec.Revision = rev
	return spec, nil
}

// Implement callback function for metakv
func (service *ReplicationSpecService) ReplicationSpecServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("ReplicationSpecServiceCallback called on path = %v\n", path)

	newSpec, err := constructReplicationSpec(value, rev)
	if err != nil {
		service.logger.Errorf("Error marshaling replication spec. value=%v, err=%v\n", string(value), err)
		return err
	}

	specId := service.getReplicationIdFromKey(GetKeyFromPath(path))

	return service.updateCache(specId, newSpec)

}

func (service *ReplicationSpecService) updateCacheInternalNoLock(specId string, newSpec *metadata.ReplicationSpecification) (*metadata.ReplicationSpecification, bool, error) {
	oldSpec, err := service.replicationSpec(specId)
	if err != nil {
		oldSpec = nil
	}

	updated := false
	if newSpec == nil {
		if oldSpec != nil {
			// replication spec has been deleted
			service.removeSpecFromCache(specId)
			updated = true
		}
	} else {
		// replication spec has been created or updated

		// no need to update cache if newSpec is the same as the one already in cache
		if !newSpec.SameSpec(oldSpec) {
			err = service.cacheSpec(service.getCache(), specId, newSpec)
			if err == nil {
				specId = newSpec.Id
				updated = true
			} else {
				return oldSpec, updated, err
			}
		}

	}

	return oldSpec, updated, err
}

func (service *ReplicationSpecService) updateCache(specId string, newSpec *metadata.ReplicationSpecification) error {
	//this ensures that all accesses to the cache in this method are a single atomic operation,
	// this is needed because this method can be called concurrently
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

	oldSpec, updated, err := service.updateCacheInternalNoLock(specId, newSpec)

	if updated && service.metadata_change_callback != nil {
		err = service.metadata_change_callback(specId, oldSpec, newSpec)
		if err != nil {
			service.logger.Error(err.Error())
			return err
		}
	}

	return nil
}

func (service *ReplicationSpecService) writeUiLog(spec *metadata.ReplicationSpecification, action, reason string) {
	if service.uilog_svc != nil {
		var uiLogMsg string
		remoteClusterName := service.remote_cluster_svc.GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)
		if reason != "" {
			uiLogMsg = fmt.Sprintf("Replication from bucket \"%s\" to bucket \"%s\" on cluster \"%s\" %s, since %s", spec.SourceBucketName, spec.TargetBucketName, remoteClusterName, action, reason)
		} else {
			uiLogMsg = fmt.Sprintf("Replication from bucket \"%s\" to bucket \"%s\" on cluster \"%s\" %s.", spec.SourceBucketName, spec.TargetBucketName, remoteClusterName, action)
		}
		service.uilog_svc.Write(uiLogMsg)
	}
}

func (service *ReplicationSpecService) writeUiLogWithAdditionalInfo(spec *metadata.ReplicationSpecification, action, additionalInfo string) {
	if service.uilog_svc != nil {
		remoteClusterName := service.remote_cluster_svc.GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)
		uiLogMsg := fmt.Sprintf("Replication from bucket \"%s\" to bucket \"%s\" on cluster \"%s\" %s.", spec.SourceBucketName, spec.TargetBucketName, remoteClusterName, action)
		if additionalInfo != "" {
			uiLogMsg += fmt.Sprintf("\n%s", additionalInfo)
		}
		service.uilog_svc.Write(uiLogMsg)
	}
}

func (service *ReplicationSpecService) IsReplicationValidationError(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), ReplicationSpecAlreadyExistErrorMessage) || strings.HasPrefix(err.Error(), ReplicationSpecNotFoundErrorMessage)
	} else {
		return false
	}
}

func getKeyFromReplicationId(replicationId string) string {
	return ReplicationSpecsCatalogKey + base.KeyPartsDelimiter + replicationId
}

func (service *ReplicationSpecService) getReplicationIdFromKey(key string) string {
	prefix := ReplicationSpecsCatalogKey + base.KeyPartsDelimiter
	if !strings.HasPrefix(key, prefix) {
		// should never get here.
		panic(fmt.Sprintf("Got unexpected key %v for replication spec", key))
	}
	return key[len(prefix):]
}

func (service *ReplicationSpecService) validateExistingReplicationSpec(spec *metadata.ReplicationSpecification) (error, error) {
	//validate the existence of source bucket
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		err := fmt.Errorf("XDCRTopologySvc.MyConnectionStr() returned empty string when validating spec %v", spec.Id)
		return err, err
	}
	sourceBucketUuid, err_source := service.utils.LocalBucketUUID(local_connStr, spec.SourceBucketName, service.logger)

	if err_source != nil {
		if err_source == service.utils.GetNonExistentBucketError() {
			/* When we get NonExistentBucketError, there are two possibilities:
			1. the source bucket has been deleted.
			2. the source node is not accessible.

			We need to make an additional call to retrieve a list of buckets from source cluster
			A. if the call returns successfully and shows that the source bucket is no longer in bucket list, it is case #1
			It is safe to delete replication spec in this case.
			B. if the call returns successfully and shows that the source bucket is still in bucket list, we can use the new info
			retrieved to continue source bucket validation
			C. if the call does not return sucessfully, we have to play safe and skip the current round of source bucket check */
			buckets, err := service.utils.GetLocalBuckets(local_connStr, service.logger)
			if err == nil {
				foundSourceBucket := false
				for bucketName, bucketUuid := range buckets {
					if bucketName == spec.SourceBucketName {
						foundSourceBucket = true
						sourceBucketUuid = bucketUuid
						break
					}
				}
				if !foundSourceBucket {
					// case 1, delete repl spec
					errMsg := fmt.Sprintf("spec %v refers to non-existent source bucket \"%v\"", spec.Id, spec.SourceBucketName)
					service.logger.Warn(errMsg)
					return InvalidReplicationSpecError, errors.New(errMsg)
				}
				// if source bucket is found, we have already populated sourceBucketUuid accordingly
				// continue with source bucket validation
			} else {
				// case 2, source node inaccessible. skip source bucket check
				errMsg := fmt.Sprintf("Skipping source bucket check for spec %v since source node %v is not accessible. err=%v", spec.Id, local_connStr, err)
				service.logger.Warn(errMsg)
				err = errors.New(errMsg)
				return err, err
			}
		} else {
			errMsg := fmt.Sprintf("Skipping source bucket check for spec %v since failed to get bucket infor for %v. err=%v", spec.Id, spec.SourceBucketName, err_source)
			service.logger.Warn(errMsg)
			err := errors.New(errMsg)
			return err, err
		}
	}

	if spec.SourceBucketUUID != "" && sourceBucketUuid != "" && spec.SourceBucketUUID != sourceBucketUuid {
		errMsg := fmt.Sprintf("spec %v refers to bucket %v which was deleted and recreated", spec.Id, spec.SourceBucketName)
		service.logger.Error(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}

	return nil, nil
}

func (service *ReplicationSpecService) ValidateAndGC(spec *metadata.ReplicationSpecification) {
	err, detail_err := service.validateExistingReplicationSpec(spec)

	if err == InvalidReplicationSpecError {
		service.logger.Errorf("Replication specification %v is no longer valid, garbage collect it. error=%v\n", spec.Id, detail_err)
		_, err1 := service.DelReplicationSpecWithReason(spec.Id, detail_err.Error())
		if err1 != nil {
			service.logger.Infof("Failed to garbage collect spec %v, err=%v\n", spec.Id, err1)
		}
	}
}

func (service *ReplicationSpecService) sourceBucketUUID(bucketName string) (string, error) {
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		return "", errors.New("XDCRTopologySvc.MyConnectionStr() returned empty string")
	}
	return service.utils.LocalBucketUUID(local_connStr, bucketName, service.logger)
}

// this method is currently used for testing only. it is not kept up to date and should not be used in production
func (service *ReplicationSpecService) targetBucketUUID(targetClusterUUID, bucketName string) (string, error) {
	ref, err_target := service.remote_cluster_svc.RemoteClusterByUuid(targetClusterUUID, false)
	if err_target != nil {
		return "", err_target
	}
	remote_connStr, err_target := ref.MyConnectionStr()
	if err_target != nil {
		return "", err_target
	}
	remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err_target := ref.MyCredentials()
	if err_target != nil {
		return "", err_target
	}

	return service.utils.BucketUUID(remote_connStr, bucketName, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
}

// used by unit test only. does not use https and is not of production quality
func (service *ReplicationSpecService) ConstructNewReplicationSpec(sourceBucketName, targetClusterUUID, targetBucketName string) (*metadata.ReplicationSpecification, error) {
	sourceBucketUUID, err := service.sourceBucketUUID(sourceBucketName)
	if err != nil {
		return nil, err
	}

	targetBucketUUID, err := service.targetBucketUUID(targetClusterUUID, targetBucketName)
	if err != nil {
		return nil, err
	}

	return metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID, targetBucketName, targetBucketUUID)
}

func (service *ReplicationSpecService) cacheSpec(cache *MetadataCache, specId string, spec *metadata.ReplicationSpecification) error {
	var cachedObj *ReplicationSpecVal = nil
	var updatedCachedObj *ReplicationSpecVal = nil
	var ok1 bool
	cachedVal, ok := cache.Get(specId)
	if ok && cachedVal != nil {
		cachedObj, ok1 = cachedVal.(*ReplicationSpecVal)
		if !ok1 || cachedObj == nil {
			panic("Object in ReplicationSpecServcie cache is not of type *replicationSpecVal")
		}
		updatedCachedObj = &ReplicationSpecVal{
			spec:       spec,
			derivedObj: cachedObj.derivedObj,
			cas:        cachedObj.cas}
	} else {
		//never being cached before
		updatedCachedObj = &ReplicationSpecVal{spec: spec}
	}
	return cache.Upsert(specId, updatedCachedObj)
}

func (service *ReplicationSpecService) SetDerivedObj(specId string, derivedObj interface{}) error {
	var err error
	setDerivedObjFunc := func() error {
		err = service.setDerivedObjInner(specId, derivedObj)
		if err == service_def.MetadataNotFoundErr {
			return nil
		} else {
			return err
		}
	}

	expOpErr := service.utils.ExponentialBackoffExecutor("ReplSpecSvc.SetDerivedObj", base.RetryIntervalSetDerivedObj, base.MaxNumOfRetriesSetDerivedObj,
		base.MetaKvBackoffFactor, setDerivedObjFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		service.logger.Errorf("SetDerivedObj for %v failed after max retry. err=%v", specId, err)
		err = expOpErr
	}

	return err
}

func (service *ReplicationSpecService) setDerivedObjInner(specId string, derivedObj interface{}) error {
	cache := service.getCache()

	cachedVal, ok := cache.Get(specId)
	if !ok || cachedVal == nil {
		return fmt.Errorf(ReplicationSpecNotFoundErrorMessage)
	}
	cachedObj, ok := cachedVal.(*ReplicationSpecVal)
	if !ok {
		panic("Object in ReplicationSpecServcie cache is not of type *replciationSpecVal")
	}

	if cachedObj.spec == nil && derivedObj == nil {
		//remove it from the cache
		service.logger.Infof("Remove spec %v from the cache\n", specId)
		cache.Delete(specId)
	} else {
		updatedCachedObj := &ReplicationSpecVal{
			spec:       cachedObj.spec,
			derivedObj: derivedObj,
			cas:        cachedObj.cas}
		err := cache.Upsert(specId, updatedCachedObj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (service *ReplicationSpecService) GetDerivedObj(specId string) (interface{}, error) {
	cachedVal, ok := service.getCache().Get(specId)
	if !ok || cachedVal == nil {
		return nil, fmt.Errorf(ReplicationSpecNotFoundErrorMessage)
	}

	cachedObj, ok := cachedVal.(*ReplicationSpecVal)
	if !ok || cachedObj == nil {
		panic("Object in ReplicationSpecServcie cache is not of type *replciationSpecVal")
	}
	return cachedObj.derivedObj, nil
}
