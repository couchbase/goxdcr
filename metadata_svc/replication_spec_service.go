// Copyright (c) 2013-2019 Couchbase, Inc.
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
	"strings"
	"sync"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
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

type specGCMap map[string]int

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

	// Replication Spec GC Counters
	gcMtx    sync.Mutex
	srcGcMap specGCMap
	tgtGcMap specGCMap
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
		srcGcMap:               make(specGCMap),
		tgtGcMap:               make(specGCMap),
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
func (service *ReplicationSpecService) validateSourceBucket(errorMap base.ErrorMap, sourceBucket, targetCluster, targetBucket string) (string, int, string, error) {
	var err_source error
	start_time := time.Now()
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		return "", 0, "", errors.New("XDCRTopologySvc.MyConnectionStr() returned empty string")
	}

	_, sourceBucketType, sourceBucketUUID, sourceConflictResolutionType, sourceEvictionPolicy, sourceBucketKVVBMap, err_source := service.utils.BucketValidationInfo(local_connStr, sourceBucket, "", "", base.HttpAuthMechPlain, nil, false, nil, nil, service.logger)
	service.logger.Infof("Result from local bucket look up: bucketName=%v, err_source=%v, time taken=%v\n", sourceBucket, err_source, time.Since(start_time))
	service.validateBucket(sourceBucket, targetCluster, targetBucket, sourceBucketType, sourceEvictionPolicy, err_source, errorMap, true)

	return sourceBucketUUID, base.GetNumberOfVbs(sourceBucketKVVBMap), sourceConflictResolutionType, nil
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

		if sourceClusterUuid == targetClusterRef.Uuid() {
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
	repId := metadata.ReplicationId(sourceBucket, targetClusterRef.Uuid(), targetBucket)
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
		username = targetClusterRef.UserName()
		password = targetClusterRef.Password()
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

	sourceBucketUUID, sourceBucketNumberOfVbs, sourceConflictResolutionType, err := service.validateSourceBucket(errorMap, sourceBucket, targetCluster, targetBucket)
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

	useExternal, err := service.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
	if err != nil {
		return "", "", nil, errorMap, err, nil
	}

	targetBucketInfo, targetBucketUUID, targetBucketNumberOfVbs, targetConflictResolutionType, targetKVVBMap := service.validateTargetBucket(errorMap, remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, sourceBucket, targetCluster, useExternal)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	if sourceBucketNumberOfVbs != targetBucketNumberOfVbs {
		errMsg := fmt.Sprintf("The number of vbuckets in source cluster, %v, and target cluster, %v, does not match. This configuration is not supported.", sourceBucketNumberOfVbs, targetBucketNumberOfVbs)
		service.logger.Error(errMsg)
		errorMap[base.ToBucket] = errors.New(errMsg)
		return "", "", nil, errorMap, nil, nil
	}

	err, warnings := service.validateReplicationSettingsInternal(errorMap, sourceBucket, targetCluster, targetBucket, settings,
		targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate,
		clientCertificate, clientKey, targetKVVBMap, targetBucketInfo, true /*newSetting*/)
	if len(errorMap) > 0 || err != nil {
		return "", "", nil, errorMap, err, nil
	}

	repl_type, ok := settings[metadata.ReplicationTypeKey]
	if !ok {
		repl_type = metadata.ReplicationTypeXmem
	}

	esWarnings := service.validateES(errorMap, targetClusterRef, targetBucketInfo, repl_type, sourceConflictResolutionType, targetConflictResolutionType)
	if len(errorMap) > 0 {
		return "", "", nil, errorMap, nil, nil
	}

	base.ConcatenateStringSlices(warnings, esWarnings)

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

	useExternal, err := service.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
	if err != nil {
		return nil, err
	}

	targetBucketInfo, _, _, _, _, targetKVVBMap, err := service.utils.RemoteBucketValidationInfo(remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger, useExternal)
	if err != nil {
		return nil, err
	}

	err, _ = service.validateReplicationSettingsInternal(errorMap, sourceBucket, targetCluster, targetBucket, settings, targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, targetKVVBMap, targetBucketInfo, false /*new*/)
	return errorMap, err
}

func (service *ReplicationSpecService) validateReplicationSettingsInternal(errorMap base.ErrorMap, sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap,
	targetClusterRef *metadata.RemoteClusterReference, remote_connStr, remote_userName, remote_password string, httpAuthMech base.HttpAuthMech, certificate []byte, sanInCertificate bool,
	clientCertificate, clientKey []byte, targetKVVBMap map[string][]uint16, targetBucketInfo map[string]interface{}, newSettings bool) (error, []string) {
	var populateErr error
	var err error
	var warnings []string

	allKvConnStrs, username, password, populateErr := service.populateConnectionCreds(targetClusterRef, targetKVVBMap, targetBucket, targetBucketInfo)
	if populateErr != nil {
		return populateErr, warnings
	}
	isEnterprise, _ := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()

	repl_type, ok := settings[metadata.ReplicationTypeKey]
	compressionType, compressionOk := settings[metadata.CompressionTypeKey]
	if !compressionOk {
		if isEnterprise {
			compressionType = metadata.DefaultReplicationSettings().CompressionType
			settings[metadata.CompressionTypeKey] = compressionType
		} else {
			compressionType = base.CompressionTypeNone
		}
	}

	if filter, ok := settings[metadata.FilterExpressionKey].(string); ok {
		// Validate filter if it's a new setting OR it's an existing adv filter
		if version, ok := settings[metadata.FilterVersionKey]; newSettings || (ok && (version.(base.FilterVersionType) == base.FilterVersionAdvanced)) {
			err = base.ValidateAdvFilter(filter)
			if err != nil {
				return err, warnings
			}
		}
	}

	if !ok || repl_type == metadata.ReplicationTypeXmem {
		service.validateXmemSettings(errorMap, targetClusterRef, targetKVVBMap, sourceBucket, targetBucket, targetBucketInfo, allKvConnStrs[0], username, password)
		if len(errorMap) > 0 {
			return nil, warnings
		}

		// Compression is only allowed in XMEM mode
		if compressionType != base.CompressionTypeNone {
			err = service.validateCompression(errorMap, sourceBucket, targetClusterRef, targetKVVBMap, targetBucket, targetBucketInfo, compressionType.(int), allKvConnStrs, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey)
			if len(errorMap) > 0 || err != nil {
				if compressionType == base.CompressionTypeAuto && isEnterprise {
					// For enterprise version, if target doesn't support the compression setting but AUTO is set, let the replication creation/change through
					// because Pipeline Manager will be able to detect this and then handle it
					warning := fmt.Sprintf("Compression pre-requisite to cluster %v check failed. Compression will be temporarily disabled for replication.", targetClusterRef.Name())
					warnings = append(warnings, warning)
					// Since we are disabling compression, reset errors
					for k, _ := range errorMap {
						delete(errorMap, k)
					}
					err = nil
				} else {
					return err, warnings
				}
			}
		}
	} else {
		// CAPI mode
		if compressionType == base.CompressionTypeAuto {
			warning := fmt.Sprintf("Compression is disabled automatically for XDCR Version 1 (CAPI Protocol)")
			warnings = append(warnings, warning)
		} else if compressionType != base.CompressionTypeNone {
			errorMap[base.CompressionTypeREST] = fmt.Errorf("Compression feature is incompatible with XDCR Version 1 (CAPI Protocol)")
			return nil, warnings
		}
	}

	return nil, warnings
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
		useExternal, err := service.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
		if err != nil {
			return err
		}
		sslPortMap, err = service.utils.GetMemcachedSSLPortMap(connStr, username, password, httpAuthMech, certificate, SANInCertificate, clientCertificate, clientKey, targetBucket, service.logger, useExternal)
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
	sourceBucket string, targetCluster string, useExternal bool) (targetBucketInfo map[string]interface{}, targetBucketUUID string, targetBucketNumberOfVBs int, targetConflictResolutionType string, targetKVVBMap map[string][]uint16) {
	start_time := time.Now()

	targetBucketInfo, targetBucketType, targetBucketUUID, targetConflictResolutionType, _, targetKVVBMap, err_target := service.utils.RemoteBucketValidationInfo(remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger, useExternal)
	service.logger.Infof("Result from remote bucket look up: connStr=%v, bucketName=%v, targetBucketType=%v, err_target=%v, time taken=%v\n", remote_connStr, targetBucket, targetBucketType, err_target, time.Since(start_time))

	service.validateBucket(sourceBucket, targetCluster, targetBucket, targetBucketType, "", err_target, errorMap, false)

	return targetBucketInfo, targetBucketUUID, base.GetNumberOfVbs(targetKVVBMap), targetConflictResolutionType, targetKVVBMap
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
	} else if bucketType == base.EphemeralBucketType && !isEnterprise {
		errMsg := fmt.Sprintf("XDCR replication for ephemeral bucket '%v' is available only on Enterprise Edition", bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	}
}

func (service *ReplicationSpecService) AddReplicationSpec(spec *metadata.ReplicationSpecification, additionalInfo string) error {
	if spec == nil {
		service.logger.Errorf("Start AddReplicationSpec has nil spec")
		return base.ErrorInvalidInput
	}
	service.logger.Infof("Start AddReplicationSpec, spec=%v, additionalInfo=%v\n", spec.String(), additionalInfo)

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
	return service.setReplicationSpecInternal(spec, true /*lock*/)
}

func (service *ReplicationSpecService) setReplicationSpecInternal(spec *metadata.ReplicationSpecification, lock bool) error {
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

	err = service.updateCacheInternal(spec.Id, spec, lock)
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
		replSpec, err := service.constructReplicationSpec(marshalledSpec, rev, false /*lock*/)
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

// Note, this method returns spec instead of spec.Clone() to avoid generating too much garbage.
// This method should be used when:
// 1. The method is called frequently
// and 2. The specs returned are NEVER modified
// With the above restriction, there should not be concurrent access issues
// since this service performs copy on write and never modifies the specs either.
func (service *ReplicationSpecService) AllActiveReplicationSpecsReadOnly() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		spec := val.(*ReplicationSpecVal).spec
		if spec != nil && spec.Settings.Active {
			specs[key] = spec
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

func (service *ReplicationSpecService) AllReplicationSpecIdsForTargetBucket(bucket string) ([]string, error) {
	var repIds []string
	allRepIds, err := service.AllReplicationSpecIds()
	if err != nil {
		return nil, err
	}

	for _, repId := range allRepIds {
		// there should not be any errors since allRepIds should contain valid replication ids
		match, _ := metadata.IsReplicationIdForTargetBucket(repId, bucket)
		if match {
			repIds = append(repIds, repId)
		}
	}
	return repIds, nil
}

func (service *ReplicationSpecService) AllReplicationSpecsWithRemote(remoteClusterRef *metadata.RemoteClusterReference) (list []*metadata.ReplicationSpecification, err error) {
	if remoteClusterRef == nil {
		err = base.ErrorInvalidInput
		return
	}

	specsROMap, err := service.AllActiveReplicationSpecsReadOnly()
	if err != nil {
		service.logger.Warnf("Error retrieving all active replication specs: %v", err)
		return
	}

	for _, roVal := range specsROMap {
		if roVal.TargetClusterUUID == remoteClusterRef.Uuid() {
			list = append(list, roVal.Clone())
		}
	}
	return
}

func (service *ReplicationSpecService) removeSpecFromCache(specId string) error {
	//soft remove it from cache by setting SpecVal.spec = nil, but keep the key there
	//so that the derived object can still be retrieved and be acted on for cleaning-up.
	cache := service.getCache()
	val, ok := cache.Get(specId)
	if ok && val != nil {
		specVal, ok1 := val.(*ReplicationSpecVal)
		if ok1 {
			updatedCachedObj := &ReplicationSpecVal{
				spec:       nil,
				derivedObj: specVal.derivedObj,
				cas:        specVal.cas}
			return cache.Upsert(specId, updatedCachedObj)
		}
	}

	return nil
}

func (service *ReplicationSpecService) constructReplicationSpec(value []byte, rev interface{}, lock bool) (*metadata.ReplicationSpecification, error) {
	if value == nil {
		return nil, nil
	}

	spec := &metadata.ReplicationSpecification{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}
	spec.Revision = rev

	spec.Settings.PostProcessAfterUnmarshalling()

	service.handleSettingsUpgrade(spec, lock)

	return spec, nil
}

// spec.Settings needs to have all setting values populated,
// otherwise, replication behavior may change unexpectedly after upgrade when there are changes to default setting values.
// When new replication spec is created, spec.Settings always has all applicable setting values populated.
// The only scenario where values may be missing from spec.Settings is when new setting keys are added after upgrade.
// This method checks whether there are values missing from spec.Settings.
// If there are, it populates these values using default values, and writes updated settings/spec to metakv.
func (service *ReplicationSpecService) handleSettingsUpgrade(spec *metadata.ReplicationSpecification, lock bool) {
	updatedKeys := spec.Settings.PopulateDefault()
	isEnterprise, _ := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()
	if !isEnterprise && spec.Settings.Values[metadata.CompressionTypeKey] == base.CompressionTypeAuto {
		spec.Settings.Values[metadata.CompressionTypeKey] = base.CompressionTypeNone
		updatedKeys = append(updatedKeys, metadata.CompressionTypeKey)
	} else if spec.Settings.Values[metadata.CompressionTypeKey] == base.CompressionTypeSnappy {
		spec.Settings.Values[metadata.CompressionTypeKey] = base.CompressionTypeAuto
		updatedKeys = append(updatedKeys, metadata.CompressionTypeKey)
	}
	if len(updatedKeys) == 0 {
		return
	}

	service.logger.Infof("Updating spec %v in metakv since its settings has been upgraded. upgraded settings keys = %v", spec.Id, updatedKeys)
	err := service.setReplicationSpecInternal(spec, lock)
	if err != nil {
		// ignore this error since it is not fatal.
		// 1. updated settings will be saved to metakv if there are further updates to spec
		// 2. if updated settings are not saved to metakv, there is no impact if default values of new settings are not changed
		// 3. in the worst case that updated settings are not saved to metakv, and default values of new settings are changed,
		//    the old default values will be used, which is not desirable but will not cause production downtime
		// not to ignore this error would be more risky, since some critical ops, like responding to metakv updates,
		// may get skipped
		service.logger.Warnf("Error upgrading replication settings in spec %v in metakv. err = %v\n", spec.Id, err)
	}
}

// Implement callback function for metakv
func (service *ReplicationSpecService) ReplicationSpecServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("ReplicationSpecServiceCallback called on path = %v\n", path)

	newSpec, err := service.constructReplicationSpec(value, rev, true /*lock*/)
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
	return service.updateCacheInternal(specId, newSpec, true /*lock*/)
}

func (service *ReplicationSpecService) updateCacheNoLock(specId string, newSpec *metadata.ReplicationSpecification) error {
	return service.updateCacheInternal(specId, newSpec, false /*lock*/)
}

func (service *ReplicationSpecService) updateCacheInternal(specId string, newSpec *metadata.ReplicationSpecification, lock bool) error {
	if lock {
		service.cache_lock.Lock()
		defer service.cache_lock.Unlock()
	}

	oldSpec, updated, err := service.updateCacheInternalNoLock(specId, newSpec)

	if updated && oldSpec != nil && newSpec == nil {
		service.gcMtx.Lock()
		delete(service.srcGcMap, oldSpec.Id)
		delete(service.tgtGcMap, oldSpec.Id)
		service.gcMtx.Unlock()
		service.remote_cluster_svc.UnRequestRemoteMonitoring(oldSpec)
	}

	if updated && oldSpec == nil && newSpec != nil {
		service.remote_cluster_svc.RequestRemoteMonitoring(newSpec)
	}

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

func getBucketMissingError(bucketName string) error {
	return fmt.Errorf("Bucket %v has been missing for %v times", bucketName, base.ReplicationSpecGCCnt)
}

func getBucketChangedError(bucketName, origUUID, newUUID string) error {
	return fmt.Errorf("Bucket %v UUID has changed from %v to %v, indicating a bucket deletion and recreation", bucketName, origUUID, newUUID)
}

// Returns true if GC count hits the max
func (service *ReplicationSpecService) incrementGCCnt(gcMap specGCMap, specId string) bool {
	service.gcMtx.Lock()
	defer service.gcMtx.Unlock()
	gcMap[specId]++
	if gcMap[specId] >= base.ReplicationSpecGCCnt {
		return true
	}
	return false
}

// Returns true if need to delete the spec, and a non-nil error along with it for the reason why
func (service *ReplicationSpecService) gcTargetBucket(spec *metadata.ReplicationSpecification) (bool, error) {
	if spec == nil {
		return false, base.ErrorInvalidInput
	}

	ref, err := service.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false /*refresh*/)
	if err != nil {
		service.logger.Warnf("Unable to retrieve reference from spec %v due to %v", spec.Id, err.Error())
		return false, err
	}

	err = service.utils.VerifyTargetBucket(spec.TargetBucketName, spec.TargetBucketUUID, ref, service.logger)
	if err == nil {
		service.gcMtx.Lock()
		service.tgtGcMap[spec.Id] = 0
		service.gcMtx.Unlock()
		return false, nil
	}

	service.logger.Warnf("Error verifying target bucket for %v. err=%v", spec.Id, err)

	if err == service.utils.GetNonExistentBucketError() {
		shouldDel := service.incrementGCCnt(service.tgtGcMap, spec.Id)
		if shouldDel {
			err = getBucketMissingError(spec.TargetBucketName)
			return shouldDel, err
		} else {
			return false, err
		}
	} else if err == service.utils.GetBucketRecreatedError() {
		return true, err
	} else {
		return false, err
	}
}

// Returns true if need to delete the spec, and a non-nil error along with it for the reason why
func (service *ReplicationSpecService) gcSourceBucket(spec *metadata.ReplicationSpecification) (bool, error) {
	if spec == nil {
		return false, base.ErrorInvalidInput
	}

	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		err := fmt.Errorf("XDCRTopologySvc.MyConnectionStr() returned empty string when validating spec %v", spec.Id)
		return false, err
	}

	username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, err := service.xdcr_comp_topology_svc.MyCredentials()
	if err != nil {
		service.logger.Warnf("Unable to retrieve credentials due to %v", err.Error())
		return false, err
	}

	bucketInfo, err := service.utils.GetBucketInfo(local_connStr, spec.SourceBucketName, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
	if err == service.utils.GetNonExistentBucketError() {
		shouldDel := service.incrementGCCnt(service.srcGcMap, spec.Id)
		if shouldDel {
			err = getBucketMissingError(spec.SourceBucketName)
			return shouldDel, err
		}
	} else {
		service.gcMtx.Lock()
		service.srcGcMap[spec.Id] = 0
		service.gcMtx.Unlock()
	}
	if err != nil {
		service.logger.Warnf("Unable to retrieve bucket info for source bucket %v due to %v", spec.SourceBucketName, err.Error())
		return false, err
	}

	if spec.SourceBucketUUID != "" {
		extractedUuid, err := service.utils.GetBucketUuidFromBucketInfo(spec.SourceBucketName, bucketInfo, service.logger)
		if err != nil {
			service.logger.Warnf("Unable to check source bucket %v UUID due to %v", spec.SourceBucketName, err.Error())
			return false, err
		}

		if extractedUuid != spec.SourceBucketUUID {
			return true, getBucketChangedError(spec.SourceBucketName, spec.SourceBucketUUID, extractedUuid)
		}
	}
	return false, nil
}

func (service *ReplicationSpecService) ValidateAndGC(spec *metadata.ReplicationSpecification) {
	needToRemove, detailErr := service.gcSourceBucket(spec)
	if !needToRemove {
		needToRemove, detailErr = service.gcTargetBucket(spec)
	}

	if detailErr != nil {
		service.logger.Warnf("Error validating replication specification %v. error=%v\n", spec.Id, detailErr)
	}

	if needToRemove {
		service.logger.Errorf("Replication specification %v is no longer valid, garbage collect it. error=%v\n", spec.Id, detailErr)
		_, err1 := service.DelReplicationSpecWithReason(spec.Id, detailErr.Error())
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
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

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
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

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
