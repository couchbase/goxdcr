// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// metadata service implementation leveraging gometa
package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

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
var InvalidReplicationSpecError = errors.New("Invalid Replication spec")

// replication spec and its derived object
// This is what is put into the cache
type ReplicationSpecVal struct {
	spec       metadata.GenericSpecification
	derivedObj interface{}
	cas        int64
}

func (rsv *ReplicationSpecVal) CAS(obj CacheableMetadataObj) bool {
	if rsv == nil || obj == nil {
		return true
	} else if rsv2, ok := obj.(*ReplicationSpecVal); ok {
		if rsv.cas == rsv2.cas {
			genSpec, ok2 := rsv.spec.(metadata.GenericSpecification)
			genSpec2, ok3 := rsv2.spec.(metadata.GenericSpecification)
			if ok2 && ok3 && !genSpec.SameSpecGeneric(genSpec2) {
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
			clonedRsv.spec = rsv.spec.CloneGeneric()
		}
		return clonedRsv
	}
	return rsv
}

func (rsv *ReplicationSpecVal) Redact() CacheableMetadataObj {
	if rsv != nil && rsv.spec != nil {
		rsv.spec.RedactGeneric()
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

type callbackOp int

const (
	addOp callbackOp = iota
	delOp callbackOp = iota
	modOp callbackOp = iota
)

type MetadataChangeHandlerCallback struct {
	cb          base.MetadataChangeHandlerCallbackWithWg
	addPriority base.MetadataChangeHandlerPriority
	delPriority base.MetadataChangeHandlerPriority
	modPriority base.MetadataChangeHandlerPriority
}

func (m *MetadataChangeHandlerCallback) GetPriority(op callbackOp) base.MetadataChangeHandlerPriority {
	switch op {
	case addOp:
		return m.addPriority
	case delOp:
		return m.delPriority
	case modOp:
		return m.modPriority
	default:
		panic("Need to implement")
	}
}

func NewMetadataChangeHandlerCallback(cb base.MetadataChangeHandlerCallbackWithWg, addPriority, delPriority, modPriority base.MetadataChangeHandlerPriority) MetadataChangeHandlerCallback {
	return MetadataChangeHandlerCallback{
		cb:          cb,
		addPriority: addPriority,
		delPriority: delPriority,
		modPriority: modPriority,
	}
}

type ReplicationSpecService struct {
	xdcr_comp_topology_svc service_def.XDCRCompTopologySvc
	metadata_svc           service_def.MetadataSvc
	uilog_svc              service_def.UILogSvc
	remote_cluster_svc     service_def.RemoteClusterSvc
	resolver_svc           service_def.ResolverSvcIface
	replicationSettingSvc  service_def.ReplicationSettingsSvc

	cache                  *MetadataCache
	cache_lock             *sync.Mutex // Lock when update status, update spec or delete a pipeline.
	logger                 *log.CommonLogger
	metadataChangeCallback []MetadataChangeHandlerCallback
	cbIDs                  []string
	metadataChangeMtx      sync.RWMutex
	utils                  utilities.UtilsIface

	// Replication Spec GC Counters
	gcMtx    sync.Mutex
	srcGcMap specGCMap
	tgtGcMap specGCMap

	// Request Remote Buckets background retry tasks
	rcBgReqMtx      sync.Mutex
	rcBgReqFinchMap map[string]chan bool
	rcBgReqSyncMap  map[string]bool

	manifestsGetterMtx sync.RWMutex
	manifestsGetter    service_def.ManifestsGetter
	manifestsGetterSet uint32
}

func NewReplicationSpecService(uilog_svc service_def.UILogSvc, remote_cluster_svc service_def.RemoteClusterSvc, metadata_svc service_def.MetadataSvc, xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, resolver_svc service_def.ResolverSvcIface, logger_ctx *log.LoggerContext, utilities_in utilities.UtilsIface, replicationSettingsSvc service_def.ReplicationSettingsSvc) (*ReplicationSpecService, error) {
	logger := log.NewLogger("ReplSpecSvc", logger_ctx)
	svc := &ReplicationSpecService{
		metadata_svc:           metadata_svc,
		uilog_svc:              uilog_svc,
		remote_cluster_svc:     remote_cluster_svc,
		xdcr_comp_topology_svc: xdcr_comp_topology_svc,
		resolver_svc:           resolver_svc,
		cache:                  nil,
		cache_lock:             &sync.Mutex{},
		logger:                 logger,
		utils:                  utilities_in,
		srcGcMap:               make(specGCMap),
		tgtGcMap:               make(specGCMap),
		rcBgReqFinchMap:        make(map[string]chan bool),
		rcBgReqSyncMap:         make(map[string]bool),
		replicationSettingSvc:  replicationSettingsSvc,
	}

	return svc, svc.initCacheFromMetaKV()
}

func (service *ReplicationSpecService) SetMetadataChangeHandlerCallback(id string, callBack base.MetadataChangeHandlerCallbackWithWg, add base.MetadataChangeHandlerPriority, del base.MetadataChangeHandlerPriority, mod base.MetadataChangeHandlerPriority) {
	service.metadataChangeMtx.Lock()
	defer service.metadataChangeMtx.Unlock()
	service.metadataChangeCallback = append(service.metadataChangeCallback, NewMetadataChangeHandlerCallback(callBack, add, del, mod))
	service.cbIDs = append(service.cbIDs, id)
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

func (service *ReplicationSpecService) ReplicationSpecReadOnly(replicationId string) (*metadata.ReplicationSpecification, error) {
	spec, err := service.replicationSpec(replicationId)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

// this method is cheaper than ReplicationSpec() and should be called only when the spec returned won't be modified or that the modifications do not matter.
func (service *ReplicationSpecService) replicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	val, ok := service.getCache().Get(replicationId)
	if !ok {
		return nil, base.ReplNotFoundErr
	}
	replSpecVal, ok := val.(*ReplicationSpecVal)
	if !ok || replSpecVal == nil {
		return nil, base.ReplNotFoundErr
	}
	replSpec, ok := replSpecVal.spec.(*metadata.ReplicationSpecification)
	if !ok || replSpec == nil {
		return nil, base.ReplNotFoundErr
	}
	return replSpec, nil
}

// validate the existence of source bucket
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
func (service *ReplicationSpecService) validateSrcTargetNonIdenticalBucket(errorMap base.ErrorMap, sourceBucket, targetBucket string, targetClusterRef *metadata.RemoteClusterReference, sourceClusterUuid string) error {
	if sourceBucket == targetBucket {
		if sourceClusterUuid == targetClusterRef.Uuid() {
			errorMap[base.PlaceHolderFieldKey] = errors.New("Replication from a bucket to the same bucket is not allowed")
		} else {
			service.logger.Infof("Validated that source bucket and target bucket are not the same")
		}
	}
	return nil
}

// validate remote cluster ref
func (service *ReplicationSpecService) getRemoteReference(errorMap base.ErrorMap, targetCluster string) (*metadata.RemoteClusterReference, string, string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, metadata.Capability) {
	var remote_userName, remote_password string
	var httpAuthMech base.HttpAuthMech
	var certificate []byte
	var sanInCertificate bool
	var clientCertificate []byte
	var clientKey []byte
	var err error
	var remote_connStr string
	var emptyCapability metadata.Capability
	start_time := time.Now()
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByRefName(targetCluster, false)
	if err == RefreshNotEnabledYet {
		// If refresh is not enabled yet, this means that there is a refresh ongoing. Set refresh to true to piggy-back off of the current refresh
		targetClusterRef, err = service.remote_cluster_svc.RemoteClusterByRefName(targetCluster, true)
	}
	if err != nil {
		errorMap[base.ToCluster] = service.utils.NewEnhancedError("cannot find remote cluster", err)
		return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, emptyCapability
	}

	service.logger.Infof("Successfully retrieved target cluster reference %v. time taken=%v\n", targetClusterRef.Name, time.Since(start_time))
	remote_connStr, err = targetClusterRef.MyConnectionStr()
	if err != nil {
		errorMap[base.ToCluster] = service.utils.NewEnhancedError("Invalid remote cluster. MyConnectionStr() failed.", err)
		return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, emptyCapability
	}
	remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err = targetClusterRef.MyCredentials()
	if err != nil {
		errorMap[base.ToCluster] = service.utils.NewEnhancedError("Invalid remote cluster. MyCredentials() failed.", err)
		return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, emptyCapability
	}
	rcCapability, err := service.remote_cluster_svc.GetCapability(targetClusterRef)
	if err != nil {
		errorMap[base.ToCluster] = service.utils.NewEnhancedError("Invalid remote cluster. Unable to determine remote cluster capability", err)
		return nil, "", remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, emptyCapability
	}

	return targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, rcCapability
}

func (service *ReplicationSpecService) validateReplicationSpecDoesNotAlreadyExist(errorMap base.ErrorMap, sourceBucket string, targetClusterRef *metadata.RemoteClusterReference, targetBucket string) {
	stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("validateReplicationSpecDoesNotAlreadyExist(%v, _, %v)", sourceBucket, targetBucket), base.DiagInternalThreshold)
	defer stopFunc()
	repId := metadata.ReplicationId(sourceBucket, targetClusterRef.Uuid(), targetBucket)
	_, err := service.replicationSpec(repId)
	if err == nil {
		errorMap[base.PlaceHolderFieldKey] = errors.New(ReplicationSpecAlreadyExistErrorMessage)
	}
}

func (service *ReplicationSpecService) populateConnectionCreds(targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16) (allKvConnStrs []string, username, password string, err error) {
	if len(targetKVVBMap) == 0 {
		err = errors.New("kv vb map is empty")
		return
	}

	stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("populateConnectionCreds(%v)", targetClusterRef.Name()), base.DiagInternalThreshold)
	stopFunc()

	// Pull all the kv connection strings to a slice
	i := 0
	allKvConnStrs = make([]string, len(targetKVVBMap), len(targetKVVBMap))
	for kvaddr, _ := range targetKVVBMap {
		allKvConnStrs[i] = kvaddr
		i++
	}

	username = targetClusterRef.UserName()
	password = targetClusterRef.Password()
	return
}

func (service *ReplicationSpecService) validateXmemSettings(errorMap base.ErrorMap, targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16, sourceBucket, targetBucket string, targetBucketInfo map[string]interface{}, kvConnStr, username, password string) {
	if targetClusterRef.IsHalfEncryption() {
		stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("validateXmemSettings(%v, %v, %v)", targetClusterRef.Name(), sourceBucket, targetBucket), base.DiagNetworkThreshold)
		defer stopFunc()
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

/**
 * Main Validation routine, supplemented by multiple helper sub-routines.
 * Each sub-routine may be daisy chained by variables that would be helpful for further subroutines.
 */
func (service *ReplicationSpecService) ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (string, string, *metadata.RemoteClusterReference, base.ErrorMap, error, service_def.UIWarnings, *metadata.CollectionsManifestPair) {
	errMap := make(base.ErrorMap)
	service.logger.Infof("Start ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v, performRPC=%v settings=%v", sourceBucket, targetCluster, targetBucket, performRemoteValidation, settings.CloneAndRedact())
	defer service.logger.Infof("Finished ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v, errorMap=%v, performRPC=%v settings=%v", sourceBucket, targetCluster, targetBucket, errMap, performRemoteValidation, settings.CloneAndRedact())

	stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("ValidateNewReplicationSpec(%v, %v, %v)", sourceBucket, targetCluster, targetBucket), base.DiagNetworkThreshold)
	defer stopFunc()

	var errMapMtx sync.Mutex
	var srcSideWaitGrpPhase1 sync.WaitGroup
	var tgtSideWaitGrpPhase1 sync.WaitGroup
	// Try to do things in parallel if possible

	var sourceBucketUUID string
	var sourceBucketNumberOfVbs int
	var sourceConflictResolutionType string
	var validateSourceBucketErr error
	validateSourceBucketErrMap := make(base.ErrorMap)
	var sourceClusterUuid string
	var sourceClusterUuidErr error
	var sourceCompat int
	var sourceCompatErr error

	// Phase 1 concurrent check
	srcSideWaitGrpPhase1.Add(1)
	go func() {
		defer srcSideWaitGrpPhase1.Done()
		sourceBucketUUID, sourceBucketNumberOfVbs, sourceConflictResolutionType, validateSourceBucketErr = service.validateSourceBucket(validateSourceBucketErrMap, sourceBucket, targetCluster, targetBucket)
		if len(validateSourceBucketErrMap) > 0 {
			errMapMtx.Lock()
			base.ConcatenateErrors(errMap, validateSourceBucketErrMap, math.MaxInt32, nil)
			errMapMtx.Unlock()
		}
	}()

	var myClusterUUIDWaitGrp sync.WaitGroup
	myClusterUUIDWaitGrp.Add(1)
	srcSideWaitGrpPhase1.Add(1)
	go func() {
		defer myClusterUUIDWaitGrp.Done()
		defer srcSideWaitGrpPhase1.Done()
		sourceClusterUuid, sourceClusterUuidErr = service.xdcr_comp_topology_svc.MyClusterUuid()
	}()

	var myCompatWaitGrp sync.WaitGroup
	myCompatWaitGrp.Add(1)
	srcSideWaitGrpPhase1.Add(1)
	go func() {
		defer myCompatWaitGrp.Done()
		defer srcSideWaitGrpPhase1.Done()
		sourceCompat, sourceCompatErr = service.xdcr_comp_topology_svc.MyClusterCompatibility()
	}()

	var targetClusterRef *metadata.RemoteClusterReference
	var remoteConnstr string
	var remoteUsername string
	var remotePassword string
	var httpAuthMech base.HttpAuthMech
	var certificate []byte
	var sanInCertificate bool
	var clientCertificate []byte
	var clientKey []byte
	getRemoteReferenceErrMap := make(base.ErrorMap)
	var validateNonIdenticalErr error
	var useExternal bool
	var shouldUseAlternateErr error
	var targetBucketInfo map[string]interface{}
	var targetBucketUUID string
	var targetBucketNumberOfVbs int
	var targetConflictResolutionType string
	var targetKVVBMap map[string][]uint16
	var rcCapability metadata.Capability
	var manifestsPair *metadata.CollectionsManifestPair

	tgtSideWaitGrpPhase1.Add(1)
	go func() {
		defer tgtSideWaitGrpPhase1.Done()
		targetClusterRef, remoteConnstr, remoteUsername, remotePassword, httpAuthMech, certificate, sanInCertificate,
			clientCertificate, clientKey, rcCapability = service.getRemoteReference(getRemoteReferenceErrMap, targetCluster)
		if len(getRemoteReferenceErrMap) > 0 {
			errMapMtx.Lock()
			base.ConcatenateErrors(errMap, getRemoteReferenceErrMap, math.MaxInt32, nil)
			errMapMtx.Unlock()
			return
		}

		myCompatWaitGrp.Wait()
		if sourceCompatErr != nil {
			myCompatErrMap := make(base.ErrorMap)
			errMapMtx.Lock()
			myCompatErrMap["MyClusterCompatibility"] = sourceCompatErr
			base.ConcatenateErrors(errMap, myCompatErrMap, math.MaxInt32, nil)
			errMapMtx.Unlock()
			return
		}

		// If source nodes all have upgraded to p2p + manifest sharing capability, it has collections
		// and source bucket manifests
		// If target cluster as a whole has collections support, the target bucket must have manifests to be retrieved
		getAndShareTgtBucketManifest := rcCapability.HasCollectionSupport() &&
			base.IsClusterCompatible(sourceCompat, base.VersionForP2PManifestSharing)

		var tgtSideWaitGrpPhase2 sync.WaitGroup
		tgtSideWaitGrpPhase2.Add(1)
		go func() {
			defer tgtSideWaitGrpPhase2.Done()
			useExternal, shouldUseAlternateErr = service.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
		}()

		var tgtSideWaitGrpPhase3 sync.WaitGroup
		validateReplDNEErrMap := make(base.ErrorMap)
		tgtSideWaitGrpPhase3.Add(1)
		go func() {
			defer tgtSideWaitGrpPhase3.Done()
			service.validateReplicationSpecDoesNotAlreadyExist(validateReplDNEErrMap, sourceBucket, targetClusterRef, targetBucket)
			if len(validateReplDNEErrMap) > 0 {
				errMapMtx.Lock()
				base.ConcatenateErrors(errMap, validateReplDNEErrMap, math.MaxInt32, nil)
				errMapMtx.Unlock()
			}
		}()

		validateNonIdenticalErrMap := make(base.ErrorMap)
		tgtSideWaitGrpPhase3.Add(1)
		go func() {
			defer tgtSideWaitGrpPhase3.Done()
			myClusterUUIDWaitGrp.Wait()
			if sourceClusterUuidErr != nil {
				// Don't even try the following
				errMapMtx.Lock()
				validateNonIdenticalErrMap["MyClusterUUID"] = sourceClusterUuidErr
				base.ConcatenateErrors(errMap, validateNonIdenticalErrMap, math.MaxInt32, nil)
				errMapMtx.Unlock()
				return
			}
			validateNonIdenticalErr = service.validateSrcTargetNonIdenticalBucket(validateNonIdenticalErrMap, sourceBucket, targetBucket, targetClusterRef, sourceClusterUuid)
			if len(validateNonIdenticalErrMap) > 0 {
				errMapMtx.Lock()
				base.ConcatenateErrors(errMap, validateNonIdenticalErrMap, math.MaxInt32, nil)
				errMapMtx.Unlock()
			}
		}()

		validateTargetBucketErrMap := make(base.ErrorMap)
		tgtSideWaitGrpPhase3.Add(1)
		go func() {
			defer tgtSideWaitGrpPhase3.Done()
			tgtSideWaitGrpPhase2.Wait()
			if shouldUseAlternateErr != nil {
				return
			}
			if performRemoteValidation {
				targetBucketInfo, targetBucketUUID, targetBucketNumberOfVbs, targetConflictResolutionType, targetKVVBMap = service.validateTargetBucket(validateTargetBucketErrMap, remoteConnstr, targetBucket, remoteUsername, remotePassword, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, sourceBucket, targetCluster, useExternal)
			}
			if len(validateTargetBucketErrMap) > 0 {
				errMapMtx.Lock()
				base.ConcatenateErrors(errMap, validateTargetBucketErrMap, math.MaxInt32, nil)
				errMapMtx.Unlock()
			}
		}()

		tgtSideWaitGrpPhase3.Add(1)
		go func() {
			defer tgtSideWaitGrpPhase3.Done()
			tgtSideWaitGrpPhase2.Wait()
			if shouldUseAlternateErr != nil {
				return
			}
			if !getAndShareTgtBucketManifest {
				// In the future, as more clusters upgrade, this case becomes less likely
				return
			}
			var err error
			manifestsPair, err = service.getManifests(sourceBucket, targetBucket, targetClusterRef)
			if err != nil {
				getManifestsErrMap := make(base.ErrorMap)
				getManifestsErrMap["getManifests"] = err
				errMapMtx.Lock()
				base.ConcatenateErrors(errMap, getManifestsErrMap, math.MaxInt32, nil)
				errMapMtx.Unlock()
				return
			}
		}()

		tgtSideWaitGrpPhase3.Wait()
	}()

	srcSideWaitGrpPhase1.Wait()
	tgtSideWaitGrpPhase1.Wait()

	if len(errMap) > 0 {
		return "", "", nil, errMap, nil, nil, nil
	}
	// Return errors in order
	if validateSourceBucketErr != nil {
		return "", "", nil, nil, validateSourceBucketErr, nil, nil
	}
	if sourceClusterUuidErr != nil {
		return "", "", nil, nil, sourceClusterUuidErr, nil, nil
	}
	if shouldUseAlternateErr != nil {
		return "", "", nil, nil, shouldUseAlternateErr, nil, nil
	}
	if validateNonIdenticalErr != nil {
		return "", "", nil, nil, validateNonIdenticalErr, nil, nil
	}

	if performRemoteValidation && sourceBucketNumberOfVbs != targetBucketNumberOfVbs {
		errMsg := fmt.Sprintf("The number of vbuckets in source cluster, %v, and target cluster, %v, does not match. This configuration is not supported.", sourceBucketNumberOfVbs, targetBucketNumberOfVbs)
		service.logger.Error(errMsg)
		errMap[base.ToBucket] = errors.New(errMsg)
		return "", "", nil, errMap, nil, nil, nil
	}

	err, warnings := service.validateReplicationSettingsInternal(errMap, sourceBucket, targetCluster, targetBucket, settings, targetClusterRef, remoteConnstr, remoteUsername, remotePassword, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, targetKVVBMap, targetBucketInfo, true, performRemoteValidation)
	if len(errMap) > 0 || err != nil {
		return "", "", nil, errMap, err, nil, nil
	}

	if performRemoteValidation && sourceConflictResolutionType != targetConflictResolutionType {
		errMap[base.PlaceHolderFieldKey] = errors.New("Replication between buckets with different ConflictResolutionType setting is not allowed")
		return "", "", nil, errMap, nil, nil, nil
	}

	if sourceConflictResolutionType == base.ConflictResolutionType_Custom {
		if _, ok := settings[metadata.MergeFunctionMappingKey]; !ok {
			errMsg := fmt.Sprintf("Replication setting %v is required for custom conflict resolution.", base.MergeFunctionMappingKey)
			service.logger.Errorf(errMsg)
			errMap[base.PlaceHolderFieldKey] = errors.New(errMsg)
			return "", "", nil, errMap, nil, nil, nil
		}
	}

	if warnings != nil {
		service.logger.Warnf("Warnings from ValidateAddReplicationSpec : %v\n", warnings.String())
	}
	return sourceBucketUUID, targetBucketUUID, targetClusterRef, errMap, nil, warnings, manifestsPair
}

func (service *ReplicationSpecService) ValidateReplicationSettings(sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, performRemoteValidation bool) (base.ErrorMap, error, service_def.UIWarnings) {
	var errorMap base.ErrorMap = make(base.ErrorMap)

	targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, _ := service.getRemoteReference(errorMap, targetCluster)
	if len(errorMap) > 0 {
		return errorMap, nil, nil
	}

	useExternal, err := service.remote_cluster_svc.ShouldUseAlternateAddress(targetClusterRef)
	if err != nil {
		return nil, err, nil
	}

	var targetBucketInfo map[string]interface{}
	var targetKVVBMap map[string][]uint16
	if performRemoteValidation {
		targetBucketInfo, _, _, _, _, targetKVVBMap, err = service.utils.RemoteBucketValidationInfo(remote_connStr, targetBucket, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger, useExternal)
		if err != nil {
			return nil, err, nil
		}
	}

	err, warnings := service.validateReplicationSettingsInternal(errorMap, sourceBucket, targetCluster, targetBucket, settings, targetClusterRef, remote_connStr, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, targetKVVBMap, targetBucketInfo, false, performRemoteValidation)
	return errorMap, err, warnings
}

func (service *ReplicationSpecService) validateReplicationSettingsInternal(errorMap base.ErrorMap, sourceBucket, targetCluster, targetBucket string, settings metadata.ReplicationSettingsMap, targetClusterRef *metadata.RemoteClusterReference, remote_connStr, remote_userName, remote_password string, httpAuthMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, targetKVVBMap map[string][]uint16, targetBucketInfo map[string]interface{}, newSettings, performTargetValidation bool) (error, service_def.UIWarnings) {
	var populateErr error
	var err error

	allKvConnStrs, username, password, populateErr := service.populateConnectionCreds(targetClusterRef, targetKVVBMap)
	if performTargetValidation && populateErr != nil {
		return populateErr, nil
	}

	isEnterprise, _ := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()

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
			// Ensure that all nodes in the source cluster can handle the advanced filter
			clusterCompat, err := service.xdcr_comp_topology_svc.MyClusterCompatibility()
			if err != nil {
				service.logger.Errorf("Unable to get local cluster compatibility as part of replSpec validation: %v", err)
				return err, nil
			}
			if !base.IsClusterCompatible(clusterCompat, base.VersionForAdvFilteringSupport) {
				return base.ErrorAdvFilterMixedModeUnsupported, nil
			}

			err = base.ValidateAdvFilter(filter)
			if err != nil {
				return err, nil
			}
		}
	}

	// If merge function is specified, check that it exists
	if mergeFunctions, ok := settings[base.MergeFunctionMappingKey].(base.MergeFunctionMappingType); ok {
		for _, f := range mergeFunctions {
			err = service.resolver_svc.CheckMergeFunction(f)
			if err != nil {
				if f != base.DefaultMergeFunc {
					errorMap[base.MergeFunctionMappingKey] = err
					return nil, nil
				} else {
					// Create the default merge function if it is not there
					service.resolver_svc.InitDefaultFunc()
				}
			}
		}
	}

	if mobile, ok := settings[metadata.MobileCompatibleKey].(int); ok {
		if mobile != base.MobileCompatibilityOff {
			// Mobile is on. Make sure source bucket enableCrossClusterVersioning is true.
			connstr, err := service.xdcr_comp_topology_svc.MyConnectionStr()
			if err != nil {
				return err, nil
			}
			username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, err := service.xdcr_comp_topology_svc.MyCredentials()
			if err != nil {
				service.logger.Warnf("Unable to retrieve credentials due to %v", err.Error())
				return err, nil
			}
			bucketInfo, err := service.utils.GetBucketInfo(connstr, sourceBucket, username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
			if err != nil {
				return err, nil
			}
			crossClusterVer, err := service.utils.GetCrossClusterVersioningFromBucketInfo(bucketInfo)
			if err != nil {
				return err, nil
			}
			if !crossClusterVer {
				return fmt.Errorf("mobile must be off when %v is false for the source bucket", base.EnableCrossClusterVersioningKey), nil
			}
		}
	}

	warnings := base.NewUIWarning()
	if performTargetValidation {
		service.validateXmemSettings(errorMap, targetClusterRef, targetKVVBMap, sourceBucket, targetBucket, targetBucketInfo, allKvConnStrs[0], username, password)
		if len(errorMap) > 0 {
			return nil, nil
		}

		// Compression is only allowed in XMEM mode
		if compressionType != base.CompressionTypeNone {
			err = service.validateCompression(errorMap, sourceBucket, targetClusterRef, targetKVVBMap, targetBucket, targetBucketInfo, compressionType.(int), allKvConnStrs, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey)
			if len(errorMap) > 0 || err != nil {
				if compressionType == base.CompressionTypeAuto && isEnterprise {
					// For enterprise version, if target doesn't support the compression setting but AUTO is set, let the replication creation/change through
					// because Pipeline Manager will be able to detect this and then handle it
					warning := fmt.Sprintf("Compression pre-requisite to cluster %v check failed. Compression will be temporarily disabled for replication.", targetClusterRef.Name())
					warnings.AppendGeneric(warning)
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
		if mobile, ok := settings[metadata.MobileCompatibleKey].(int); ok {
			if mobile != base.MobileCompatibilityOff {
				// Mobile is on. Make sure target bucket enableCrossClusterVersioning is true.
				crossClusterVer, err := service.utils.GetCrossClusterVersioningFromBucketInfo(targetBucketInfo)
				if err != nil {
					return err, nil
				}
				if !crossClusterVer {
					return fmt.Errorf("mobile must be off when %v is false for the target bucket", base.EnableCrossClusterVersioningKey), nil
				}
			}
		}
	}

	service.appendGoMaxProcsWarnings(sourceBucket, targetClusterRef, targetBucket, warnings, settings)
	return nil, warnings
}

// validate compression - should only be called if compression setting dictates that there is compression
func (service *ReplicationSpecService) validateCompression(errorMap base.ErrorMap, sourceBucket string, targetClusterRef *metadata.RemoteClusterReference, targetKVVBMap map[string][]uint16, targetBucket string, targetBucketInfo map[string]interface{}, compressionType int, allKvConnStrs []string, username, password string,
	httpAuthMech base.HttpAuthMech, certificate []byte, SANInCertificate bool, clientCertificate, clientKey []byte) error {
	var requestedFeaturesSet utilities.HELOFeatures
	var err error
	requestedFeaturesSet.CompressionType = base.GetCompressionType(compressionType)
	errKey := base.CompressionTypeREST

	stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("validateCompression(%v, %v, %v)", sourceBucket, targetClusterRef.Name(), targetBucket), base.DiagNetworkThreshold)
	defer stopFunc()
	if compressionType == base.CompressionTypeNone {
		// Don't check anything
		return err
	}
	err = service.validateCompressionPreReq(errorMap, targetBucket, targetBucketInfo, compressionType, errKey)
	if len(errorMap) > 0 || err != nil {
		return err
	}
	return nil
}

// Prereq compression check is cheap
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

// validate target bucket
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

	stopFunc := service.utils.StartDiagStopwatch(fmt.Sprintf("validateBucket(%v,%v,%v,%v)", sourceBucket, targetCluster, targetBucket, bucketType), base.DiagInternalThreshold)
	defer stopFunc()

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
	} else if bucketType != base.CouchbaseBucketType && bucketType != base.EphemeralBucketType {
		errMsg := fmt.Sprintf("Incompatible %v bucket '%v'", qualifier, bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	} else if err != nil {
		errMsg := fmt.Sprintf("Error validating %v bucket '%v'. err=%v", qualifier, bucketName, err)
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
		return nil, errors.New(base.ReplicationSpecNotFoundErrorMessage)
	}

	key := getKeyFromReplicationId(replicationId)
	err = service.metadata_svc.DelWithCatalog(ReplicationSpecsCatalogKey, key, spec.Revision)
	if err != nil {
		// Check to see if the data still exists. Only if it doesn't exist is the del op considered passed
		_, _, checkErr := service.metadata_svc.Get(key)
		if checkErr != service_def.MetadataNotFoundErr {
			service.logger.Errorf("Failed to delete replication spec, key=%v, err=%v\n", key, err)
			return nil, err
		}
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
		service.remote_cluster_svc.RequestRemoteMonitoring(replSpec)
	}
	return

}

func (service *ReplicationSpecService) AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		spec := cacheableMetadataObjToReplicationSpec(val)
		if spec != nil {
			specs[key] = spec.Clone()
		}
	}
	return specs, nil
}

func cacheableMetadataObjToReplicationSpec(val CacheableMetadataObj) *metadata.ReplicationSpecification {
	rsv, ok := val.(*ReplicationSpecVal)
	if !ok {
		return nil
	}
	if rsv.spec == nil {
		return nil
	}
	genSpec, ok := rsv.spec.(metadata.GenericSpecification)
	if !ok {
		return nil
	}
	spec := genSpec.(*metadata.ReplicationSpecification)
	return spec
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
		spec := cacheableMetadataObjToReplicationSpec(val)
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
		spec := cacheableMetadataObjToReplicationSpec(val)
		if spec != nil {
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

	// We should only really handle settings upgrade during node boot up time as part of rolling upgrade
	// Otherwise we will be racing against actual set commands
	if !lock {
		service.handleSettingsUpgrade(spec)
	}

	return spec, nil
}

// spec.Settings needs to have all setting values populated,
// otherwise, replication behavior may change unexpectedly after upgrade when there are changes to default setting values.
// When new replication spec is created, spec.Settings always has all applicable setting values populated.
// The only scenario where values may be missing from spec.Settings is when new setting keys are added after upgrade.
// This method checks whether there are values missing from spec.Settings.
// If there are, it populates these values using default values, and writes updated settings/spec to metakv.
func (service *ReplicationSpecService) handleSettingsUpgrade(spec *metadata.ReplicationSpecification) {
	updatedKeys := spec.Settings.PopulateDefault()
	updatedKeys = service.handleCompressionUpgrade(spec, updatedKeys)
	updatedKeys = service.handleAdvFilteringUpgrade(spec, updatedKeys)
	if len(updatedKeys) == 0 {
		return
	}

	service.logger.Infof("Updating spec %v in metakv since its settings has been upgraded. upgraded settings keys = %v", spec.Id, updatedKeys)
	err := service.setReplicationSpecInternal(spec, false)
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

func (service *ReplicationSpecService) handleCompressionUpgrade(spec *metadata.ReplicationSpecification, updatedKeys []string) []string {
	isEnterprise, _ := service.xdcr_comp_topology_svc.IsMyClusterEnterprise()
	if !isEnterprise && spec.Settings.Values[metadata.CompressionTypeKey] == base.CompressionTypeAuto {
		spec.Settings.Values[metadata.CompressionTypeKey] = base.CompressionTypeNone
		updatedKeys = append(updatedKeys, metadata.CompressionTypeKey)
	} else if spec.Settings.Values[metadata.CompressionTypeKey] == base.CompressionTypeSnappy {
		spec.Settings.Values[metadata.CompressionTypeKey] = base.CompressionTypeAuto
		updatedKeys = append(updatedKeys, metadata.CompressionTypeKey)
	}
	return updatedKeys
}

func (service *ReplicationSpecService) handleAdvFilteringUpgrade(spec *metadata.ReplicationSpecification, keys []string) []string {
	if spec.Settings.FilterExpression == "" {
		// No filtering, nothing to upgrade
		return keys
	}

	clusterCompat, err := service.xdcr_comp_topology_svc.MyClusterCompatibility()
	if err != nil {
		service.logger.Warnf("Unable to get local cluster compatibility: %v - ignoring adv filtering upgrade", err)
		return keys
	}

	if base.IsClusterCompatible(clusterCompat, base.VersionForAdvFilteringSupport) {
		keys = spec.Settings.UpgradeFilterIfNeeded(keys)
	}
	return keys
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

func (service *ReplicationSpecService) updateCacheInternal(specId string, newSpec *metadata.ReplicationSpecification, lock bool) error {
	if lock {
		service.cache_lock.Lock()
		defer service.cache_lock.Unlock()
	}

	oldSpec, updated, err := service.updateCacheInternalNoLock(specId, newSpec)
	if err != nil {
		service.logger.Warnf("updateCacheInternalNoLock(%v) returned err %v", specId, err)
	}

	if updated && oldSpec != nil && newSpec == nil {
		service.gcMtx.Lock()
		delete(service.srcGcMap, oldSpec.Id)
		delete(service.tgtGcMap, oldSpec.Id)
		service.gcMtx.Unlock()
		// If a same replication is created and then deleted almost simultaneously on one node,
		// IIRC, the other node's metakv listener is not guaranteed to send the add + delete in order
		// If that is the case, this request/unrequest could leak
		// The fix of it is to kill goxdcr process, or remote the cluster ref and re-add it
		service.unRequestRemoteMonitoring(oldSpec)
	}

	if updated && oldSpec == nil && newSpec != nil {
		if service.remote_cluster_svc.RequestRemoteMonitoring(newSpec) != nil {
			service.retryRequestRemoteMonitoring(specId, newSpec, oldSpec)
			// Skip the metadatachangeCb for now as gofunc above will call it later
			return nil
		}
	}

	var sumErr error
	if updated {
		sumErr = service.callMetadataChangeCb(specId, newSpec, oldSpec)
	}

	return sumErr
}

func (service *ReplicationSpecService) unRequestRemoteMonitoring(oldSpec *metadata.ReplicationSpecification) error {
	// First, check for any concurrent bg running request remote monitoring processes
	service.rcBgReqMtx.Lock()
	finCh, exists := service.rcBgReqFinchMap[oldSpec.Id]
	if exists {
		close(finCh)
		service.rcBgReqSyncMap[oldSpec.Id] = true
		service.rcBgReqMtx.Unlock()
		return nil
	}
	service.rcBgReqMtx.Unlock()

	return service.remote_cluster_svc.UnRequestRemoteMonitoring(oldSpec)
}

func (service *ReplicationSpecService) retryRequestRemoteMonitoring(specId string, newSpec *metadata.ReplicationSpecification, oldSpec *metadata.ReplicationSpecification) {
	service.rcBgReqMtx.Lock()
	stopCh := make(chan bool)
	service.rcBgReqFinchMap[specId] = stopCh
	service.rcBgReqMtx.Unlock()

	// Do the following in a separate go-routine since this call path is synchronized by a single
	// go-routine handling all the metakv listeners
	go func() {
		// In automated test environment where remote cluster reference and spec is created
		// almost simultaneously, there's no guarantee that the remote cluster ref created
		// on an active node has propagated to a non-active node before the new spec is propagated to the said
		// non-acive node.
		// Because of this operation requires that the ref be present first, and it must not fail as
		// other components like collections manifest service depends on this, retry up to 8 seconds
		retryOp := func(interface{}) (interface{}, error) {
			err := service.remote_cluster_svc.RequestRemoteMonitoring(newSpec)
			if err != nil {
				service.logger.Warnf("RequestRemoteMonitoring for %v returned err %v", newSpec.Id, err)
			}
			return nil, err
		}
		_, err := service.utils.ExponentialBackoffExecutorWithFinishSignal("RequestRemoteMonitoring", base.RemoteBucketMonitorWaitTime, base.RemoteBucketMonitorMaxRetry, base.RemoteBucketMonitorRetryFactor, retryOp, nil, stopCh)
		finChCalled := err != nil && strings.Contains(err.Error(), base.FinClosureStr)
		if err != nil && !finChCalled {
			// Worst case scenario even after 8 seconds, panic and XDCR process will reload everything in order from metakv
			// as in, load the remote cluster reference first, then load the specs
			rcs, _ := service.remote_cluster_svc.RemoteClusters()
			redactedRcs := make(map[string]interface{})
			for k, v := range rcs {
				redactedRcs[k] = v.ToMap()
			}
			panicString := fmt.Sprintf("Cannot continue without requesting remote monitoring: spec %v requires remote cluster cluster UUID %v. Currently, only present remote clusters: %v",
				newSpec.Id, newSpec.TargetClusterUUID, redactedRcs)
			panic(panicString)
		}

		// Once here, it is either because the call succeeded OR the finCh was called
		// Before calling metadatachange cb, ensure the right cases
		service.rcBgReqMtx.Lock()
		unRequestCalled := service.rcBgReqSyncMap[specId]
		delete(service.rcBgReqFinchMap, specId)
		delete(service.rcBgReqSyncMap, specId)
		service.rcBgReqMtx.Unlock()

		if finChCalled {
			service.logger.Warnf("While RequestRemoteMonitoring was retrying for %v, finCh was called", specId)
			// Unregister was called while the retry above was happening
			// finChCalled returned means that the Request never went through. This is essentially an no-op
		} else if unRequestCalled {
			// the retry register call succeeded, but while that was happening, the unRegister called came in for this specId
			// XDCR should faithfully unregister it on its behalf
			service.remote_cluster_svc.UnRequestRemoteMonitoring(newSpec)
		} else {
			service.callMetadataChangeCb(specId, newSpec, oldSpec)
		}
	}()
}

func (service *ReplicationSpecService) callMetadataChangeCb(specId string, newSpec *metadata.ReplicationSpecification, oldSpec *metadata.ReplicationSpecification) error {
	errMap := make(base.ErrorMap)
	var highErr error
	var medErr error
	var lowErr error

	service.metadataChangeMtx.RLock()
	defer service.metadataChangeMtx.RUnlock()
	if len(service.metadataChangeCallback) == 0 {
		return nil
	}

	// Replication spec callbacks are to be executed concurrently only for those of the same priorities
	// So for each type of operation (add spec, del spec, or mod spec), do the high priority ones first
	// concurrently, then medium, then low, etc
	if newSpec != nil && oldSpec == nil {
		highErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, addOp, base.MetadataChangeHighPrioriy)
		medErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, addOp, base.MetadataChangeMedPrioriy)
		lowErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, addOp, base.MetadataChangeLowPrioriy)
	} else if newSpec == nil && oldSpec != nil {
		highErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, delOp, base.MetadataChangeHighPrioriy)
		medErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, delOp, base.MetadataChangeMedPrioriy)
		lowErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, delOp, base.MetadataChangeLowPrioriy)
	} else {
		highErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, modOp, base.MetadataChangeHighPrioriy)
		medErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, modOp, base.MetadataChangeMedPrioriy)
		lowErr = service.executeCallbackWithPriority(specId, newSpec, oldSpec, modOp, base.MetadataChangeLowPrioriy)
	}

	if highErr != nil {
		errMap["highPriorityErr"] = highErr
	}
	if medErr != nil {
		errMap["mediumPriorityErr"] = medErr
	}
	if lowErr != nil {
		errMap["lowPriorityErr"] = lowErr
	}

	if len(errMap) == 0 {
		return nil
	}
	return fmt.Errorf(base.FlattenErrorMap(errMap))
}

func (service *ReplicationSpecService) executeCallbackWithPriority(specId string, newSpec *metadata.ReplicationSpecification, oldSpec *metadata.ReplicationSpecification, op callbackOp, priority base.MetadataChangeHandlerPriority) error {
	var waitGrp sync.WaitGroup
	var err error
	var sumErr error
	for idx, callback := range service.metadataChangeCallback {
		if callback.GetPriority(op) != priority {
			continue
		}
		waitGrp.Add(1)
		stopWatchStop := service.utils.StartDiagStopwatch(fmt.Sprintf("Executing callback for %v", service.cbIDs[idx]), base.DiagInternalThreshold)
		warnTimer := time.AfterFunc(10*time.Second, func() {
			service.logger.Warnf("Executing callback %v stuck for more than 10 seconds", service.cbIDs[idx])
		})
		err = callback.cb(specId, oldSpec, newSpec, &waitGrp)
		stopWatchStop()
		warnTimer.Stop()
		if err != nil {
			service.logger.Error(err.Error())
			if sumErr == nil {
				sumErr = err
			} else {
				sumErr = fmt.Errorf("%v; %v", sumErr, err)
			}
		}
	}
	waitGrp.Wait()
	return sumErr
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
		return strings.HasPrefix(err.Error(), ReplicationSpecAlreadyExistErrorMessage) || strings.HasPrefix(err.Error(), base.ReplicationSpecNotFoundErrorMessage)
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
		return fmt.Errorf(base.ReplicationSpecNotFoundErrorMessage)
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
	// No need to lock service.cache_lock since the cache is atomic.Value
	// If cache is being updated at the same time, the atomic.Value will ensure that
	// we get a consistent value.
	cachedVal, ok := service.getCache().Get(specId)
	if !ok || cachedVal == nil {
		return nil, fmt.Errorf(base.ReplicationSpecNotFoundErrorMessage)
	}

	cachedObj, ok := cachedVal.(*ReplicationSpecVal)
	if !ok || cachedObj == nil {
		panic("Object in ReplicationSpecServcie cache is not of type *replciationSpecVal")
	}
	return cachedObj.derivedObj, nil
}

func (service *ReplicationSpecService) appendGoMaxProcsWarnings(sourceBucketName string, ref *metadata.RemoteClusterReference, targetBucket string, warnings service_def.UIWarnings, settings metadata.ReplicationSettingsMap) {
	currentGoMaxProcs := runtime.GOMAXPROCS(0)
	sourceNozzleCnt, srcExists := settings[metadata.SourceNozzlePerNodeKey].(int)
	targetNozzleCnt, tgtExists := settings[metadata.TargetNozzlePerNodeKey].(int)

	if !srcExists || !tgtExists {
		// This can happen when replication is being updated - fetch existing spec
		replId := metadata.ReplicationId(sourceBucketName, ref.Uuid(), targetBucket)
		currentSpec, specErr := service.replicationSpec(replId)
		var currentSettings *metadata.ReplicationSettings
		if specErr != nil || currentSpec == nil {
			// Most likely it is creation path
			defaultReplSettings, err := service.replicationSettingSvc.GetDefaultReplicationSettings()
			if err != nil {
				service.logger.Errorf("Unable to get default replication setting - skip checking GoMaxProcs and nozzles warnings")
				return
			}
			currentSettings = defaultReplSettings
		} else {
			currentSettings = currentSpec.Settings
		}
		if !srcExists {
			sourceNozzleCnt = currentSettings.Values[metadata.SourceNozzlePerNodeKey].(int)
		}
		if !tgtExists {
			targetNozzleCnt = currentSettings.Values[metadata.TargetNozzlePerNodeKey].(int)
		}
	}

	if (sourceNozzleCnt + targetNozzleCnt) > currentGoMaxProcs {
		msg := fmt.Sprintf("Settings requested %v source nozzles per node and %v target nozzles per node, which is greater than GOMAXPROCS (%v). System performance may be degraded",
			sourceNozzleCnt, targetNozzleCnt, currentGoMaxProcs)
		// Currently, UI only needs warning under the Source label
		warnings.AddWarning(base.SourceNozzlePerNode, msg)
	}
	return
}

func (service *ReplicationSpecService) SetManifestsGetter(getter service_def.ManifestsGetter) {
	service.manifestsGetterMtx.Lock()
	defer service.manifestsGetterMtx.Unlock()
	service.manifestsGetter = getter
}

func (service *ReplicationSpecService) waitForManifestGetter() {
	var manifestGetterFound bool
	for !manifestGetterFound {
		service.manifestsGetterMtx.RLock()
		manifestGetterFound = service.manifestsGetter != nil
		service.manifestsGetterMtx.RUnlock()
		if !manifestGetterFound {
			time.Sleep(time.Duration(base.ManifestsGetterSleepTimeSecs) * time.Second)
		}
	}
}

func (service *ReplicationSpecService) getManifests(sourceBucket string, targetBucket string,
	ref *metadata.RemoteClusterReference) (*metadata.CollectionsManifestPair, error) {
	service.waitForManifestGetter()
	getterSpec, err := metadata.NewReplicationSpecification(sourceBucket, "", ref.Uuid(), targetBucket, "")
	if err != nil {
		return nil, err
	}

	srcManifest, tgtManifest, err := service.manifestsGetter(getterSpec, true)
	if err != nil {
		return nil, err
	}

	if srcManifest == nil {
		return nil, fmt.Errorf("Source bucket %v has no manifest", sourceBucket)
	}
	if tgtManifest == nil {
		return nil, fmt.Errorf("Target bucket %v has no manifest", targetBucket)
	}

	return &metadata.CollectionsManifestPair{Source: srcManifest, Target: tgtManifest}, nil
}
