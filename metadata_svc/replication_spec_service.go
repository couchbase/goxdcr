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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
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
}

func NewReplicationSpecService(uilog_svc service_def.UILogSvc, remote_cluster_svc service_def.RemoteClusterSvc,
	metadata_svc service_def.MetadataSvc, xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext) (*ReplicationSpecService, error) {
	logger := log.NewLogger("ReplicationSpecService", logger_ctx)
	svc := &ReplicationSpecService{
		metadata_svc:           metadata_svc,
		uilog_svc:              uilog_svc,
		remote_cluster_svc:     remote_cluster_svc,
		xdcr_comp_topology_svc: xdcr_comp_topology_svc,
		cluster_info_svc:       cluster_info_svc,
		cache:                  nil,
		cache_lock:             &sync.Mutex{},
		logger:                 logger,
	}

	err := svc.initCache()
	if err != nil {
		return nil, err
	}
	return svc, nil
}

func (service *ReplicationSpecService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

func (service *ReplicationSpecService) initCache() error {
	service.logger.Info("Init cache for ReplicationSpecService...")
	cache := NewMetadataCache(service.logger)

	entries, err := service.metadata_svc.GetAllMetadataFromCatalog(ReplicationSpecsCatalogKey)
	if err != nil {
		service.logger.Errorf("Failed to get all entries, err=%v\n", err)
		return err
	}

	for _, entry := range entries {
		spec, err := constructReplicationSpec(entry.Value, entry.Rev)
		if err != nil || spec == nil {
			service.logger.Errorf("Failed to contruct replication spec, key=%v, err=%v\n", entry.Key, err)
			return err
		}
		service.cacheSpec(cache, spec.Id, spec)
	}
	service.cache = cache
	service.logger.Info("Cache has been initialized for ReplicationSpecService")
	return nil
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

func (service *ReplicationSpecService) ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string, settings map[string]interface{}) (string, string, *metadata.RemoteClusterReference, map[string]error) {
	service.logger.Infof("Start ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v\n", sourceBucket, targetCluster, targetBucket)

	errorMap := make(map[string]error)

	//validate the existence of source bucket
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}

	var err_source error
	start_time := time.Now()
	sourceBucketObj, err_source := utils.LocalBucket(local_connStr, sourceBucket)
	service.logger.Infof("Result from local bucket look up: err_source=%v, time taken=%v\n", err_source, time.Since(start_time))
	service.validateBucket(sourceBucket, targetCluster, targetBucket, sourceBucketObj.Type, err_source, errorMap, true)

	sourceBucketUUID := ""
	if sourceBucketObj != nil {
		sourceBucketUUID = sourceBucketObj.UUID
	}

	// validate remote cluster ref
	start_time = time.Now()
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByRefName(targetCluster, true)
	if err != nil {
		errorMap[base.ToCluster] = utils.NewEnhancedError("cannot find remote cluster", err)
		return "", "", nil, errorMap
	}
	service.logger.Infof("Successfully retrieved target cluster reference. time take=%v\n", time.Since(start_time))

	// validate that the source bucket and target bucket are not the same bucket
	// i.e., validate that the following are not both true:
	// 1. sourceBucketName == targetBucketName
	// 2. sourceClusterUuid == targetClusterUuid
	if sourceBucket == targetBucket {
		start_time = time.Now()
		sourceClusterUuid, err := service.xdcr_comp_topology_svc.MyClusterUuid()
		if err != nil {
			panic("cannot get local cluster uuid")
		}

		if sourceClusterUuid == targetClusterRef.Uuid {
			errorMap[base.PlaceHolderFieldKey] = errors.New("Replication from a bucket to the same bucket is not allowed")
			return "", "", nil, errorMap
		}
		service.logger.Infof("Validated that source bucket and target bucket are not the same. time taken=%v\n", time.Since(start_time))
	}

	remote_connStr, err := targetClusterRef.MyConnectionStr()
	if err != nil {
		errorMap[base.ToCluster] = utils.NewEnhancedError("Invalid remote cluster. MyConnectionStr() failed.", err)
		return "", "", nil, errorMap
	}
	remote_userName, remote_password, certificate, sanInCertificate, err := targetClusterRef.MyCredentials()
	if err != nil {
		errorMap[base.ToCluster] = utils.NewEnhancedError("Invalid remote cluster. MyCredentials() failed.", err)
		return "", "", nil, errorMap
	}

	//validate target bucket
	start_time = time.Now()
	//get uuid and type from bucket info
	targetBucketInfo, err_target := utils.GetBucketInfo(remote_connStr, targetBucket, remote_userName, remote_password, certificate, sanInCertificate, service.logger)

	targetBucketType := ""
	if err_target == nil && targetBucketInfo != nil {
		targetBucketTypeObj, ok := targetBucketInfo[base.BucketTypeKey]
		if !ok {
			err_target = fmt.Errorf("Error looking up bucket type of target bucket %v", targetBucket)
		} else {
			targetBucketType, ok = targetBucketTypeObj.(string)
			if !ok {
				err_target = fmt.Errorf("Bucket type of target bucket %v is of wrong type", targetBucket)
			}
		}
	}

	service.logger.Infof("Result from remote bucket look up: err_target=%v, time taken=%v\n", err_target, time.Since(start_time))
	service.validateBucket(sourceBucket, targetCluster, targetBucket, targetBucketType, err_target, errorMap, false)

	// validate that source and target bucket have the same conflict resolution type metadata
	targetConflictResolutionType, err := utils.GetConflictResolutionTypeFromBucketInfo(targetBucket, targetBucketInfo)
	if err != nil {
		errorMap[base.PlaceHolderFieldKey] = errors.New("Error retrieving ConflictResolutionType setting on target bucket")
		return "", "", nil, errorMap
	}
	if sourceBucketObj.ConflictResolutionType != targetConflictResolutionType {
		errorMap[base.PlaceHolderFieldKey] = errors.New("Replication between buckets with different ConflictResolutionType setting is not allowed")
		return "", "", nil, errorMap
	}

	targetBucketUUID := ""
	if targetBucketInfo != nil {
		targetBucketUUID, err = utils.GetBucketUuidFromBucketInfo(targetBucket, targetBucketInfo, service.logger)
		if err != nil {
			errorMap[base.PlaceHolderFieldKey] = err
		}
	}

	repId := metadata.ReplicationId(sourceBucket, targetClusterRef.Uuid, targetBucket)
	_, err = service.replicationSpec(repId)
	if err == nil {
		errorMap[base.PlaceHolderFieldKey] = errors.New(ReplicationSpecAlreadyExistErrorMessage)
	}

	// if replication type is set to xmem, validate that the target cluster is xmem compatible
	repl_type, ok := settings[metadata.ReplicationType]
	if !ok || repl_type == metadata.ReplicationTypeXmem {
		xmemCompatible, err := service.cluster_info_svc.IsClusterCompatible(targetClusterRef, []int{2, 2})
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get cluster version information, err=%v\n", err)
			service.logger.Error(errMsg)
			errorMap[base.ToCluster] = errors.New(errMsg)
		} else {
			if !xmemCompatible {
				errorMap[base.ToCluster] = errors.New("Version 2 replication is disallowed. Cluster has nodes with versions less than 2.2.")
			}
		}
	}

	service.logger.Infof("Finished ValidateAddReplicationSpec. errorMap=%v\n", errorMap)

	return sourceBucketUUID, targetBucketUUID, targetClusterRef, errorMap
}

func (service *ReplicationSpecService) validateBucket(sourceBucket, targetCluster, targetBucket, bucketType string, err error, errorMap map[string]error, isSourceBucket bool) {
	var qualifier, errKey, bucketName string
	if isSourceBucket {
		qualifier = "source"
		errKey = base.FromBucket
		bucketName = sourceBucket
	} else {
		qualifier = "target"
		errKey = base.ToBucket
		bucketName = targetBucket
	}

	if err == utils.NonExistentBucketError {
		service.logger.Errorf("Spec [sourceBucket=%v, targetCluster=%v, targetBucket=%v] refers to non-existent %v bucket\n", sourceBucket, targetCluster, targetBucket, qualifier)
		errorMap[errKey] = utils.BucketNotFoundError(bucketName)
	} else if err != nil {
		errMsg := fmt.Sprintf("Error validating %v bucket '%v'. err=%v", qualifier, bucketName, err)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	} else if bucketType != base.CouchbaseBucketType {
		errMsg := fmt.Sprintf("Incompatible %v bucket '%v'", qualifier, bucketName)
		service.logger.Error(errMsg)
		errorMap[errKey] = fmt.Errorf(errMsg)
	}
}

func (service *ReplicationSpecService) AddReplicationSpec(spec *metadata.ReplicationSpecification) error {
	service.logger.Infof("Start AddReplicationSpec, spec=%v\n", spec)

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

	err = service.updateCache(spec.Id, spec)
	if err == nil {
		service.writeUiLog(spec, "created", "")
	}
	return err
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

	_, rev, err := service.metadata_svc.Get(key)
	if err != nil {
		return err
	}
	spec.Revision = rev

	err = service.updateCache(spec.Id, spec)
	if err == nil {
		service.logger.Infof("Replication spec %s has been updated, rev=%v\n", spec.Id, rev)
		return nil
	} else {
		return err
	}
}

func (service *ReplicationSpecService) DelReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	return service.delReplicationSpec_internal(replicationId, "")
}

func (service *ReplicationSpecService) delReplicationSpec_internal(replicationId, reason string) (*metadata.ReplicationSpecification, error) {
	spec, err := service.replicationSpec(replicationId)
	if err != nil {
		return nil, errors.New(ReplicationSpecNotFoundErrorMessage)
	}

	key := getKeyFromReplicationId(replicationId)
	err = service.metadata_svc.DelWithCatalog(ReplicationSpecsCatalogKey, key, spec.Revision)
	if err != nil {
		service.logger.Errorf("Failed to delete replication spec, key=%v, rev=%v\n", key, spec.Revision)
		return nil, err
	}

	err = service.updateCache(replicationId, nil)
	if err == nil {
		service.writeUiLog(spec, "removed", "")
		return spec, nil
	} else {
		return nil, err
	}

}

func (service *ReplicationSpecService) AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		if val.(*ReplicationSpecVal).spec != nil {
			specs[key] = val.(*ReplicationSpecVal).spec
		}
	}
	return specs, nil
}

func (service *ReplicationSpecService) AllReplicationSpecIds() ([]string, error) {
	repIds := []string{}
	rep_map, err := service.AllReplicationSpecs()
	if err != nil {
		return nil, err
	}
	for key, _ := range rep_map {
		repIds = append(repIds, key)
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

func (service *ReplicationSpecService) updateCache(specId string, newSpec *metadata.ReplicationSpecification) error {
	//this ensures that all accesses to the cache in this method are a single atomic operation,
	// this is needed because this method can be called concurrently
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

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
				return err
			}
		}

	}

	if updated && service.metadata_change_callback != nil {
		err := service.metadata_change_callback(specId, oldSpec, newSpec)
		if err != nil {
			service.logger.Error(err.Error())
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

func (service *ReplicationSpecService) ValidateExistingReplicationSpec(spec *metadata.ReplicationSpecification) (error, error) {
	//validate the existence of source bucket
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	sourceBucketUuid, err_source := utils.LocalBucketUUID(local_connStr, spec.SourceBucketName)

	if err_source == utils.NonExistentBucketError {
		errMsg := fmt.Sprintf("spec %v refers to non-existent source bucket \"%v\"", spec.Id, spec.SourceBucketName)
		service.logger.Error(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}

	if spec.SourceBucketUUID != "" && spec.SourceBucketUUID != sourceBucketUuid {
		//spec is referring to a deleted bucket
		errMsg := fmt.Sprintf("spec %v refers to bucket %v which was deleted and recreated", spec.Id, spec.SourceBucketName)
		service.logger.Error(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}

	//validate target cluster
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == service_def.MetadataNotFoundErr {
		//remote cluster is no longer valid
		errMsg := fmt.Sprintf("spec %v refers to non-existent remote cluster reference \"%v\"", spec.Id, spec.TargetClusterUUID)
		service.logger.Errorf(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	} else if err != nil {
		return err, nil
	}

	remote_connStr, err := targetClusterRef.MyConnectionStr()
	if err != nil {
		errMsg := fmt.Sprintf("spec %v refers to an invalid remote cluster reference \"%v\", as RemoteClusterRef.MyConnectionStr() returns err=%v\n", spec.Id, spec.TargetClusterUUID, err)
		service.logger.Errorf(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}
	remote_userName, remote_password, certificate, sanInCertificate, err := targetClusterRef.MyCredentials()
	if err != nil {
		errMsg := fmt.Sprintf("spec %v refers to an invalid remote cluster reference \"%v\", as RemoteClusterRef.MyCredentials() returns err=%v\n", spec.Id, spec.TargetClusterUUID, err)
		service.logger.Errorf(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}

	//validate target bucket
	targetBucketUUID, err_target := utils.RemoteBucketUUID(remote_connStr, spec.TargetBucketName, remote_userName, remote_password, certificate, sanInCertificate, service.logger)
	service.logger.Infof("result of remote bucket call:  remote_connStr=%v, targetBucketUUID=%v, err_target=%v\n", remote_connStr, targetBucketUUID, err_target)

	if err_target == utils.NonExistentBucketError {
		errMsg := fmt.Sprintf("spec %v refers to non-existent target bucket \"%v\"\n", spec.Id, spec.TargetBucketName)
		service.logger.Errorf(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	} else if err_target != nil {
		service.logger.Infof("Received error %v when validating target bucket %v for spec %v. Skipping target bucket validation. remote_connStr=%v, remote_userName=%v\n",
			err_target, spec.TargetBucketName, spec.Id, remote_connStr, remote_userName)
	}

	if spec.TargetBucketUUID != "" && spec.TargetBucketUUID != targetBucketUUID {
		//spec is referring to a deleted bucket
		errMsg := fmt.Sprintf("spec %v refers to bucket %v which was deleted and recreated\n", spec.Id, spec.TargetBucketName)
		service.logger.Errorf(errMsg)
		return InvalidReplicationSpecError, errors.New(errMsg)
	}

	return nil, nil
}

func (service *ReplicationSpecService) ValidateAndGC(spec *metadata.ReplicationSpecification) {
	err, detail_err := service.ValidateExistingReplicationSpec(spec)
	if err == InvalidReplicationSpecError {
		service.logger.Errorf("Replication specification %v is no longer valid, garbage collect it. error=%v\n", spec.Id, detail_err)
		_, err1 := service.delReplicationSpec_internal(spec.Id, detail_err.Error())
		if err1 != nil {
			service.logger.Infof("Failed to garbage collect spec %v, err=%v\n", spec.Id, err1)
		}
	}
}

func (service *ReplicationSpecService) sourceBucketUUID(bucketName string) (string, error) {
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	return utils.LocalBucketUUID(local_connStr, bucketName)
}

func (service *ReplicationSpecService) targetBucketUUID(targetClusterUUID, bucketName string) (string, error) {
	ref, err_target := service.remote_cluster_svc.RemoteClusterByUuid(targetClusterUUID, false)
	if err_target != nil {
		return "", err_target
	}
	remote_connStr, err_target := ref.MyConnectionStr()
	if err_target != nil {
		return "", err_target
	}
	remote_userName, remote_password, certificate, sanInCertificate, err_target := ref.MyCredentials()
	if err_target != nil {
		return "", err_target
	}

	return utils.RemoteBucketUUID(remote_connStr, bucketName, remote_userName, remote_password, certificate, sanInCertificate, service.logger)
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

	spec := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID, targetBucketName, targetBucketUUID)
	return spec, nil
}

func (service *ReplicationSpecService) cacheSpec(cache *MetadataCache, specId string, spec *metadata.ReplicationSpecification) error {
	var cachedObj *ReplicationSpecVal = nil
	var updatedCachedObj *ReplicationSpecVal = nil
	var ok1 bool
	cachedVal, ok := cache.Get(specId)
	if ok && cachedVal != nil {
		cachedObj, ok1 = cachedVal.(*ReplicationSpecVal)
		if !ok1 || cachedObj == nil {
			panic("Object in ReplicationSpecServcie cache is not of type *replciationSpecVal")
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

func (service *ReplicationSpecService) GetDerviedObj(specId string) (interface{}, error) {
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
